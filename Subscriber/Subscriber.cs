#region

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Broker;
using CommonTypes;

#endregion

namespace Subscriber
{
    internal class Subscriber : BaseProcess, ISubscriber
    {
        // this site's brokers
        public List<IBroker> Brokers { get; set; } = new List<IBroker>();
        // the sequence numbers for publishers
        public IDictionary<string, int> InSequenceNumbers { get; private set; } = new ConcurrentDictionary<string, int>();
        // a hold-back queue that stores delayed messages
        public IDictionary<string, MessageQueue> HoldbackQueue { get; } =
            new ConcurrentDictionary<string, MessageQueue>();

        // a registry of topics subscribed. This is useful to clean up after an unsubscription
        public IDictionary<string, List<string>> Topics { get; } = new ConcurrentDictionary<string, List<string>>();
        // the sequence number used by messages sent to the broker group
        public int OutSequenceNumber { get; private set; }
        // the sent subscriptions
        public ProcessHistory History { get; } = new ProcessHistory();
        // the broker where the messages are sent
        public IBroker DesignatedBroker { get; set; }

        public Subscriber(string processName, string processUrl, string puppetMasterUrl, string siteName)
            : base(processName, processUrl, puppetMasterUrl, siteName)
        {
            List<string> brokerUrls = GetBrokers(puppetMasterUrl);
            int smallestPort = int.MaxValue;

            // connect to the brokers at the site
            foreach (string brokerUrl in brokerUrls)
            {
                string name;
                int port;
                Utility.DivideUrl(brokerUrl, out port, out name);

                Utility.ConnectFunction<IBroker> fun = (string url) =>
                {
                    IBroker broker = (IBroker) Activator.GetObject(typeof (IBroker), url);
                    broker.RegisterPubSub(ProcessName, Url);

                    return broker;
                };

                try
                {
                    IBroker brokerObject = Utility.TryConnection(fun, brokerUrl);
                    Brokers.Add(brokerObject);
                    if (port < smallestPort)
                    {
                        smallestPort = port;
                        DesignatedBroker = brokerObject;
                    }
                } catch (Exception)
                {
                    Console.Out.WriteLine("********************************************\r\n");
                    Console.Out.WriteLine("\tERROR: Couldn't connect to broker '" + brokerUrl + "'. It might be dead");
                    Console.Out.WriteLine("\r\n********************************************");
                }
            }
        }

        /// <summary>
        ///     Delivers a command to the subscriber
        /// </summary>
        /// <param name="command"></param>
        /// <returns> True if the command was successfuly delivered. False otherwise </returns>
        public override bool DeliverCommand(string[] command)
        {
            if (Status == Status.Frozen && !command[0].Equals("Unfreeze"))
            {
                base.DeliverCommand(command);
                return true;
            }

            lock (this)
            {
                string complete = string.Join(" ", command);
                Console.Out.WriteLine("Received command: " + complete);
                switch (command[0])
                {
                    // generic commands
                    case "Status":
                        base.DeliverCommand(command);
                        PrintStatus();
                        break;

                    case "Crash":
                    case "Freeze":
                        base.DeliverCommand(command);
                        break;

                    case "Unfreeze":
                        Utility.DebugLog("Unfreezing");
                        Status = Status.Unfrozen;
                        ProcessFrozenListCommands();
                        break;

                    case "Subscribe":
                        SendSubscription(command[1]);
                        break;

                    case "Unsubscribe":
                        SendUnsubscription(command[1]);
                        break;

                    default:
                        Utility.DebugLog("Command: " + command[0] + " doesn't exist!");
                        return false;
                }
            }

            return true;
        }

        private void PrintStatus()
        {
            bool emptyQueues = true;
            foreach (var queue in HoldbackQueue.Values)
            {
                if (queue.GetSequenceNumbers() != null && queue.GetSequenceNumbers().Count != 0)
                {
                    emptyQueues = false;
                    break;
                }
            }
            // prints delayed messages in holdback queue
            if (HoldbackQueue.Keys.Count == 0 || emptyQueues)
            {
                Console.Out.WriteLine("*\t There are no delayed publications");
            } else
            {
                Console.Out.WriteLine("*\tPublications in HoldBack queue: ");
                foreach (string pub in HoldbackQueue.Keys)
                {
                    MessageQueue queue = HoldbackQueue[pub];
                    ICollection<int> seqNums = queue.GetSequenceNumbers();

                    if (seqNums.Count == 0)
                        continue;

                    Console.Out.Write("*\tPublisher '" + pub + "' has messages ");

                    foreach (int seqNum in queue.GetSequenceNumbers())
                    {
                        Console.Out.Write(seqNum + " ");
                    }

                    Console.Out.WriteLine("in HoldBack queue");
                }
            }

            // prints number of received message
            foreach (var pair in InSequenceNumbers)
            {
                Console.Out.WriteLine("*\t Received " + pair.Value + " messages from " + pair.Key);
            }
            Console.Out.WriteLine("*******************\t\n");
        }

        /// <summary>
        ///     Delivers publication according to the specified ordering garantee
        /// </summary>
        /// <param name="publisher"></param>
        /// <param name="topic"></param>
        /// <param name="publication"></param>
        /// <param name="sequenceNumber"></param>
        public void DeliverPublication(string publisher, string topic, string publication, int sequenceNumber)
        {
            lock (HoldbackQueue)
            {
                if (PublicationReceived(publisher, topic, publication, sequenceNumber))
                {
                    Utility.DebugLog("Received publication '" + publication + "' with seq no " + sequenceNumber);
                    PublicationProcessed(publisher, topic, sequenceNumber);
                    Notify(publisher, topic, sequenceNumber);
                }
            }
        }

        /// <summary>
        ///     Notifies a broker of the last publication received after a certain time has passed
        /// without any new messages
        /// </summary>
        private void Notify(string publisher, string topic, int sequenceNumber)
        {
            lock (HoldbackQueue)
            {
                new Thread(() =>
                {
                    Thread.Sleep(3000);
                    int seqNo;

                    // checks if this sequence number is the last one
                    if (InSequenceNumbers.TryGetValue(publisher, out seqNo) && sequenceNumber == seqNo)
                    {
                        // checks if there are queued message
                        // If there are then the resend request was already sent
                        MessageQueue checkQueue;
                        if (HoldbackQueue.TryGetValue(publisher, out checkQueue) && checkQueue.GetSequenceNumbers().Any())
                            return;

                        // if we unsubbed from the topic, we don't want to receive anything else
                        List<string> pubs;
                        if (!Topics.TryGetValue(topic, out pubs))
                            return;

                        bool retry = true;
                        while (retry)
                        {
                            Thread subThread =
                                new Thread(() =>
                                {
                                    try
                                    {
                                        IBroker broker = Brokers[this.Random.Next(0, Brokers.Count)];
                                        broker.NotifyOfLast(publisher, SiteName, sequenceNumber, ProcessName);
                                        Utility.DebugLog("Notifying of last pub with seq no " + sequenceNumber);
                                        retry = false;
                                    } catch (Exception)
                                    {
                                        Utility.DebugLog("WARNING: Failed notification for seq no " + sequenceNumber +
                                                         ". Retrying.");
                                    }
                                });
                            subThread.Start();
                            subThread.Join();
                        }
                    }
                }).Start();
            }
        }

        /// <summary>
        ///     Decides what to do with the publication. 
        /// </summary>
        /// <returns> Returns true if it should be further processed or false if it shouldn't </returns>
        private bool PublicationReceived(string publisher, string topic, string publication, int sequenceNumber)
        {
            int seqNum;
            if (!InSequenceNumbers.TryGetValue(publisher, out seqNum))
            {
                seqNum = 0;
                InSequenceNumbers[publisher] = 0;
            }

            List<string> publishers;
            if (!Topics.TryGetValue(topic, out publishers))
            {
                publishers = new List<string> {publisher};
            } else if (!publishers.Contains(publisher))
            {
                publishers.Add(publisher);
            }

            Topics[topic] = publishers;

            // we queue if the ordering is incorrect or we're frozen
            if (sequenceNumber > seqNum + 1 || Status == Status.Frozen)
            {
                MessageQueue queue;
                if (!HoldbackQueue.TryGetValue(publisher, out queue))
                    queue = new MessageQueue();

                Utility.DebugLog("Queueing publication '" + publication + "' with seq " + sequenceNumber);
                queue.Add(new[] {publisher, topic, publication, sequenceNumber.ToString()}, sequenceNumber);
                HoldbackQueue[publisher] = queue;

                // request resend
                new Thread(() =>
                {
                    while (true)
                    {
                        // waits half a second before checking the queue.  if the minimum sequence
                        // number is this one, then our message didn't arrive and it's the one blocking 
                        // the queue. In that case, we request it
                        Thread.Sleep(500);
                        lock (HoldbackQueue)
                        {
                            MessageQueue checkQueue;
                            if (HoldbackQueue.TryGetValue(publisher, out checkQueue) &&
                                checkQueue.GetSequenceNumbers().Any())
                            {
                                int minSeqNo = checkQueue.GetSequenceNumbers().Min();
                                if (minSeqNo == sequenceNumber)
                                {
                                    Request(publisher, sequenceNumber);
                                    return;
                                }
                                // if the minimum sequence number is smaller than ours 
                                // we wait and repeat
                                if (minSeqNo < sequenceNumber)
                                    continue;

                                return;
                            }
                            return;
                        }
                    }
                }).Start();

                return false;
            }

            // if everything is right with the ordering, we deliver
            if (sequenceNumber == seqNum + 1)
                return true;

            Utility.DebugLog("Received previous publication with seqNo " + sequenceNumber + ". Ignoring");
            return false;
        }

        /// <summary>
        ///     Updates this subscriber's state. Updates sequence numbers, unblocks delayed messages (if any), etc
        /// </summary>
        /// <param name="topic"> The subscribed topic </param>
        /// <param name="publisher"> The publisher </param>
        /// <param name="sequenceNumber"> The message's sequence number </param>
        private void PublicationProcessed(string publisher, string topic, int sequenceNumber)
        {
            InSequenceNumbers[publisher] = sequenceNumber;
            new Thread(
                () =>
                {
                    try
                    {
                        PuppetMaster.DeliverLog("SubEvent " + ProcessName + ", " + publisher + ", " + topic);
                    } catch (Exception)
                    {
                        Utility.DebugLog("WARNING: " + ProcessName + " couldn't deliver log");
                    }
                }).Start();

            MessageQueue queue;
            if (HoldbackQueue.TryGetValue(publisher, out queue))
            {
                string[] message = queue.GetAndRemove(sequenceNumber + 1);
                if (message == null)
                    return;
                Utility.DebugLog("Unblocking publication with seq " + (sequenceNumber + 1));

                new Thread(
                    () => DeliverPublication(message[0], message[1], message[2], sequenceNumber + 1)).Start();
            }
        }

        /// <summary>
        ///     Requests a specific publication for a broker
        /// </summary>
        /// <param name="publisher"></param>
        /// <param name="sequenceNumber"></param>
        private void Request(string publisher, int sequenceNumber)
        {
            Utility.DebugLog("Requesting resend of seq no " + (sequenceNumber - 1) + " from broker");
            Thread thread =
                new Thread(() =>
                {
                    bool retry = true;
                    while (retry)
                    {
                        IBroker broker;
                        lock (Brokers)
                        {
                            // picks a random broker for load-balancing purposes
                            int brokerIndex = this.Random.Next(0, Brokers.Count);
                            broker = Brokers[brokerIndex];
                        }

                        Thread subThread =
                            new Thread(() =>
                            {
                                try
                                {
                                    broker.ResendPublication(publisher, SiteName, sequenceNumber - 1, ProcessName);
                                    retry = false;
                                } catch (Exception)
                                {
                                    DesignatedBroker = Brokers[this.Random.Next(0, Brokers.Count)];
                                }
                            });
                        subThread.Start();
                        subThread.Join();
                    }
                });
            thread.Start();
        }

        public void ProcessFrozenListCommands()
        {
            string[] command;
            while (CommandBacklog.TryDequeue(out command))
            {
                DeliverCommand(command);
            }

            foreach (MessageQueue queue in HoldbackQueue.Values)
            {
                string[] message;
                if ((message = queue.GetFirstAndRemove()) != null)
                    DeliverPublication(message[0], message[1], message[2], int.Parse(message[3]));
            }
        }

        /// <summary>
        ///     This method sends a subscription to a random broker at this site
        /// </summary>
        /// <param name="topic"> The topic of the subscription </param>
        private void SendSubscription(string topic)
        {
            int seqNo;
            lock (this)
            {
                seqNo = ++OutSequenceNumber;
            }

            Utility.DebugLog("Subscribing to topic " + topic + " with seq no " + seqNo);
            bool retry = true;
            while (retry)
            {
                Thread thread = new Thread(() =>
                {
                    try
                    {
                        DesignatedBroker.DeliverSubscription(ProcessName, topic, SiteName, seqNo);
                        retry = false;
                    } catch (Exception)
                    {
                        DesignatedBroker = Brokers[this.Random.Next(0, Brokers.Count)];
                        Utility.DebugLog("Failed sending subscription to broker. Resending");
                    }
                });
                thread.Start();
                thread.Join();
            }
        }

        private void SendUnsubscription(string topic)
        {
            int seqNo;
            lock (this)
            {
                ++OutSequenceNumber;
                seqNo = OutSequenceNumber;
            }

            List<string> publishers;
            if (Topics.TryGetValue(topic, out publishers))
            {
                foreach (string pub in publishers)
                {
                    if (HoldbackQueue.ContainsKey(pub))
                        HoldbackQueue.Remove(pub);
                }
            }

            bool retry = true;
            while (retry)
            {
                Thread thread = new Thread(() =>
                {
                    try
                    {
                        DesignatedBroker.DeliverUnsubscription(ProcessName, topic, SiteName, seqNo);
                        retry = false;
                    } catch (Exception)
                    {
                        DesignatedBroker = Brokers[this.Random.Next(0, Brokers.Count)];
                        Utility.DebugLog("Failed sending unsubscription to broker. Resending");
                    }
                });
                thread.Start();
                thread.Join();
            }
        }

        public override string ToString()
        {
            return "Subscriber";
        }
    }
}