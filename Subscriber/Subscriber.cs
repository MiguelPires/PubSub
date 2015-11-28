#region

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.Remoting;
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
        public IDictionary<string, int> SequenceNumbers { get; private set; } = new ConcurrentDictionary<string, int>();
        // a hold-back queue that stores delayed messages
        public IDictionary<string, MessageQueue> HoldbackQueue { get; } =
            new ConcurrentDictionary<string, MessageQueue>();

        // 
        public IDictionary<string, List<string>> Topics { get; } = new ConcurrentDictionary<string, List<string>>();

        public Subscriber(string processName, string processUrl, string puppetMasterUrl, string siteName)
            : base(processName, processUrl, puppetMasterUrl, siteName)
        {
            List<string> brokerUrls = GetBrokers(puppetMasterUrl);

            // connect to the brokers at the site
            foreach (string brokerUrl in brokerUrls)
            {
                UtilityFunctions.ConnectFunction<IBroker> fun = (string url) =>
                {
                    IBroker broker = (IBroker) Activator.GetObject(typeof (IBroker), url);
                    broker.RegisterPubSub(ProcessName, Url);

                    return broker;
                };

                try
                {
                    IBroker brokerObject = UtilityFunctions.TryConnection(fun, brokerUrl);
                    Brokers.Add(brokerObject);
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
                    Console.Out.WriteLine("Unfreezing");
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
                    Console.Out.WriteLine("Command: " + command[0] + " doesn't exist!");
                    return false;
            }
            return true;
        }

        private void PrintStatus()
        {
            if (HoldbackQueue.Keys.Count == 0)
            {
                Console.Out.WriteLine("\t There are no delayed publications");
                Console.Out.WriteLine("*******************\t\n");
                return;
            }

            Console.Out.WriteLine("\tPublications in HoldBack queue: ");
            foreach (string pub in HoldbackQueue.Keys)
            {
                MessageQueue queue = HoldbackQueue[pub];
                ICollection<int> seqNums = queue.GetSequenceNumbers();

                if (seqNums.Count == 0)
                    continue;

                Console.Out.Write("\tPublisher '" + pub + "' has messages ");

                foreach (int seqNum in queue.GetSequenceNumbers())
                {
                    Console.Out.Write(seqNum + " ");
                }

                Console.Out.WriteLine("in HoldBack queue");
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
                    Console.Out.WriteLine("Received publication '" + publication + "' with seq no " + sequenceNumber);
                    PublicationProcessed(publisher, topic, sequenceNumber);
                }
            }
        }

        /// <summary>
        ///     Decides what to do with the publication. 
        /// </summary>
        /// <param name="origin"></param>
        /// <param name="topic"></param>
        /// <param name="publication"></param>
        /// <param name="sequenceNumber"></param>
        /// <returns> Returns true if it should be further processed or false if it shouldn't </returns>
        private bool PublicationReceived(string publisher, string topic, string publication, int sequenceNumber)
        {
            int seqNum;
            if (!SequenceNumbers.TryGetValue(publisher, out seqNum))
            {
                seqNum = 0;
                SequenceNumbers[publisher] = 0;
            }

            // we queue if the ordering is incorrect or we're frozen
            if ((OrderingGuarantee == OrderingGuarantee.Fifo &&sequenceNumber > seqNum + 1) || 
                Status == Status.Frozen)
            {
                MessageQueue queue;
                if (!HoldbackQueue.TryGetValue(publisher, out queue))
                    queue = new MessageQueue();

                Console.Out.WriteLine("Queueing publication '" + publication + "' with seq " + sequenceNumber);
                queue.Add(new[] { publisher, publication, topic, sequenceNumber.ToString()}, sequenceNumber);
                HoldbackQueue[publisher] = queue;

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

            // if everything is right with the ordering we deliver
            if (OrderingGuarantee == OrderingGuarantee.No || sequenceNumber == seqNum + 1)
                return true;

            Console.Out.WriteLine("Received previous publication with seqNo " + sequenceNumber + ". Ignoring");
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
            ++SequenceNumbers[publisher];
            Thread thread =
                new Thread(
                    () => PuppetMaster.DeliverLog("SubEvent " + ProcessName + ", " + publisher + ", " + topic));
            thread.Start();

            MessageQueue queue;
            if (HoldbackQueue.TryGetValue(publisher, out queue))
            {
                string[] command = queue.GetAndRemove(sequenceNumber + 1);
                if (command == null)
                    return;
                Console.Out.WriteLine("Unblocking publication with seq " + (sequenceNumber + 1));

                thread = new Thread(
                    () => DeliverPublication(command[0], command[1], command[2], sequenceNumber + 1));
                thread.Start();
            }
        }

        /// <summary>
        ///     Requests a specific publication for a broker
        /// </summary>
        /// <param name="publisher"></param>
        /// <param name="sequenceNumber"></param>
        private void Request(string publisher, int sequenceNumber)
        {
            Console.Out.WriteLine("Requesting resend of seq no "+(sequenceNumber-1)+" from broker");
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
                                    broker.RequestPublication(publisher, SiteName, sequenceNumber - 1, ProcessName);
                                    retry = false;
                                }
                                catch (RemotingException)
                                {
                                }
                                catch (SocketException)
                                {
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
            bool retry = true;
            while (retry)
            {
                // picks a random broker for load-balancing purposes
                int brokerIndex;
                lock (Brokers)
                {
                    brokerIndex = Random.Next(0, Brokers.Count);
                }

                Thread thread = new Thread(() =>
                {
                    try
                    {
                        Brokers[brokerIndex].DeliverSubscription(ProcessName, topic, SiteName);
                        retry = false;
                    } catch (Exception)
                    {
                        Console.Out.WriteLine("Failed sending subscription to broker. Resending");
                    }
                });
                thread.Start();
                thread.Join();
            }
        }

        private void SendUnsubscription(string topic)
        {
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
                // picks a random broker for load-balancing purposes
                int brokerIndex;
                lock (Brokers)
                {
                    brokerIndex = Random.Next(0, Brokers.Count);
                }

                Thread thread = new Thread(() =>
                {
                    try
                    {
                        Brokers[brokerIndex].DeliverUnsubscription(ProcessName, topic, SiteName);
                        retry = false;
                    } catch (Exception)
                    {
                        Console.Out.WriteLine("Failed sending unsubscription to broker. Resending");
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