using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Broker;
using CommonTypes;

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

                Random rand = new Random();
                int sleepTime = rand.Next(100, 1100);
                IBroker brokerObject = UtilityFunctions.TryConnection(fun, sleepTime, 15, brokerUrl);
                Brokers.Add(brokerObject);
            }
        }

        public override void DeliverCommand(string[] command)
        {
            if (Status == Status.Frozen && !command[0].Equals("Unfreeze"))
            {
                base.DeliverCommand(command);
                return;
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
                    break;
            }
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

        public void DeliverPublication(string publication, string topic, string process, int sequenceNumber)
        {
            if (this.OrderingGuarantee == OrderingGuarantee.No)
            {
                Console.Out.WriteLine("Received publication '" + publication + "'");
                return;
            }

            if (PublicationReceived(publication, topic, process, sequenceNumber))
            {
                Console.Out.WriteLine("Received publication '" + publication + "' with seq no " + sequenceNumber);
                PublicationProcessed(topic, process, sequenceNumber);
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
       
        private bool PublicationReceived(string publication, string topic, string process, int sequenceNumber)
        {
            lock (HoldbackQueue)
            {
                int seqNum;
                if (SequenceNumbers.TryGetValue(process, out seqNum))
                {
                    if (sequenceNumber > seqNum + 1)
                    {
                        MessageQueue queue;
                        if (!HoldbackQueue.TryGetValue(process, out queue))
                            queue = new MessageQueue();

                        Console.Out.WriteLine("Queueing publication '" + publication + "' with seq " + sequenceNumber);
                        queue.AddCommand(new[] {publication, topic, process}, sequenceNumber);
                        HoldbackQueue[process] = queue;
                        return false;
                    } else if (sequenceNumber < seqNum +1)
                    {
                        Console.Out.WriteLine("Received previous publication with seqNo " + sequenceNumber+". Ignoring");
                        return false;
                    }
                }
                else
                {
                    SequenceNumbers[process] = sequenceNumber;
                    Console.Out.WriteLine("Setting baseline for " + process + " at " + sequenceNumber);
                }
                return true;
            }
        }

        /// <summary>
        ///     Updates this subscriber's state. Updates sequence numbers, unblocks delayed messages (if any), etc
        /// </summary>
        /// <param name="topic"> The subscribed topic </param>
        /// <param name="process"> The publisher </param>
        /// <param name="sequenceNumber"> The message's sequence number </param>
        private void PublicationProcessed(string topic, string process, int sequenceNumber)
        {
            lock (HoldbackQueue)
            {
                int seqNum;
                if (SequenceNumbers.TryGetValue(process, out seqNum) && sequenceNumber == seqNum + 1)
                {
                    ++SequenceNumbers[process];
                    Thread thread =
                        new Thread(
                            () => PuppetMaster.DeliverLog("SubEvent " + ProcessName + ", " + process + ", " + topic));
                    thread.Start();

                    MessageQueue queue;
                    if (HoldbackQueue.TryGetValue(process, out queue))
                    {
                        string[] command = queue.GetCommandAndRemove(sequenceNumber + 1);
                        if (command == null)
                            return;
                        Console.Out.WriteLine("Unblocking publication with seq " + (sequenceNumber + 1));

                        thread = new Thread(
                            () => DeliverPublication(command[0], command[1], command[2], sequenceNumber + 1));
                        thread.Start();
                    }
                }
            }
        }

        public void ProcessFrozenListCommands()
        {
            string[] command;
            while (CommandBacklog.TryDequeue(out command))
            {
                DeliverCommand(command);
            }
        }

        /// <summary>
        ///     This method sends a subscription to a random broker at this site
        /// </summary>
        /// <param name="topic"> The topic of the subscription </param>
        private void SendSubscription(string topic)
        {
            // picks a random broker for load-balancing purposes
            Random rand = new Random();

            Thread thread;
            lock (Brokers)
            {
                int brokerIndex = rand.Next(0, Brokers.Count);
                thread =
                    new Thread(() => Brokers[brokerIndex].DeliverSubscription(ProcessName, topic, SiteName));
            }

            thread.Start();
            thread.Join();
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

            // picks a random broker for load-balancing purposes
            Random rand = new Random();

            Thread thread;
            lock (Brokers)
            {
                int brokerIndex = rand.Next(0, Brokers.Count);
                thread = new Thread(() => Brokers[brokerIndex].DeliverUnsubscription(ProcessName, topic, SiteName));
            }

            thread.Start();
            thread.Join();
        }

        public override string ToString()
        {
            return "Subscriber";
        }
    }
}