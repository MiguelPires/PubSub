using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using Broker;
using CommonTypes;

namespace Subscriber
{
    internal class Subscriber : BaseProcess, ISubscriber
    {
        // this site's brokers
        public List<IBroker> Brokers { get; set; }
        // 
        public IDictionary<string, int> SequenceNumbers { get; private set; }
        // 
        public IDictionary<string, CommandQueue> HoldbackQueue { get; }
        //
        public IDictionary<string, List<string>> Topics { get; }

        public Subscriber(string processName, string processUrl, string puppetMasterUrl, string siteName)
            : base(processName, processUrl, puppetMasterUrl, siteName)
        {
            Brokers = new List<IBroker>();
            SequenceNumbers = new ConcurrentDictionary<string, int>();
            HoldbackQueue = new ConcurrentDictionary<string, CommandQueue>();
            Topics = new ConcurrentDictionary<string, List<string>>();

            var brokerUrls = GetBrokers(puppetMasterUrl);

            // connect to the brokers at the site
            foreach (var brokerUrl in brokerUrls)
            {
                UtilityFunctions.ConnectFunction<IBroker> fun = (string url) =>
                    {
                        IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), url);
                        broker.RegisterPubSub(ProcessName, Url);

                        return broker;
                    };

                IBroker brokerObject = UtilityFunctions.TryConnection<IBroker>(fun, 500, 5, brokerUrl);
                Brokers.Add(brokerObject);
            }
        }

        public override void DeliverCommand(string[] command)
        {
            lock (this)
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

                        Console.Out.WriteLine("\t" + "Publications in HoldBack queue: ");
                        foreach (var pub in HoldbackQueue.Keys)
                        {
                            CommandQueue queue = HoldbackQueue[pub];
                            ICollection<int> seqNums = queue.GetSequenceNumbers();

                            if (seqNums.Count == 0)
                                continue;

                            Console.Out.Write("\tPublisher '" + pub+"' has messages " );

                            foreach (var seqNum in queue.GetSequenceNumbers())
                            {
                                Console.Out.Write(seqNum+" ");

                            }

                            Console.Out.WriteLine("in HoldBack queue");

                        }
                        Console.Out.WriteLine("*******************\t\n");
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
        }

        public void DeliverPublication(string publication, string topic, string process, int sequenceNumber)
        {
            // TODO: retirar o lock
            lock (this)
            {
                if (OrderingGuarantee == OrderingGuarantee.No)
                {
                    Console.Out.WriteLine("Received publication '" + publication + "'");
                    return;
                }

                int seqNum;
                if (SequenceNumbers.TryGetValue(process, out seqNum))
                {
                    if (sequenceNumber > seqNum + 1)
                    {
                        CommandQueue queue;
                        if (!HoldbackQueue.TryGetValue(process, out queue))
                            queue = new CommandQueue();

                        Console.Out.WriteLine("Queueing publication '"+publication+"' with seq "+sequenceNumber);
                        queue.AddCommand(new string[] {publication, topic, process}, sequenceNumber);
                        HoldbackQueue[process] = queue;

                    } else if (sequenceNumber == seqNum + 1)
                    {
                        ++SequenceNumbers[process];
                        Console.Out.WriteLine("Received publication '" + publication + "' with seq no "+ sequenceNumber);

                        Thread thread = new Thread(() => PuppetMaster.DeliverLog("SubEvent " + ProcessName + ", " + process + ", " + topic));
                        thread.Start();

                        CommandQueue queue;
                        if (HoldbackQueue.TryGetValue(process, out queue))
                        {
                            string[] command = queue.GetCommandAndRemove(sequenceNumber + 1);
                            if (command == null)
                                return;
                            Console.Out.WriteLine("Unblocking publication with seq "+(sequenceNumber+1));
                            
                            DeliverPublication(command[0], command[1], command[2], sequenceNumber + 1);
                        }

                    }
                    else
                    {
                        Console.Out.WriteLine("Received previous: "+sequenceNumber);
                    }
                }
                else
                {
                    SequenceNumbers[process] = sequenceNumber;
                    Console.Out.WriteLine("Setting baseline for "+process+" at "+sequenceNumber);
                    Console.Out.WriteLine("Received publication '" + publication + "' with seq no " + sequenceNumber);
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
            int brokerIndex = rand.Next(0, Brokers.Count);
            Thread thread =
                new Thread(() => Brokers[brokerIndex].DeliverSubscription(ProcessName, topic, SiteName));
            
            thread.Start();
            thread.Join();

        }

        private void SendUnsubscription(string topic)
        {
            lock (this)
            {


                // picks a random broker for load-balancing purposes
                Random rand = new Random();
                int brokerIndex = rand.Next(0, Brokers.Count);

                List<string> publishers;
                if (Topics.TryGetValue(topic, out publishers))
                {
                    foreach (var pub in publishers)
                    {
                        if (HoldbackQueue.ContainsKey(pub))
                            HoldbackQueue.Remove(pub);
                    }
                }

                Thread thread =
                    new Thread(() => Brokers[brokerIndex].DeliverUnsubscription(ProcessName, topic, SiteName));
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