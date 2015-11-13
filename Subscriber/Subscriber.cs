using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using CommonTypes;

namespace Subscriber
{
    internal class Subscriber : BaseProcess, ISubscriber
    {
        // this site's brokers
        public List<IBroker> Brokers { get; set; }
        // the sequence number used by messages sent to the broker group
        public int OutSequenceNumber { get; private set; }

        public Subscriber(string processName, string processUrl, string puppetMasterUrl, string siteName)
            : base(processName, processUrl, puppetMasterUrl, siteName)
        {
            Brokers = new List<IBroker>();
            var brokerUrls = GetBrokers(puppetMasterUrl);
            OutSequenceNumber = 0;
            // connect to the brokers at the site
            foreach (var brokerUrl in brokerUrls)
            {
                UtilityFunctions.ConnectFunction<IBroker> fun = (string url) =>
                    {
                        IBroker parentBroker = (IBroker)Activator.GetObject(typeof(IBroker), url);
                        parentBroker.RegisterPubSub(ProcessName, Url);

                        return parentBroker;
                    };

                var parBroker = UtilityFunctions.TryConnection<IBroker>(fun, 500, 5, brokerUrl);
                Brokers.Add(parBroker);
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
                    Console.Out.WriteLine("\t" + "Sequence Number: {0}", OutSequenceNumber);
                    Console.Out.WriteLine("\t" + "*******************");
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

        void ISubscriber.DeliverPublication(string publication, int sequenceNumber)
        {
            // TODO: add sequence number checking
                Console.Out.WriteLine("Received publication '" + publication + "'");
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

            if (this.OrderingGuarantee == OrderingGuarantee.Fifo)
                ++OutSequenceNumber;

            Thread thread =
                new Thread(() => Brokers[brokerIndex].DeliverSubscription(ProcessName, topic, SiteName, OutSequenceNumber));
            thread.Start();
        }

        private void SendUnsubscription(string topic)
        {
            // picks a random broker for load-balancing purposes
            Random rand = new Random();
            int brokerIndex = rand.Next(0, Brokers.Count);

            if (this.OrderingGuarantee == OrderingGuarantee.Fifo)
                ++OutSequenceNumber;

            Thread thread =
                new Thread(() => Brokers[brokerIndex].DeliverUnsubscription(ProcessName, topic, SiteName, OutSequenceNumber));
            thread.Start();
        }
        public override string ToString()
        {
            return "Subscriber";
        }
    }
}