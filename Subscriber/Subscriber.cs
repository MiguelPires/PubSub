using System;
using System.Collections.Generic;
using System.Linq;
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
        // the sequence number used by messages received by the broker group
        public int InSequenceNumber { get; private set; }

        public Subscriber(string processName, string processUrl, string puppetMasterUrl)
            : base(processName, processUrl, puppetMasterUrl)
        {
            Brokers = new List<IBroker>();

            List<string> brokerUrls = GetBrokers(puppetMasterUrl);

            // connect to the brokers at the site
            foreach (string brokerUrl in brokerUrls)
            {
                IBroker parentBroker = (IBroker)Activator.GetObject(typeof(IBroker), brokerUrl);
                parentBroker.RegisterPubSub(ProcessName, Url);
                Brokers.Add(parentBroker);
            }


        }

        public void ProcessFrozenListCommands()
        {
            string[] command;
            while (EventBacklog.TryDequeue(out command))
            {
                DeliverCommand(command);
            }
        }

        public override void DeliverCommand(string[] command)
        {
            if (Status == Status.Frozen)
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
                    string topic = command[1];
                    SendSubscription(topic);
                    break;

                case "Unsubscribe":
                    //string topic = command[1];
                    //Unsubscribe from topic
                    break;

                default:
                    Console.Out.WriteLine("Command: " + command[0] + " doesn't exist!");
                    break;
                    // subscriber specific commands
            }
        }

        void ISubscriber.DeliverPublication(string publication, int sequenceNumber)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// This method sends a subscription to a random broker at this site
        /// </summary>
        /// <param name="topic"> The topic of the subscription </param>
        private void SendSubscription(string topic)
        {
            ++OutSequenceNumber;
           // picks a random broker for load-balancing purposes
            Random rand = new Random();
            int brokerIndex = rand.Next(0, Brokers.Count);
            Thread thread = new Thread(() => Brokers[brokerIndex].DeliverSubscription(this.ProcessName, topic, OutSequenceNumber));
            thread.Start();
        }

        public override string ToString()
        {
            return "Subscriber";
        }
    }
}