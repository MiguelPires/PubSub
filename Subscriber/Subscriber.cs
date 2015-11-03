using System;
using System.Collections.Generic;
using System.Linq;
using CommonTypes;

namespace Subscriber
{
    internal class Subscriber : BaseProcess, ISubscriber
    {
        // this site's brokers
        public List<IBroker> Brokers { get; set; }

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
                    //string topic = command[1];
                    //subscribe to topic
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

        void ISubscriber.DeliverPublication(string publication)
        {
            throw new NotImplementedException();
        }

        public override string ToString()
        {
            return "Subscriber";
        }
    }
}