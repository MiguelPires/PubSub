using System;
using System.Collections.Generic;
using System.Net;
using CommonTypes;

namespace Publisher
{
    internal class Publisher : BaseProcess, IPublisher
    {
        // this site's brokers
        public List<IBroker> Brokers { get; set; }

        public Publisher(string processName, string processUrl, string puppetMasterUrl)
            : base(processName, processUrl, puppetMasterUrl)
        {
            Brokers = new List<IBroker>();
            List<string> brokerUrls = GetBrokers(puppetMasterUrl);

            // connect to the brokers at the site
            foreach (string brokerUrl in brokerUrls)
            {
                IBroker parentBroker = (IBroker) Activator.GetObject(typeof (IBroker), brokerUrl);
                parentBroker.RegisterPubSub(ProcessName, Url);
                Brokers.Add(parentBroker);
            }
        }
        public override void ProcessFrozenListCommands()
        {
            base.ProcessFrozenListCommands();
            foreach (String[] command in FrozenStateList)
            {
                switch (command[0])
                {
                    case "Publish":
                        //string topic = command[1];
                        //subscribe to topic
                        break;
                }
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
                    base.DeliverCommand(command);
                    break;
                case "Crash":
                    base.DeliverCommand(command);
                    break;
                case "Freeze":
                    base.DeliverCommand(command);
                    break;
                case "Unfreeze":
                    base.DeliverCommand(command);
                    break;
                case "Publish":
                    var numberOfEvents = 0;

                    if ( !(int.TryParse(command[1], out numberOfEvents)))
                    {
                        // Parsing was not successful..
                        Console.Out.WriteLine("int parse failed (numberOfEvents)");
                        return;
                    }
                    var topic = command[3];
                    var timeInterval = 0;
                    if (!(int.TryParse(command[5], out timeInterval)))
                    {
                        // Parsing was not successful..
                        Console.Out.WriteLine("int parse failed (timeInterval)");
                        return;
                    }
                    for (int i = 0; i < numberOfEvents; i++)
                    {
                        string content = ProcessName + i;
                       // sendPublication(topic,content);
                        System.Threading.Thread.Sleep(timeInterval);
                    }
                    break;

                default:
                    Console.Out.WriteLine("Command: " + command[0] + " doesn't exist!");
                    break;
                    // subscriber specific commands
            }
        }

        void IPublisher.SendPublication(string publication)
        {
            throw new NotImplementedException();
        }

        public override string ToString()
        {
            return "Publisher";
        }
    }
}