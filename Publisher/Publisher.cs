using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using CommonTypes;

namespace Publisher
{
    internal class Publisher : BaseProcess, IPublisher
    {
        // this site's brokers
        public List<IBroker> Brokers { get; set; }

        public Publisher(string processName, string processUrl, string puppetMasterUrl, string siteName)
            : base(processName, processUrl, puppetMasterUrl, siteName)
        {
            Brokers = new List<IBroker>();
            List<string> brokerUrls = GetBrokers(puppetMasterUrl);

            // connect to the brokers at the site
            foreach (string brokerUrl in brokerUrls)
            {
                try
                {
                    IBroker parentBroker = (IBroker) Activator.GetObject(typeof (IBroker), brokerUrl);
                    parentBroker.RegisterPubSub(ProcessName, Url);
                    Brokers.Add(parentBroker);
                }
                catch (SocketException)
                {
                    Console.Out.WriteLine(processName+" couldn't connect to "+brokerUrl);
                }
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
                case "Crash":
                case "Freeze":
                    base.DeliverCommand(command);
                    break;

                case "Unfreeze":
                    Console.Out.WriteLine("Unfreezing");
                    Status = Status.Unfrozen;
                    ProcessFrozenListCommands();
                    break;

                case "Publish":
                    int numberOfEvents = 0;

                    if (!(int.TryParse(command[1], out numberOfEvents)))
                    {
                        Console.Out.WriteLine("Publisher " + this + ": invalid number of events");
                        return;
                    }

                    string topic = command[2];
                    int timeInterval = 0;

                    if (!(int.TryParse(command[3], out timeInterval)))
                    {
                        Console.Out.WriteLine("Publisher " + this + ": invalid time interval");
                        return;
                    }

                    for (int i = 0; i < numberOfEvents; i++)
                    {
                        string content = ProcessName + i;
                        // sendPublication(topic,content);
                        Thread.Sleep(timeInterval);
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

        public void ProcessFrozenListCommands()
        {
            string[] command;
            while (EventBacklog.TryDequeue(out command))
            {
                DeliverCommand(command);
            }
        }

        public override string ToString()
        {
            return "Publisher";
        }
    }
}