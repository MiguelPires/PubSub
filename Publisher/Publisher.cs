using System;
using System.Collections.Generic;
using System.Net.Mime;
using System.Net.Sockets;
using System.Threading;
using CommonTypes;

namespace Publisher
{
    internal class Publisher : BaseProcess, IPublisher
    {
        // this site's brokers
        public List<IBroker> Brokers { get; set; }
        // the sequence number used by messages sent to the broker group
        public int OutSequenceNumber { get; private set; }

        public Publisher(string processName, string processUrl, string puppetMasterUrl, string siteName)
            : base(processName, processUrl, puppetMasterUrl, siteName)
        {
            Brokers = new List<IBroker>();
            List<string> brokerUrls = GetBrokers(puppetMasterUrl);
            OutSequenceNumber = 0;
            // connect to the brokers at the site
            foreach (string brokerUrl in brokerUrls)
            {
                UtilityFunctions.ConnectFunction<IBroker> fun = (string urlToConnect) =>
                {
                    IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), urlToConnect);
                    broker.RegisterPubSub(ProcessName, Url);

                    return broker;
                };

                var brokerObject = UtilityFunctions.TryConnection<IBroker>(fun, 500, 5, brokerUrl);
                Brokers.Add(brokerObject);
            }
        }

        public override void DeliverCommand(string[] command)
        {
            lock(this)
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

                    case "Publish":
                        int numberOfEvents = 0;

                        if (!(int.TryParse(command[1], out numberOfEvents)))
                        {
                            Console.Out.WriteLine("Publisher " + ProcessName + ": invalid number of events");
                            return;
                        }

                        string topic = command[2];
                        int timeInterval = 0;

                        if (!(int.TryParse(command[3], out timeInterval)))
                        {
                            Console.Out.WriteLine("Publisher " + ProcessName + ": invalid time interval");
                            return;
                        }

                        for (int i = 0; i < numberOfEvents; i++)
                        {
                            string content = ProcessName +"-"+ topic+"-"+i;
                            Console.Out.WriteLine("Publishing '" + content + "' on topic " + topic);
                            SendPublication(topic, content);
                            Thread.Sleep(timeInterval);
                        }
                        break;

                    default:
                        Console.Out.WriteLine("Command: " + command[0] + " doesn't exist!");
                        break;
                        // subscriber specific commands
                }
            }
        }

        public void SendPublication(string topic, string publication)
        {
            Random rand = new Random();
            int brokersIndex = rand.Next(0, Brokers.Count);
            IBroker broker = Brokers[brokersIndex];

            if (this.OrderingGuarantee == OrderingGuarantee.Fifo)
                ++OutSequenceNumber;
            Thread thread =
              new Thread(() => PuppetMaster.DeliverLog("PubEvent " + ProcessName + ", " + topic));
            thread.Start();

            thread =
                          new Thread(() => broker.DeliverPublication(ProcessName, topic, publication, SiteName, OutSequenceNumber));
            thread.Start();
            thread.Join();
        }

        public void ProcessFrozenListCommands()
        {
            string[] command;
            while (CommandBacklog.TryDequeue(out command))
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