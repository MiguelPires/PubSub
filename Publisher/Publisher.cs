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
        //
        public int EventNumber { get; private set; }

        public Publisher(string processName, string processUrl, string puppetMasterUrl, string siteName)
            : base(processName, processUrl, puppetMasterUrl, siteName)
        {
            Brokers = new List<IBroker>();
            List<string> brokerUrls = GetBrokers(puppetMasterUrl);
            OutSequenceNumber = 0;
            EventNumber = 0;

            // connect to the brokers at the site
            foreach (string brokerUrl in brokerUrls)
            {
                UtilityFunctions.ConnectFunction<IBroker> fun = (string urlToConnect) =>
                {
                    IBroker broker = (IBroker)Activator.GetObject(typeof(IBroker), urlToConnect);
                    broker.RegisterPubSub(ProcessName, Url);

                    return broker;
                };

                Random rand = new Random();
                int sleepTime = rand.Next(100, 1100);
                var brokerObject = UtilityFunctions.TryConnection<IBroker>(fun, sleepTime, 15, brokerUrl);
                Brokers.Add(brokerObject);
            }
        }

        public override void DeliverCommand(string[] command)
        {
            // TODO - acabar de remover os locks e testar com os outros configs
            // TODO - acho que ha um problema nas mensagens delayed
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
                        Console.Out.WriteLine("\tSequence Number: "+ OutSequenceNumber);
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
                            string content = ProcessName +"-"+ topic+"-"+EventNumber;
                            Console.Out.WriteLine("Publishing '" + content + "' on topic " + topic);
                            SendPublication(topic, content);
                            Thread.Sleep(timeInterval);
                            EventNumber++;
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

            IBroker broker;
            lock (Brokers)
            {
                int brokersIndex = rand.Next(0, Brokers.Count);
                broker = Brokers[brokersIndex];
            }

            if (this.OrderingGuarantee == OrderingGuarantee.Fifo)
                ++OutSequenceNumber;
            Thread thread =
              new Thread(() => PuppetMaster.DeliverLog("PubEvent " + ProcessName + ", " + topic));
            thread.Start();

            thread =new Thread(() => broker.DeliverPublication(ProcessName, topic, publication, SiteName, OutSequenceNumber));
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