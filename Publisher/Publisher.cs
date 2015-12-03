#region

using System;
using System.Collections.Generic;
using System.Threading;
using CommonTypes;

#endregion

namespace Publisher
{
    internal class Publisher : BaseProcess, IPublisher
    {
        // this class' random instance. Since the default seed is time dependent we don«t
        // want to instantiate every time we send a message
        private readonly Random _random = new Random();
        // this site's brokers
        public List<IBroker> Brokers { get; set; }
        // the sequence number used by messages sent to the broker group
        public int OutSequenceNumber { get; private set; }
        //
        public int EventNumber { get; private set; } = 1;
        // the sent publications
        public ProcessHistory History { get; } = new ProcessHistory();

        public Publisher(string processName, string processUrl, string puppetMasterUrl, string siteName)
            : base(processName, processUrl, puppetMasterUrl, siteName)
        {
            Brokers = new List<IBroker>();
            List<string> brokerUrls = GetBrokers(puppetMasterUrl);

            // connect to the brokers at the site
            foreach (string brokerUrl in brokerUrls)
            {
                Utility.ConnectFunction<IBroker> fun = (string urlToConnect) =>
                {
                    IBroker broker = (IBroker) Activator.GetObject(typeof (IBroker), urlToConnect);
                    broker.RegisterPubSub(ProcessName, Url);

                    return broker;
                };

                try
                {
                    IBroker brokerObject = Utility.TryConnection(fun, brokerUrl);
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
        ///     Delivers a command.
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
                    lock (this)
                    {
                        Console.Out.WriteLine("\tSequence Number: " + OutSequenceNumber);
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

                case "Publish":
                    return Publish(command);

                default:
                    Console.Out.WriteLine("Command: " + command[0] + " doesn't exist!");
                    return false;
                // subscriber specific commands
            }

            return true;
        }

        public void SendPublication(string topic, string publication, int sequenceNumber = -1)
        {
            int seqNo;
            if (sequenceNumber == -1)
            {
                lock (this)
                {
                    ++OutSequenceNumber;
                    seqNo = OutSequenceNumber;
                }
            } else
            {
                seqNo = sequenceNumber;
            }

            History.AddMessage(new[] {topic, publication}, seqNo);
            new Thread(() =>
            {
                bool retry = true;
                while (retry)
                {
                    IBroker broker;
                    lock (Brokers)
                    {
                        int brokerIndex = this._random.Next(Brokers.Count);
                        broker = Brokers[brokerIndex];
                    }

                    Thread subThread = new Thread(() =>
                    {
                        try
                        {
                            broker.DeliverPublication(ProcessName, topic, publication, SiteName, seqNo);
                            retry = false;
                        } catch (Exception)
                        {
                            Utility.DebugLog("Failed sending to broker. Resending");
                        }
                    });
                    subThread.Start();
                    subThread.Join();
                }
            }).Start();
        }

        public void RequestPublication(int sequenceNumber)
        {
            string[] message = History.GetMessage(sequenceNumber);
            if (message == null)
                return;

            SendPublication(message[0], message[1], sequenceNumber);
        }

        private bool Publish(string[] command)
        {
            int numberOfEvents = 0;

            if (!(int.TryParse(command[1], out numberOfEvents)))
            {
                Utility.DebugLog("Publisher " + ProcessName + ": invalid number of events");
                return false;
            }

            string topic = command[2];
            int timeInterval = 0;

            if (!(int.TryParse(command[3], out timeInterval)))
            {
                Utility.DebugLog("Publisher " + ProcessName + ": invalid time interval");
                return false;
            }

            for (int i = 0; i < numberOfEvents; i++)
            {
                string content;
                lock (this)
                {
                    content = ProcessName + "-" + topic + "-" + EventNumber;
                    ++EventNumber;
                }
                Utility.DebugLog("Publishing '" + content + "' on topic " + topic);
                SendPublication(topic, content);
                new Thread(() =>
                {
                    try
                    {
                        PuppetMaster.DeliverLog("PubEvent " + ProcessName + ", " + topic);
                    } catch (Exception)
                    {
                    }
                }).Start();
                Thread.Sleep(timeInterval);
            }
            return true;
        }

        public void NotifyOfLast(string publisher, string fromSite, int sequenceNumber)
        {
            if (OutSequenceNumber > sequenceNumber)
            {
                string[] message = History.GetMessage(sequenceNumber);
                if (message == null)
                {
                    Utility.DebugLog("No message in history for notif");
                    return;
                }

                for (int i = sequenceNumber + 1; i <= OutSequenceNumber; i++)
                {
                    SendPublication(message[0], message[1], i);
                    Utility.DebugLog("Resend message seq no " + i + " after notif");
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

        public override string ToString()
        {
            return "Publisher";
        }
    }
}