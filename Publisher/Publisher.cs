﻿using System;
using System.Collections.Generic;
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

        public override void DeliverCommand(string[] command)
        {
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

               // publisher specific commands
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