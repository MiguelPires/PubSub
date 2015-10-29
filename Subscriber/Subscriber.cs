using System;
using System.Collections.Generic;
using CommonTypes;

namespace Subscriber
{
    internal class Subscriber : BaseProcess
    {
        // this site's brokers
        public List<IBroker> Brokers { get; set; }

        public Subscriber(string processName, string processUrl, string puppetMasterUrl) : base (processName, processUrl, puppetMasterUrl)
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

        public override string ToString()
        {
            return "Subscriber";
        }

        public override void DeliverCommand(string[] command)
        {
            throw new NotImplementedException();
        }

        public override void SendLog(string log)
        {
            throw new NotImplementedException();
        }
    }
}