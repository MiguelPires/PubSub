using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using CommonTypes;

namespace Broker
{
    public class Broker : BaseProcess, IBroker
    {
        // maps a site's name to a list of the site's brokers
        public IDictionary<string, List<IBroker>> Children { get; }
        // this broker's parent brokers
        public List<IBroker> ParentBrokers { get; }
        // maps a process name to a process instance
        public IDictionary<string, IProcess> LocalProcesses { get; }
        // this site's name
        public string SiteName { get; set; }
        // a thread-safe queue with the event backlog (events that are yet to be processed)
        public ConcurrentQueue<string[]> Backlog { get; private set; }

        public Broker(string name, string url, string puppetMasterUrl, string siteName, string parentSite)
            : base(name, url, puppetMasterUrl)
        {
            SiteName = siteName;

            // initialize state
            Children = new ConcurrentDictionary<string, List<IBroker>>();
            LocalProcesses = new ConcurrentDictionary<string, IProcess>();
            ParentBrokers = new List<IBroker>();
            Backlog = new ConcurrentQueue<string[]>();

            if (parentSite.Equals("none"))
                return;

            // obtain the parent site's brokers urls
            string parentUrl = UtilityFunctions.GetUrl(parentSite);
            List<string> brokerUrls = GetBrokers(parentUrl);

            // connect to the brokers at the parent site
            foreach (string brokerUrl in brokerUrls)
            {
                IBroker parentBroker = (IBroker) Activator.GetObject(typeof (IBroker), brokerUrl);
                parentBroker.RegisterBroker(SiteName, Url);
                ParentBrokers.Add(parentBroker);
            }
        }

        public void RegisterBroker(string siteName, string brokerUrl)
        {
            List<IBroker> siteBrokers;
            try
            {
                siteBrokers = Children[siteName];
            }
            catch (KeyNotFoundException)
            {
                siteBrokers = new List<IBroker>();
            }

            siteBrokers.Add((IBroker) Activator.GetObject(typeof (IBroker), brokerUrl));
            Children[siteName] = siteBrokers;
        }

        public void RegisterPubSub(string procName, string procUrl)
        {
            if (LocalProcesses.ContainsKey(procName))
                Console.WriteLine("There already is a process named " + procName + " at this broker (replaced anyway)");

            LocalProcesses[procName] = (IProcess) Activator.GetObject(typeof (IProcess), procUrl);
        }

        void IBroker.DeliverSubscription(string procName, string topic)
        {
            if (Status.Equals(Status.Frozen))
            {
                string[] eventMessage = new string[4];
                eventMessage[0] = "DeliverSubscription";
                eventMessage[1] = procName;
                eventMessage[2] = topic;
            }
            else
            {
                Console.Out.WriteLine("Deliver subscription");
            }
        }

        void IBroker.DeliverPublication(string topic, string publication)
        {
            if (Status.Equals(Status.Frozen))
            {
                string[] eventMessage = new string[4];
                eventMessage[0] = "DeliverPublication";
                eventMessage[1] = topic;
                eventMessage[2] = publication;
            }
            else
            {
                Console.Out.WriteLine("Deliver publication");
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
                case "Crash":
                case "Freeze":
                case "Unfreeze":
                    base.DeliverCommand(command);
                    break;

                // broker specific command
            }
        }

        public string GetUrl()
        {
            return Url;
        }

        public override string ToString()
        {
            return "Broker";
        }
    }
}