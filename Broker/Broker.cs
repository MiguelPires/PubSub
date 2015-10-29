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

        public string SiteName { get; set; }

        public Broker(string name, string url, string puppetMasterUrl, string siteName, string parentSite) : base(name, url, puppetMasterUrl)
        {
            SiteName = siteName;

            // initialize state
            Children = new ConcurrentDictionary<string, List<IBroker>>();
            LocalProcesses = new ConcurrentDictionary<string, IProcess>();
            ParentBrokers = new List<IBroker>();

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
                Console.WriteLine("There already is a process named " + procName +" at this broker (replaced anyway)");

            LocalProcesses[procName] = (IProcess) Activator.GetObject(typeof (IProcess), procUrl);
        }

        public string GetUrl()
        {
            return Url;
        }

        public override void DeliverCommand(string[] command)
        {
            throw new NotImplementedException();
        }

        public override void SendLog(string log)
        {
            throw new NotImplementedException();
        }

        public override string ToString()
        {
            return "Broker";
        }
    }
}