using System;
using System.Collections.Generic;

namespace Broker
{
    public class SubscriptionSet
    {
        // the subscription's topic
        public string Topic { get; }
        // the list of pairs <process, site>
        public IDictionary<string, string> Processes { get; }

        public SubscriptionSet(string topic)
        {
            Topic = topic;
            Processes = new Dictionary<string, string>();
        }

        public void AddSubscriber(string processName, string siteName)
        {
            string site;
            if (Processes.TryGetValue(processName, out site))
                Console.Out.WriteLine(processName + " is already subscribed to " + Topic + "; Forward to " + site);
            Processes[processName] = siteName;
        }

        public void RemoveSubscriber(string processName)
        {
            Processes.Remove(processName);
        }

        public bool IsSubscribed(string processName)
        {
            string site;
            return Processes.TryGetValue(processName, out site);
        }

        public ICollection<string> GetMatchList()
        {
            return Processes.Keys;
        }
    }
}