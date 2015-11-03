using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CommonTypes;

namespace Broker
{
    public class SubscriptionSet
    {
        // the subscription's topic
        public string Topic { get; private set; }
        // the list of pairs <broker, numSubscriptions>
        public IDictionary<string, int> Brokers { get; private set; }

        public SubscriptionSet(string topic)
        {
            Topic = topic;
            Brokers = new Dictionary<string, int>();

        }

        public void AddSubscriber(string brokerName)
        {
            int numSubs;

            if (Brokers.TryGetValue(brokerName, out numSubs))
                Brokers[brokerName] = ++numSubs;
            else
                Brokers.Add(brokerName, 1);
        }

        public void RemoveSubscriber(string brokerName)
        {
            int numSubs;
            if (Brokers.TryGetValue(brokerName, out numSubs))
                Brokers[brokerName] = --numSubs;

            if (numSubs <= 0)
                Brokers.Remove(brokerName);
        }

        public bool IsSubscribed(string brokerName)
        {
            int numSubs;
            return Brokers.TryGetValue(brokerName, out numSubs);
        }

        public ICollection<string> GetMatchList()
        {
            return Brokers.Keys;
        }
    }
}
