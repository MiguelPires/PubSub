using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.Remoting.Channels;
using System.Threading;
using CommonTypes;

namespace Broker
{
    public class Broker : BaseProcess, IBroker, IReplica
    {
        // maps a site's name to a list of the site's brokers
        public IDictionary<string, List<IBroker>> Children { get; }
        // this broker's parent brokers
        public List<IBroker> ParentBrokers { get; }
        //the list of other brokers at this site
        public List<IBroker> SiblingBrokers { get; private set; }
        // maps a process name to a process instance
        public IDictionary<string, IProcess> LocalProcesses { get; }
        // this site's name
        public string SiteName { get; set; }
        // a table that maps subscriptions to local processes
        public IDictionary<string, List<string>> LocalSubscriptions { get; private set; }
        // a table that maps subscriptions to other brokers
        public IDictionary<string, SubscriptionSet> RoutingTable { get; private set; }
        // the sequence number for messages received by other processes
        public IDictionary<string, int> InSequenceNumbers { get; private set; }
        // the sequence number for messages sent to other processes
        public IDictionary<string, int> OutSequenceNumbers { get; private set; }
        // maps a process/group to it's queue of held back commands
        public IDictionary<string, CommandQueue> HoldbackQueue { get; private set; }

        public Broker(string name, string url, string puppetMasterUrl, string siteName, string parentSite)
            : base(name, url, puppetMasterUrl)
        {
            SiteName = siteName;

            // initialize state
            Children = new Dictionary<string, List<IBroker>>();
            LocalProcesses = new Dictionary<string, IProcess>();
            ParentBrokers = new List<IBroker>();
            SiblingBrokers = new List<IBroker>();
            LocalSubscriptions = new Dictionary<string, List<string>>();
            RoutingTable = new Dictionary<string, SubscriptionSet>();
            HoldbackQueue = new Dictionary<string, CommandQueue>();
            InSequenceNumbers = new Dictionary<string, int>();
            OutSequenceNumbers = new Dictionary<string, int>();     
        
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
            OutSequenceNumbers[siteName] = 0;
        }

        public void RegisterPubSub(string procName, string procUrl)
        {
            if (LocalProcesses.ContainsKey(procName))
                Console.WriteLine("There already is a process named " + procName + " at this broker (replaced anyway)");

            LocalProcesses[procName] = (IProcess) Activator.GetObject(typeof (IProcess), procUrl);
            OutSequenceNumbers[procName] = 0;
        }

        public void RegisterSiblingBroker(string siblingUrl)
        {
            // connect to the sibling and add it to the list
        }

        public void DeliverSubscription(string origin, string topic, int sequenceNumber)
        {
            if (Status.Equals(Status.Frozen))
            {
                string[] eventMessage = new string[4];
                eventMessage[0] = "DeliverSubscription";
                eventMessage[1] = origin;
                eventMessage[2] = topic;
                eventMessage[3] = sequenceNumber.ToString();
                return;
            }

            int lastNumber;
            if (InSequenceNumbers.TryGetValue(origin, out lastNumber))
                InSequenceNumbers[origin] = 0;
            else
                lastNumber = 0;

            if (OrderingGuarantee == OrderingGuarantee.Fifo && sequenceNumber > lastNumber + 1)
            {
                string[] eventMessage = new string[4];
                eventMessage[0] = "DeliverSubscription";
                eventMessage[1] = origin;
                eventMessage[2] = topic;
                eventMessage[3] = sequenceNumber.ToString();

                CommandQueue queue;

                if (HoldbackQueue.TryGetValue(origin, out queue))
                {
                    queue.AddCommand(eventMessage, sequenceNumber);
                }
                else
                {
                    queue = new CommandQueue();      
                    queue.AddCommand(eventMessage, sequenceNumber);
                    HoldbackQueue[origin] = queue;
                }
            }
            else
            {
                Console.Out.WriteLine("Receiving subscription on topic "+topic+" from  "+origin);
             
                // if the process is local
                if (LocalProcesses.Keys.Contains(origin))
                {
                    // add process to the local subscriptions
                    List<string> processes;
                    if (LocalSubscriptions.TryGetValue(topic, out processes))
                        LocalSubscriptions[topic].Add(origin);
                    else
                        LocalSubscriptions[topic] = new List<string> {origin};

                    // send the new state to the other brokers
                    PropagateLocalSubscription(topic, origin);
                    Console.Out.WriteLine("Local subscription");
                }
                else
                {
                    Console.Out.WriteLine("Remote subscription");
                    // get or create the SubscriptionSet for this topic
                    SubscriptionSet subscriptionSet;
                    try
                    {
                        subscriptionSet = RoutingTable[topic];
                    }
                    catch (KeyNotFoundException)
                    {
                        // if there is no SubscriptionSet for this topic
                        subscriptionSet = new SubscriptionSet(topic);
                        subscriptionSet.AddSubscriber(origin);
                        RoutingTable[topic] = subscriptionSet;
                    }

                    if (subscriptionSet.IsSubscribed(origin))
                    {
                        Console.Out.WriteLine("This subscription is already present. That shouldn't happen");
                        return;
                    }
                    else
                    {
                        subscriptionSet.AddSubscriber(origin);
                    }
                }

                foreach (KeyValuePair<string, List<IBroker>> child in Children)
                {
                    List<IBroker> childBrokers = child.Value;

                    // picks a random broker for load-balancing purposes
                    Random rand = new Random();
                    int brokerIndex = rand.Next(0, childBrokers.Count);

                    // we don't send the SubscriptionSet to where it came from
                    if (!child.Key.Equals(origin))
                    {
                        int seqNum;
                        if (OrderingGuarantee == OrderingGuarantee.Fifo)
                            seqNum = ++OutSequenceNumbers[child.Key];
                        else
                            seqNum = 0;

                        Thread thread = new Thread(() => childBrokers[brokerIndex].DeliverSubscription(this.ProcessName, topic, seqNum));
                        thread.Start();
                    }
                }
            }
            MessageReceived(origin, sequenceNumber);
        }

        public void DeliverUnsubscription(string origin, string topic, int sequenceNumber)
        {
            
        }

        /// <summary>
        /// This method sends the local subscription to the other brokers
        /// </summary>
        private void PropagateLocalSubscription(string topic, string processName)
        {
            foreach (var replica in SiblingBrokers)
            {
                Thread thread = new Thread(() => replica.AddLocalSubscription(topic, processName));
                thread.Start();
            }
        }

        /// <summary>
        /// This method should be called after a received message was processes. It increments the sequenceNumber
        /// and executes the command with the next sequence number 
        /// </summary>
        /// <param name="origin"> Process/group name </param>
        /// <param name="sequenceNumber"> sequence number </param>
        private void MessageReceived(string origin, int sequenceNumber)
        {
            if (OrderingGuarantee != OrderingGuarantee.Fifo)
                return;

            InSequenceNumbers[origin] = sequenceNumber;
            CommandQueue queue;
            if (HoldbackQueue.TryGetValue(origin, out queue))
            {
                string[] command = queue.GetCommand(sequenceNumber + 1);
                ProcessInternalCommand(command);
            }
        }

        public void DeliverPublication(string topic, string publication, int sequenceNumber)
        {
            if (Status.Equals(Status.Frozen))
            {
                string[] eventMessage = new string[4];
                eventMessage[0] = "DeliverPublication";
                eventMessage[1] = topic;
                eventMessage[2] = publication;
                eventMessage[3] = sequenceNumber.ToString();
            }
            else
            {
                Console.Out.WriteLine("Deliver publication");
            }
        }

        public void AddSiblingBroker(string siblingUrl)
        {
            Console.Out.WriteLine("Received sibling "+siblingUrl);
            IBroker sibling = (IBroker)Activator.GetObject(typeof(IBroker), siblingUrl);
            SiblingBrokers.Add(sibling);
        }

        public void ProcessFrozenListCommands()
        {
            string[] command;
            while (EventBacklog.TryDequeue(out command))
            {
                DeliverCommand(command);
            }
        }

        private void ProcessInternalCommand(string[] command)
        {
            switch (command[0])
            {
                case "DeliverPublication":
                    DeliverPublication(command[1], command[2], int.Parse(command[3]));
                    break;

                case "DeliverSubscription":
                    DeliverSubscription(command[1], command[2], int.Parse(command[3]));
                    break;

                default:
                    Console.Out.WriteLine("Command: " + command[0] + " doesn't exist!");
                    break;
            }
        }

        public override void DeliverCommand(string[] command)
        {
            if (Status == Status.Frozen)
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
                    //EventBacklog();
                    break;

                default:
                    ProcessInternalCommand(command);
                    break;
                    // subscriber specific commands
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

        public void AddLocalSubscription(string topic, string process)
        {
            Console.Out.WriteLine("Received propagated local subscription");
            List<string> processes;
            if (LocalSubscriptions.TryGetValue(topic, out processes))
                LocalSubscriptions[topic].Add(process);
            else
                LocalSubscriptions[topic] = new List<string> { process };
        }

        public void RemoveLocalSubscription(string topic, string process)
        {
            throw new NotImplementedException();
        }

        public void AddRemoteSubscription(string topic, string process)
        {
            throw new NotImplementedException();
        }

        public void RemoveRemoteSubscription(string topic, string process)
        {
            throw new NotImplementedException();
        }
    }
}