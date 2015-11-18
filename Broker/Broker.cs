using System;
using System.Collections.Generic;
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
        //
        public string ParentSite { get; }
        //the list of other brokers at this site
        public List<IBroker> SiblingBrokers { get; }
        // maps a process name to a process instance
        public IDictionary<string, IProcess> LocalProcesses { get; }
        // a table that maps subscriptions to processes and their sites
        public IDictionary<string, SubscriptionSet> RoutingTable { get; }
        // the sequence number for messages received by other processes
        public IDictionary<string, int> InSequenceNumbers { get; }
        // the sequence number for messages sent to other processes
        public IDictionary<string, CommandQueue> HoldbackQueue { get; }

        public Broker(string name, string url, string puppetMasterUrl, string siteName, string parentSite)
            : base(name, url, puppetMasterUrl, siteName)
        {
            // initialize state
            Children = new Dictionary<string, List<IBroker>>();
            LocalProcesses = new Dictionary<string, IProcess>();
            ParentBrokers = new List<IBroker>();
            SiblingBrokers = new List<IBroker>();
            RoutingTable = new Dictionary<string, SubscriptionSet>();
            HoldbackQueue = new Dictionary<string, CommandQueue>();
            InSequenceNumbers = new Dictionary<string, int>();

            ParentSite = parentSite;

            if (parentSite.Equals("none"))
                return;

            // obtain the parent site's brokers urls
            string parentUrl = UtilityFunctions.GetUrl(parentSite);
            List<string> brokerUrls = GetBrokers(parentUrl);

            // connect to the brokers at the parent site
            foreach (string brokerUrl in brokerUrls)
            {

                UtilityFunctions.ConnectFunction<IBroker> fun = (string urlToConnect) =>
                {
                    IBroker parentBroker = (IBroker)Activator.GetObject(typeof(IBroker), urlToConnect);
                    parentBroker.RegisterBroker(SiteName, Url);

                    return parentBroker;
                };

                var parBroker = UtilityFunctions.TryConnection<IBroker>(fun, 0, 5, brokerUrl);
                ParentBrokers.Add(parBroker);
            }
        }

        /// <summary>
        ///     Register a child broker with this broker
        /// </summary>
        /// <param name="siteName"></param>
        /// <param name="brokerUrl"></param>
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

            siteBrokers.Add((IBroker)Activator.GetObject(typeof(IBroker), brokerUrl));
            Children[siteName] = siteBrokers;
        }

        /// <summary>
        ///     Registers a Publisher or a Subscriber with this broker
        /// </summary>
        /// <param name="procName"></param>
        /// <param name="procUrl"></param>
        public void RegisterPubSub(string procName, string procUrl)
        {
            if (LocalProcesses.ContainsKey(procName))
                Console.WriteLine("There already is a process named " + procName + " at this broker (replaced anyway)");

            LocalProcesses[procName] = (IProcess)Activator.GetObject(typeof(IProcess), procUrl);
        }

        public void DeliverSubscription(string origin, string topic, string siteName)
        {
            lock(this)
            {
                Console.Out.WriteLine("Receiving subscription on topic " + topic + " from  " + origin);

                // get or create the subscription for this topic
                SubscriptionSet subscriptionSet;
                if (!RoutingTable.TryGetValue(topic, out subscriptionSet))
                {
                    subscriptionSet = new SubscriptionSet(topic);
                }

                subscriptionSet.AddSubscriber(origin, siteName);
                RoutingTable[topic] = subscriptionSet;

                ForwardLocalSubscription(origin, topic, siteName);

                Random rand = new Random();
                foreach (KeyValuePair<string, List<IBroker>> child in Children)
                {
                    // picks a random broker for load-balancing purposes
                    List<IBroker> childBrokers = child.Value;
                    int childIndex = rand.Next(0, childBrokers.Count);

                    // we don't send the SubscriptionSet to where it came from
                    if (!child.Key.Equals(siteName))
                    {
                        IBroker childBroker = childBrokers[childIndex];

                        Thread thread =
                            new Thread(() => childBroker.DeliverSubscription(origin, topic, SiteName));
                        thread.Start();
                        thread.Join();

                        if (LoggingLevel == LoggingLevel.Full)
                        {
                            thread =
                            new Thread(() => PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
                            thread.Start();
                        }
                    }
                }

                // we don't send the subscription to where it came from
                if (!ParentSite.Equals(siteName) && !ParentSite.Equals("none"))
                {
                    // picks a random broker for load-balancing purposes
                    int parentIndex = rand.Next(0, ParentBrokers.Count);
                    IBroker parent = ParentBrokers[parentIndex];

                    Thread thread =
                        new Thread(() => parent.DeliverSubscription(origin, topic, SiteName));
                    thread.Start();

                    if (LoggingLevel == LoggingLevel.Full)
                    {
                        thread =
                        new Thread(() => PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
                        thread.Start();
                    }
                }
            }
        }

        public void DeliverUnsubscription(string origin, string topic, string siteName)
        {
            lock (this)
            {
                Console.Out.WriteLine("Receiving unsubscription on topic " + topic + " from  " + origin);

                // get or create the subscription for this topic
                SubscriptionSet subscriptionSet;
                if (!RoutingTable.TryGetValue(topic, out subscriptionSet))
                {
                    subscriptionSet = new SubscriptionSet(topic);
                }

                subscriptionSet.RemoveSubscriber(origin);
                RoutingTable[topic] = subscriptionSet;

                ForwardLocalUnsubscription(origin, topic, siteName);

                Random rand = new Random();
                foreach (KeyValuePair<string, List<IBroker>> child in Children)
                {
                    // picks a random broker for load-balancing purposes
                    List<IBroker> childBrokers = child.Value;
                    int childIndex = rand.Next(0, childBrokers.Count);

                    // we don't send the SubscriptionSet to where it came from
                    if (!child.Key.Equals(siteName))
                    {
                        IBroker childBroker = childBrokers[childIndex];
                        Thread thread =
                            new Thread(() => childBroker.DeliverUnsubscription(origin, topic, SiteName));
                        thread.Start();

                        if (LoggingLevel == LoggingLevel.Full)
                        {
                            thread = new Thread(() => PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
                            thread.Start();
                        }
                    }
                }

                // we don't send the subscription to where it came from
                if (!ParentSite.Equals(siteName) && !ParentSite.Equals("none"))
                {
                    // picks a random broker for load-balancing purposes
                    int parentIndex = rand.Next(0, ParentBrokers.Count);
                    IBroker parent = ParentBrokers[parentIndex];

                    Thread thread =
                        new Thread(() => parent.DeliverUnsubscription(origin, topic, SiteName));
                    thread.Start();

                    if (LoggingLevel == LoggingLevel.Full)
                    {
                        thread = new Thread(() => PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
                        thread.Start();
                    }
                }
            }
        }
        /// <summary>
        /// Origin is the origin process name.
        /// </summary>
        /// <param name="origin"></param>
        /// <param name="topic"></param>
        /// <param name="publication"></param>
        /// <param name="fromSite"></param>
        /// <param name="sequenceNumber"></param>
        public void DeliverPublication(string origin, string topic, string publication, string fromSite, int sequenceNumber)
        {
            lock(this)
            {
            /*    if (!PublicationReceived(origin, topic, publication, fromSite, sequenceNumber))
                {
                    return;
                }*/

                Console.Out.WriteLine("Receiving publication on topic " + topic + " from  " + origin);

                SubscriptionSet subs = null;
                IDictionary<string, string> matchList = null;
                if (RoutingTable.TryGetValue(topic, out subs))
                {
                    matchList = subs.GetMatchList();
                }

                // If there are any subscriptions
                if (matchList != null)
                {
                    // send to the interested local processes
                    foreach (KeyValuePair<string, string> match in matchList)
                    {
                        IProcess proc;
                        if (LocalProcesses.TryGetValue(match.Key, out proc))
                        {
                            Console.Out.WriteLine("Sending publication '"+publication+"' to " + match.Key);

                            ISubscriber subscriber = (ISubscriber)proc;

                            Thread thread =
                                new Thread(() => subscriber.DeliverPublication(publication, topic, origin, sequenceNumber));
                            thread.Start();

                            thread = new Thread(() => PuppetMaster.DeliverLog("SubEvent " + match.Key + ", " + origin + ", " + topic));
                            thread.Start();
                        }
                    }
                }

                if (this.RoutingPolicy == RoutingPolicy.Flood)
                {
                    Random rand = new Random();

                    foreach (KeyValuePair<string, List<IBroker>> child in Children)
                    {
                        // picks a random broker for load-balancing purposes
                        List<IBroker> childBrokers = child.Value;
                        int childIndex = rand.Next(0, childBrokers.Count);

                        // we don't send the SubscriptionSet to where it came from
                        if (!child.Key.Equals(fromSite))
                        {
                            IBroker childBroker = childBrokers[childIndex];
                            Thread thread =
                                new Thread(
                                    () =>
                                        childBroker.DeliverPublication(origin, topic, publication, this.SiteName, sequenceNumber));
                            thread.Start();

                            if (LoggingLevel == LoggingLevel.Full)
                            {
                                thread = new Thread(() => PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
                                thread.Start();
                            }
                        }
                    }

                    // we don't send the subscription to where it came from
                    if (!ParentSite.Equals(fromSite) && !ParentSite.Equals("none"))
                    {
                        // picks a random broker for load-balancing purposes
                        int parentIndex = rand.Next(0, ParentBrokers.Count);
                        IBroker parent = ParentBrokers[parentIndex];

                        Thread thread =
                            new Thread(
                                () =>
                                    parent.DeliverPublication(origin, topic, publication, this.SiteName, sequenceNumber));
                        thread.Start();

                        if (LoggingLevel == LoggingLevel.Full)
                        {
                            thread =
                            new Thread(() => PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
                            thread.Start();
                        }
                    }
                }
                else if (matchList != null)
                {
                    // if there are subscriptions to the topic, send to the appropriate sites
                    List<string> SentSites = new List<string>();

                    foreach (KeyValuePair<string, string> match in matchList)
                    {
                        // we don't want to sent multiple messages to the same site
                        if (SentSites.Contains(match.Value))
                            continue;

                        // don't send publication to where it came from
                        if (match.Value.Equals(fromSite))
                            continue;

                        Random rand = new Random();

                        List<IBroker> brokers;
                        if (Children.TryGetValue(match.Value, out brokers))
                        {
                            int brokerIndex = rand.Next(0, brokers.Count);
                            IBroker broker = brokers[brokerIndex];

                            SentSites.Add(match.Value);

                            Thread thread =
                                new Thread(() => broker.DeliverPublication(origin, topic, publication, SiteName, sequenceNumber));
                            thread.Start();

                            if (LoggingLevel == LoggingLevel.Full)
                            {
                                thread =
                                new Thread(() => PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
                                thread.Start();
                            }
                        }

                        if (ParentSite.Equals(match.Value))
                        {
                            int brokerIndex = rand.Next(0, ParentBrokers.Count);
                            IBroker parent = ParentBrokers[brokerIndex];

                            SentSites.Add(match.Value);

                            Thread thread =
                                new Thread(() => parent.DeliverPublication(origin, topic, publication, SiteName, sequenceNumber));
                            thread.Start();

                            if (LoggingLevel == LoggingLevel.Full)
                            {
                                thread =
                                new Thread(() => PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
                                thread.Start();
                            }
                        }
                    }
                }

            //    MessageProcessed(origin, sequenceNumber);
                ForwardLocalPublication(origin, sequenceNumber);
            }
        }

        public void AddSiblingBroker(string siblingUrl)
        {
            Console.Out.WriteLine("Received sibling " + siblingUrl);
            IBroker sibling = (IBroker)Activator.GetObject(typeof(IBroker), siblingUrl);
            SiblingBrokers.Add(sibling);
        }

        public override void DeliverCommand(string[] command)
        {
            lock (this)
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
                    case "Crash":
                    case "Freeze":
                        base.DeliverCommand(command);
                        break;

                    case "Unfreeze":
                        Console.Out.WriteLine("Unfreezing");
                        Status = Status.Unfrozen;
                        ProcessFrozenListCommands();
                        break;

                    case "Status":
                        PrintStatus();
                        break;

                    default:
                        ProcessInternalCommandOrMessage(command);
                        break;
                        // subscriber specific commands
                }
            }
        }

        void PrintStatus()
        {
            lock (this)
            {
                Console.Out.WriteLine("**** Status *****");
                if (RoutingTable.Keys.Count == 0)
                    Console.Out.WriteLine("\t\n\tThere are no subscriptions");

                foreach (var entry in RoutingTable)
                {
                    SubscriptionSet set = entry.Value;
                    foreach (var process in set.Processes)
                        Console.Out.WriteLine("\t\n\t" + process.Key + " is subscribed to " + entry.Key);
                }
                Console.Out.WriteLine("*******************\t\n");
            }
        }

        /// <summary>
        ///     Replicates the subscription to this replica
        /// </summary>
        /// <param name="process"></param>
        /// <param name="topic"></param>
        /// <param name="siteName"></param>
        /// <param name="sequenceNumber"></param>
        public void AddLocalSubscription(string process, string topic, string siteName)
        {
            lock (this)
            {
                Console.Out.WriteLine("Receiving replicated subscription on topic " + topic + " from " + process);

                // get or create the SubscriptionSet for this topic
                SubscriptionSet subscriptionSet;
                if (!RoutingTable.TryGetValue(topic, out subscriptionSet))
                {
                    subscriptionSet = new SubscriptionSet(topic);
                    RoutingTable[topic] = subscriptionSet;
                }

                subscriptionSet.AddSubscriber(process, siteName);
            }
        }

        /// <summary>
        ///     Replicates the unsubscription to this replica
        /// </summary>
        /// <param name="process"></param>
        /// <param name="topic"></param>
        /// <param name="siteName"></param>
        /// <param name="sequenceNumber"></param>
        public void RemoveLocalSubscription(string process, string topic, string siteName)
        {
            lock (this)
            {
                Console.Out.WriteLine("Receiving replicated unsubscription on topic " + topic + " from " + process);

                // get or create the SubscriptionSet for this topic
                SubscriptionSet subscriptionSet;
                if (!RoutingTable.TryGetValue(topic, out subscriptionSet))
                {
                    subscriptionSet = new SubscriptionSet(topic);
                    RoutingTable[topic] = subscriptionSet;
                }

                subscriptionSet.RemoveSubscriber(process);
            }
        }

        public void UpdatePublisherSequenceNumber(string process, int sequenceNumber)
        {
            InSequenceNumbers[process] = sequenceNumber;
        }

        /// <summary>
        ///     This method sends the local subscription to the other brokers
        /// </summary>
        private void ForwardLocalSubscription(string processName, string topic, string siteName)
        {
            foreach (IBroker replica in SiblingBrokers)
            {
                Thread thread =
                    new Thread(() => replica.AddLocalSubscription(processName, topic, siteName));
                thread.Start();
            }
        }
        /// <summary>
        ///     This method sends the local unsubscription to the other brokers
        /// </summary>
        private void ForwardLocalUnsubscription(string processName, string topic, string siteName)
        {
            foreach (IBroker replica in SiblingBrokers)
            {
                Thread thread =
                    new Thread(() => replica.RemoveLocalSubscription(processName, topic, siteName));
                thread.Start();
            }
        }

        /// <summary>
        /// This method updates the sequenceNumber on the given process name for all replicas
        /// </summary>
        /// <param name="processName"></param>
        /// <param name="sequenceNumber"></param>
        private void ForwardLocalPublication(string processName, int sequenceNumber)
        {
            foreach (IBroker replica in SiblingBrokers)
            {
                Thread thread =
                    new Thread(() => replica.UpdatePublisherSequenceNumber(processName, sequenceNumber));
                thread.Start();
            }
        }

        private void MessageProcessed(string origin, int sequenceNumber)
        {
            if (this.OrderingGuarantee != OrderingGuarantee.Fifo)
                return;

            InSequenceNumbers[origin] = sequenceNumber;
            CommandQueue queue;
            if (HoldbackQueue.TryGetValue(origin, out queue))
            {
                string[] command = queue.GetCommandAndRemove(sequenceNumber + 1);
                if (command == null)
                {
                    return;
                }
                Console.Out.WriteLine("Unblocking message with sequence number: "+(sequenceNumber+1));
                ProcessInternalCommandOrMessage(command);
            }
        }

        public void ProcessFrozenListCommands()
        {
            string[] command;
            while (CommandBacklog.TryDequeue(out command))
            {
                DeliverCommand(command);
            }

            foreach (var message in FrozenMessages)
                ProcessInternalCommandOrMessage(message);
        }

        private void ProcessInternalCommandOrMessage(string[] command)
        {
            switch (command[0])
            {
                case "DeliverPublication":
                    DeliverPublication(command[1], command[2], command[3], command[4], int.Parse(command[5]));
                    break;

                default:
                    Console.Out.WriteLine("Command: " + command[0] + " doesn't exist!");
                    break;
            }
        }

        /// <summary>
        ///     Decides what to do with the publication. Returns true if it should be further processed
        ///     or false if it shouldn't
        /// </summary>
        /// <param name="origin"></param>
        /// <param name="topic"></param>
        /// <param name="publication"></param>
        /// <param name="sequenceNumber"></param>
        /// <returns></returns>
        private bool PublicationReceived(string origin, string topic, string publication, string fromSite, int sequenceNumber)
        {
            //Console.Out.WriteLine("origin:" + origin + " topic:" + topic + " publi:" + publication + " fromSite:" + fromSite + " sqN:" + sequenceNumber);
            string[] eventMessage = new string[6];
            eventMessage[0] = "DeliverPublication";
            eventMessage[1] = origin;
            eventMessage[2] = topic;
            eventMessage[3] = publication;
            eventMessage[4] = fromSite;
            eventMessage[5] = sequenceNumber.ToString();

            if (Status.Equals(Status.Frozen))
            {
                FrozenMessages.Add(eventMessage);
                return false;
            }

            int lastNumber;
            if (!InSequenceNumbers.TryGetValue(origin, out lastNumber))
                lastNumber = 0;
            //Console.Out.WriteLine("lastNumber:" + lastNumber);
            if (this.OrderingGuarantee == OrderingGuarantee.Fifo && sequenceNumber > lastNumber + 1)
            {
                Console.Out.WriteLine("Delayed message detected. Queueing message: "+sequenceNumber);
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
                return false;
            }
            return true;
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