using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using CommonTypes;

namespace Broker
{
    public class Broker : BaseProcess, IBroker
    {
        // maps a site's name to a list of the site's brokers
        public IDictionary<string, List<IBroker>> Children { get; } = new ConcurrentDictionary<string, List<IBroker>>();
        // this site's parent
        public string ParentSite { get; }
        //the list of other brokers at this site
        public List<IBroker> SiblingBrokers { get; } = new List<IBroker>();
        // maps a process name to a process instance
        public IDictionary<string, IProcess> LocalProcesses { get; } = new ConcurrentDictionary<string, IProcess>();
        // a table that maps subscriptions to processes and their sites
        public IDictionary<string, SubscriptionSet> RoutingTable { get; } =
            new ConcurrentDictionary<string, SubscriptionSet>();

        // sequence numbers sent by other processes
        public IDictionary<string, int> InSequenceNumbers { get; } = new ConcurrentDictionary<string, int>();
        // sequence numbers sent to other processes - example: int seq = OutSeq [proc][site] 
        public IDictionary<string, IDictionary<string, int>> OutSequenceNumbers { get; } =
            new ConcurrentDictionary<string, IDictionary<string, int>>();

        // hold-back queue used for storing delayed messages
        public IDictionary<string, MessageQueue> HoldbackQueue { get; } =
            new ConcurrentDictionary<string, MessageQueue>();

        // this broker's parent brokers
        public List<IBroker> ParentBrokers { get; } = new List<IBroker>();

        public Broker(string name, string url, string puppetMasterUrl, string siteName, string parentSite)
            : base(name, url, puppetMasterUrl, siteName)
        {
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
                    IBroker parentBroker = (IBroker) Activator.GetObject(typeof (IBroker), urlToConnect);
                    parentBroker.RegisterBroker(SiteName, Url);

                    return parentBroker;
                };

                Random rand = new Random();
                int sleepTime = rand.Next(100, 1100);
                IBroker parBroker = UtilityFunctions.TryConnection(fun, sleepTime, 15, brokerUrl);
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

            siteBrokers.Add((IBroker) Activator.GetObject(typeof (IBroker), brokerUrl));
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

            LocalProcesses[procName] = (IProcess) Activator.GetObject(typeof (IProcess), procUrl);
        }

        public void DeliverSubscription(string origin, string topic, string siteName)
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

            Random rand = new Random();


            foreach (KeyValuePair<string, List<IBroker>> child in Children)
            {
                List<IBroker> childBrokers = child.Value;

                // picks a random broker for load-balancing purposes
                int childIndex = rand.Next(0, childBrokers.Count);

                // we don't send the SubscriptionSet to where it came from
                if (!child.Key.Equals(siteName))
                {
                    IBroker childBroker = childBrokers[childIndex];

                    Thread thread =
                        new Thread(() => childBroker.DeliverSubscription(origin, topic, SiteName));
                    thread.Start();
                    thread.Join();

                    if (this.LoggingLevel == LoggingLevel.Full)
                    {
                        thread =
                            new Thread(
                                () => PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
                        thread.Start();
                    }
                }
            }

            // we don't send the subscription to where it came from
            if (!ParentSite.Equals(siteName) && !ParentSite.Equals("none"))
            {
                IBroker parent;
                lock (ParentBrokers)
                {
                    // picks a random broker for load-balancing purposes
                    int parentIndex = rand.Next(0, ParentBrokers.Count);
                    parent = ParentBrokers[parentIndex];
                }

                Thread thread =
                    new Thread(() => parent.DeliverSubscription(origin, topic, SiteName));
                thread.Start();

                if (this.LoggingLevel == LoggingLevel.Full)
                {
                    thread =
                        new Thread(
                            () => PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
                    thread.Start();
                }
            }
        }

        public void DeliverUnsubscription(string origin, string topic, string siteName)
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

            Random rand = new Random();
            foreach (KeyValuePair<string, List<IBroker>> child in Children)
            {
                List<IBroker> childBrokers = child.Value;

                // picks a random broker for load-balancing purposes
                int childIndex = rand.Next(0, childBrokers.Count);

                // we don't send the SubscriptionSet to where it came from
                if (!child.Key.Equals(siteName))
                {
                    IBroker childBroker = childBrokers[childIndex];
                    Thread thread =
                        new Thread(() => childBroker.DeliverUnsubscription(origin, topic, SiteName));
                    thread.Start();

                    if (this.LoggingLevel == LoggingLevel.Full)
                    {
                        thread =
                            new Thread(
                                () =>
                                    PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
                        thread.Start();
                    }
                }
            }

            // we don't send the subscription to where it came from
            if (!ParentSite.Equals(siteName) && !ParentSite.Equals("none"))
            {
                IBroker parent;
                lock (ParentBrokers)
                {
                    // picks a random broker for load-balancing purposes
                    int parentIndex = rand.Next(0, ParentBrokers.Count);
                    parent = ParentBrokers[parentIndex];
                }

                Thread thread =
                    new Thread(() => parent.DeliverUnsubscription(origin, topic, SiteName));
                thread.Start();

                if (this.LoggingLevel == LoggingLevel.Full)
                {
                    thread =
                        new Thread(
                            () => PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
                    thread.Start();
                }
            }
        }

        /// <summary>
        ///     Origin is the origin process name.
        /// </summary>
        /// <param name="origin"></param>
        /// <param name="topic"></param>
        /// <param name="publication"></param>
        /// <param name="fromSite"></param>
        /// <param name="sequenceNumber"></param>
        public void DeliverPublication(string origin, string topic, string publication, string fromSite,
            int sequenceNumber)
        {
            if (!PublicationReceived(origin, topic, publication, fromSite, sequenceNumber))
            {
                return;
            }

            // TODO: refactorizar este metodo !!!

            Console.Out.WriteLine("Receiving publication " + publication + " from  " + origin + " with seq " +
                                  sequenceNumber);

            IDictionary<string, string> matchList = null;


            SubscriptionSet subs;
            if (RoutingTable.TryGetValue(topic, out subs))
            {
                matchList = subs.GetMatchList();
            }
            // If there are any subscriptions
            if (matchList != null)
            {
                IDictionary<string, int> siteToSeqNum;
                if (!OutSequenceNumbers.TryGetValue(origin, out siteToSeqNum))
                    siteToSeqNum = new ConcurrentDictionary<string, int>();

                int seqNum;
                if (!siteToSeqNum.TryGetValue(SiteName, out seqNum))
                    seqNum = 0;

                // send to the interested local processes
                foreach (KeyValuePair<string, string> match in matchList)
                {
                    IProcess proc;
                    if (LocalProcesses.TryGetValue(match.Key, out proc))
                    {
                        seqNum++;
                        siteToSeqNum[SiteName] = seqNum;
                        OutSequenceNumbers[origin] = siteToSeqNum;

                        Console.Out.WriteLine("Sending publication '" + publication + "' to " + match.Key + " with seq " +
                                              seqNum);

                        ISubscriber subscriber = (ISubscriber) proc;

                        Thread thread =
                            new Thread(() => subscriber.DeliverPublication(publication, topic, origin, seqNum));
                        thread.Start();

                        // TODO: descomentar se for para enviar um BroEvent quando é entregue um SubEvent
                        /*thread =
                            new Thread(
                                () => PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
                        thread.Start();*/
                    }
                }
            }

            if (this.RoutingPolicy == RoutingPolicy.Flood)
            {
                Random rand = new Random();

                foreach (KeyValuePair<string, List<IBroker>> child in Children)
                {
                    List<IBroker> childBrokers = child.Value;

                    // picks a random broker for load-balancing purposes
                    int childIndex = rand.Next(0, childBrokers.Count);

                    // we don't send the SubscriptionSet to where it came from
                    if (!child.Key.Equals(fromSite))
                    {
                        IDictionary<string, int> siteToSeqNum;
                        if (!OutSequenceNumbers.TryGetValue(origin, out siteToSeqNum))
                            siteToSeqNum = new ConcurrentDictionary<string, int>();

                        int seqNum;
                        if (!siteToSeqNum.TryGetValue(child.Key, out seqNum))
                            seqNum = 0;

                        seqNum++;
                        siteToSeqNum[child.Key] = seqNum;
                        OutSequenceNumbers[origin] = siteToSeqNum;

                        IBroker childBroker = childBrokers[childIndex];
                        Thread thread =
                            new Thread(
                                () =>
                                    childBroker.DeliverPublication(origin, topic, publication, SiteName, seqNum));
                        thread.Start();

                        if (this.LoggingLevel == LoggingLevel.Full)
                        {
                            thread =
                                new Thread(
                                    () =>
                                        PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
                            thread.Start();
                        }
                    }
                }

                // we don't send the subscription to where it came from
                if (!ParentSite.Equals(fromSite) && !ParentSite.Equals("none"))
                {
                    IDictionary<string, int> siteToSeqNum;
                    if (!OutSequenceNumbers.TryGetValue(origin, out siteToSeqNum))
                        siteToSeqNum = new ConcurrentDictionary<string, int>();

                    int seqNum;
                    if (!siteToSeqNum.TryGetValue(ParentSite, out seqNum))
                        seqNum = 0;

                    seqNum++;
                    siteToSeqNum[ParentSite] = seqNum;
                    OutSequenceNumbers[origin] = siteToSeqNum;

                    IBroker parent;
                    lock (ParentBrokers)
                    {
                        // picks a random broker for load-balancing purposes
                        int parentIndex = rand.Next(0, ParentBrokers.Count);
                        parent = ParentBrokers[parentIndex];
                    }

                    Thread thread =
                        new Thread(
                            () =>
                                parent.DeliverPublication(origin, topic, publication, SiteName, seqNum));
                    thread.Start();

                    if (this.LoggingLevel == LoggingLevel.Full)
                    {
                        thread =
                            new Thread(
                                () => PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
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
                        IBroker broker;
                        lock (brokers)
                        {
                            int brokerIndex = rand.Next(0, brokers.Count);
                            broker = brokers[brokerIndex];
                        }

                        SentSites.Add(match.Value);

                        IDictionary<string, int> siteToSeqNum;
                        if (!OutSequenceNumbers.TryGetValue(origin, out siteToSeqNum))
                            siteToSeqNum = new ConcurrentDictionary<string, int>();

                        int seqNum;
                        if (!siteToSeqNum.TryGetValue(match.Value, out seqNum))
                            seqNum = 0;

                        seqNum++;
                        siteToSeqNum[match.Value] = seqNum;
                        OutSequenceNumbers[origin] = siteToSeqNum;

                        Console.Out.WriteLine("Sending pub " + publication + " to site " + match.Value + " with seq " +
                                              seqNum);
                        Thread thread =
                            new Thread(() => broker.DeliverPublication(origin, topic, publication, SiteName, seqNum));
                        thread.Start();

                        if (this.LoggingLevel == LoggingLevel.Full)
                        {
                            thread =
                                new Thread(
                                    () =>
                                        PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
                            thread.Start();
                        }
                    }

                    if (ParentSite.Equals(match.Value))
                    {
                        IDictionary<string, int> siteToSeqNum;
                        if (!OutSequenceNumbers.TryGetValue(origin, out siteToSeqNum))
                            siteToSeqNum = new ConcurrentDictionary<string, int>();

                        int seqNum;
                        if (!siteToSeqNum.TryGetValue(match.Value, out seqNum))
                            seqNum = 0;

                        seqNum++;
                        siteToSeqNum[match.Value] = seqNum;
                        OutSequenceNumbers[origin] = siteToSeqNum;

                        IBroker parent;
                        lock (ParentBrokers)
                        {
                            int brokerIndex = rand.Next(0, ParentBrokers.Count);
                            parent = ParentBrokers[brokerIndex];
                        }

                        SentSites.Add(match.Value);

                        Thread thread =
                            new Thread(() => parent.DeliverPublication(origin, topic, publication, SiteName, seqNum));
                        thread.Start();

                        if (this.LoggingLevel == LoggingLevel.Full)
                        {
                            thread =
                                new Thread(
                                    () =>
                                        PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + origin + ", " + topic));
                            thread.Start();
                        }
                    }
                }
            }

            MessageProcessed(origin, sequenceNumber);
        }

        public void AddSiblingBroker(string siblingUrl)
        {
            Console.Out.WriteLine("Received sibling " + siblingUrl);
            IBroker sibling = (IBroker) Activator.GetObject(typeof (IBroker), siblingUrl);

            lock (SiblingBrokers)
            {
                SiblingBrokers.Add(sibling);
            }
        }

        public override void DeliverCommand(string[] command)
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

        /// <summary>
        ///     Replicates the unsubscription to this replica
        /// </summary>
        /// <param name="process"></param>
        /// <param name="topic"></param>
        /// <param name="siteName"></param>
        /// <param name="sequenceNumber"></param>
        public void RemoveLocalSubscription(string process, string topic, string siteName)
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

        public void UpdatePublisherSequenceNumber(string process, int sequenceNumber)
        {
            InSequenceNumbers[process] = sequenceNumber;
        }

        private void PrintStatus()
        {
            Console.Out.WriteLine("**** Status *****");
            if (RoutingTable.Keys.Count == 0)
                Console.Out.WriteLine("\tThere are no subscriptions");

            foreach (KeyValuePair<string, SubscriptionSet> entry in RoutingTable)
            {
                SubscriptionSet set = entry.Value;
                foreach (KeyValuePair<string, string> process in set.Processes)
                    Console.Out.WriteLine("\t" + process.Key + " is subscribed to " + entry.Key);
            }

            if (HoldbackQueue.Keys.Count == 0)
                Console.Out.WriteLine("\tThere are no messages in the hold-back queue");

            foreach (KeyValuePair<string, MessageQueue> messageQueue in HoldbackQueue)
            {
                Console.Out.WriteLine("There are " + messageQueue.Value.GetCount() + " messages in queue for " +
                                      messageQueue.Key);
            }
            Console.Out.WriteLine("*******************\t\n");
        }

        /// <summary>
        ///     Updates this broker's state. Updates sequence numbers, unblocks delayed messages (if any), etc
        /// </summary>
        /// <param name="topic"> The subscribed topic </param>
        /// <param name="process"> The publisher </param>
        /// <param name="sequenceNumber"> The message's sequence number </param>
        private void MessageProcessed(string origin, int sequenceNumber)
        {
            if (this.OrderingGuarantee != OrderingGuarantee.Fifo)
                return;

            lock (HoldbackQueue)
            {
                InSequenceNumbers[origin] = sequenceNumber;
                MessageQueue queue;
                if (HoldbackQueue.TryGetValue(origin, out queue))
                {
                    string[] command = queue.GetCommandAndRemove(sequenceNumber + 1);
                    if (command == null)
                    {
                        return;
                    }
                    Console.Out.WriteLine("Unblocking message " + "with sequence number: " + (sequenceNumber + 1));
                    ProcessInternalCommandOrMessage(command);
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

            lock (FrozenMessages)
            {
                foreach (string[] message in FrozenMessages)
                    ProcessInternalCommandOrMessage(message);
            }
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
        ///     Decides what to do with the publication. 
        /// </summary>
        /// <param name="origin"></param>
        /// <param name="topic"></param>
        /// <param name="publication"></param>
        /// <param name="sequenceNumber"></param>
        /// <returns> Returns true if it should be further processed or false if it shouldn't </returns>
        private bool PublicationReceived(string origin, string topic, string publication, string fromSite,
            int sequenceNumber)
        {
            string[] eventMessage = new string[6];
            eventMessage[0] = "DeliverPublication";
            eventMessage[1] = origin;
            eventMessage[2] = topic;
            eventMessage[3] = publication;
            eventMessage[4] = fromSite;
            eventMessage[5] = sequenceNumber.ToString();

            lock (HoldbackQueue)
            {
                if (Status.Equals(Status.Frozen))
                {
                    lock (FrozenMessages)
                    {
                        FrozenMessages.Add(eventMessage);
                    }
                    return false;
                }

                int lastNumber;
                if (!InSequenceNumbers.TryGetValue(origin, out lastNumber))
                    lastNumber = 0;

                if (this.OrderingGuarantee == OrderingGuarantee.Fifo && sequenceNumber > lastNumber + 1)
                {
                    Console.Out.WriteLine("Delayed message detected. Queueing message '" + publication + "' with seqNo " + sequenceNumber);
                    MessageQueue queue;

                    if (HoldbackQueue.TryGetValue(origin, out queue))
                    {
                        queue.AddCommand(eventMessage, sequenceNumber);
                    }
                    else
                    {
                        queue = new MessageQueue();
                        queue.AddCommand(eventMessage, sequenceNumber);
                        HoldbackQueue[origin] = queue;
                    }
                    return false;
                }
                return true;
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