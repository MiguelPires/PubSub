#region

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.Remoting;
using System.Threading;
using CommonTypes;

#endregion

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
        // this broker's parent brokers
        public List<IBroker> ParentBrokers { get; } = new List<IBroker>();
        // maps a subscriber name to a subscriber instance
        public IDictionary<string, IProcess> LocalProcesses { get; } = new ConcurrentDictionary<string, IProcess>();
        // a table that maps subscriptions to processes and their sites
        public IDictionary<string, SubscriptionSet> RoutingTable { get; } =
            new ConcurrentDictionary<string, SubscriptionSet>();

        // sequence numbers sent by other processes
        public IDictionary<string, int> InSequenceNumbers { get; set; } = new ConcurrentDictionary<string, int>();
        // sequence numbers sent to other processes - example: int seq = OutSeq [proc][site] 
        public IDictionary<string, IDictionary<string, int>> OutSequenceNumbers { get; set; } =
            new ConcurrentDictionary<string, IDictionary<string, int>>();

        // hold-back queue used for storing delayed messages
        public IDictionary<string, MessageQueue> HoldbackQueue { get; set; } =
            new ConcurrentDictionary<string, MessageQueue>();

        // the prevents sending the same message multiple times by the same process
        public IDictionary<string, object> ProcessLocks { get; } = new ConcurrentDictionary<string, object>();
        // history of messages sent by publishers to each site
        public MessageHistory History { get; } = new MessageHistory();
        //
        public object Subscribing = new object();

        public Broker(string name, string url, string puppetMasterUrl, string siteName, string parentSite)
            : base(name, url, puppetMasterUrl, siteName)
        {
            ParentSite = parentSite;

            if (parentSite.Equals("none"))
                return;

            // obtain the parent site's brokers urls
            string parentUrl = Utility.GetUrl(parentSite);
            List<string> brokerUrls = GetBrokers(parentUrl);

            // connect to the brokers at the parent site
            foreach (string brokerUrl in brokerUrls)
            {
                Utility.ConnectFunction<IBroker> fun = (string urlToConnect) =>
                {
                    IBroker parentBroker = (IBroker) Activator.GetObject(typeof (IBroker), urlToConnect);
                    parentBroker.RegisterBroker(SiteName, Url);

                    return parentBroker;
                };

                IBroker parBroker = Utility.TryConnection(fun, brokerUrl);
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
            } catch (KeyNotFoundException)
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
                Utility.DebugLog("There already is a subscriber named " + procName + " at this broker (replaced anyway)");

            LocalProcesses[procName] = (IProcess) Activator.GetObject(typeof (IProcess), procUrl);
        }

        //************************************************
        //
        //          Publication Methods
        //
        //************************************************

        /// <summary>
        ///     Delivers a publication after it ensures reliability (and optionally, FIFO ordering)
        /// </summary>
        /// <param name="publisher"></param>
        /// <param name="topic"></param>
        /// <param name="publication"></param>
        /// <param name="fromSite"></param>
        /// <param name="sequenceNumber"></param>
        public void DeliverPublication(string publisher, string topic, string publication, string fromSite,
            int sequenceNumber)
        {
            Console.Out.WriteLine("Deliver pub - seqNo " + sequenceNumber);

            // we might have just one broker for debug purposes
            if (SiblingBrokers.Count != 0)
            {
                lock (SiblingBrokers)
                {
                    // multicast the publication
                    foreach (var broker in SiblingBrokers)
                    {
                        Thread thread =
                            new Thread(() =>
                            {
                                try
                                {
                                    broker.InformOfPublication(publisher, topic, publication, fromSite, sequenceNumber, ProcessName);
                                } catch (RemotingException)
                                {
                                } catch (SocketException)
                                {
                                }
                            });
                        thread.Start();
                    }
                }
            } else
                ProcessPublication(publisher, topic, publication, fromSite, sequenceNumber, ProcessName);
        }

        /// <summary>
        ///     Delivers the publication (either updates the state or actually processes the
        ///  publication) and multicasts it. 
        /// </summary>
        /// <param name="publisher"></param>
        /// <param name="topic"></param>
        /// <param name="publication"></param>
        /// <param name="fromSite"></param>
        /// <param name="sequenceNumber"></param>
        /// <param name="deliverProcess"> The deliverProcess is used to inform everyone about 
        /// which process processes the publication (it's the one that received it) </param>
        public void InformOfPublication(string publisher, string topic, string publication, string fromSite, int sequenceNumber,
            string deliverProcess)
        {
            // creates a subscriber lock if needed
            object procLock;
            if (!ProcessLocks.TryGetValue(publisher, out procLock))
            {
                ProcessLocks[publisher] = new object();
            }

            lock (ProcessLocks[publisher])
            {
                lock (Subscribing)
                {
                    // check if the publication was already delivered
                    int lastNumber;
                    if (InSequenceNumbers.TryGetValue(publisher, out lastNumber) && lastNumber >= sequenceNumber)
                    {
                        return;
                    }

                    // TODO: refactor this as in the InformOfSubscription
                    MessageQueue queue;
                    if (HoldbackQueue.TryGetValue(publisher, out queue))
                    {
                        if (queue.GetSequenceNumbers().Contains(sequenceNumber))
                            return;
                    }

                    if (deliverProcess.Equals(ProcessName))
                    {
                        Console.Out.WriteLine("PROCESS - " + sequenceNumber);
                        ProcessPublication(publisher, topic, publication, fromSite, sequenceNumber, deliverProcess);
                    }
                    else
                    {
                        // stores the publication for the appropriate processes
                        StorePublicationInHistory(publisher, topic, publication, fromSite, sequenceNumber, deliverProcess);
                    }

                    // TODO: remover este lock
                    lock (SiblingBrokers)
                    {
                        // multicast the publication
                        foreach (var broker in SiblingBrokers)
                        {
                            Thread thread =
                                new Thread(() =>
                                {
                                    try
                                    {
                                        broker.InformOfPublication(publisher, topic, publication, fromSite, sequenceNumber,
                                            deliverProcess);
                                    }
                                    catch (RemotingException)
                                    {
                                    }
                                    catch (SocketException)
                                    {
                                    }
                                });
                            thread.Start();
                        }
                    }
                }
            }
        }

        private void StorePublicationInHistory(string publisher, string topic, string publication, string fromSite,
            int sequenceNumber, string deliverProcess)
        {
            // creates a subscriber lock if needed
            object procLock;
            if (!ProcessLocks.TryGetValue(publisher, out procLock))
            {
                ProcessLocks[publisher] = new object();
            }

            lock (ProcessLocks[publisher])
            {
                if (!PublicationReceived(publisher, topic, publication, fromSite, sequenceNumber, deliverProcess))
                    return;

                int lastNumber;
                if (!InSequenceNumbers.TryGetValue(publisher, out lastNumber))
                    lastNumber = 0;

                // just in case
                if (this.OrderingGuarantee == OrderingGuarantee.Fifo && sequenceNumber != lastNumber + 1)
                    return;

                Utility.DebugLog("Update pub '" + publication + "' seq " + sequenceNumber);
                InSequenceNumbers[publisher] = sequenceNumber;

                // to be stored later 
                string[] messageToStore = {publisher, topic, publication, fromSite, ""};

                IDictionary<string, string> matchList = GetTopicMatchList(topic);
                // if there are any subscriptions
                if (matchList != null)
                {
                    // determine the correct sequence number for the process
                    IDictionary<string, int> siteToSeqNum;
                    if (!OutSequenceNumbers.TryGetValue(publisher, out siteToSeqNum))
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
                            // update the sequence number for the local process
                            seqNum++;
                            siteToSeqNum[SiteName] = seqNum;
                            OutSequenceNumbers[publisher] = siteToSeqNum;

                            // store the publication
                            messageToStore[4] = seqNum.ToString();
                            History.StorePublication(SiteName, messageToStore);
                        }
                    }
                }

                if (this.RoutingPolicy == RoutingPolicy.Flood)
                {
                    foreach (KeyValuePair<string, List<IBroker>> child in Children)
                    {
                        // we don't send the SubscriptionSet to where it came from
                        if (!child.Key.Equals(fromSite))
                        {
                            // determine the correct sequence number for the broker 
                            IDictionary<string, int> siteToSeqNum;
                            if (!OutSequenceNumbers.TryGetValue(publisher, out siteToSeqNum))
                                siteToSeqNum = new ConcurrentDictionary<string, int>();

                            int seqNum;
                            if (!siteToSeqNum.TryGetValue(child.Key, out seqNum))
                                seqNum = 0;

                            // update the sequence number
                            seqNum++;
                            siteToSeqNum[child.Key] = seqNum;
                            OutSequenceNumbers[publisher] = siteToSeqNum;

                            // store the publication
                            messageToStore[4] = seqNum.ToString();
                            History.StorePublication(child.Key, messageToStore);
                        }
                    }

                    // we don't send the subscription to where it came from
                    if (!ParentSite.Equals(fromSite) && !ParentSite.Equals("none"))
                    {
                        // determine the correct sequence number for the parent broker
                        IDictionary<string, int> siteToSeqNum;
                        if (!OutSequenceNumbers.TryGetValue(publisher, out siteToSeqNum))
                            siteToSeqNum = new ConcurrentDictionary<string, int>();

                        int seqNum;
                        if (!siteToSeqNum.TryGetValue(ParentSite, out seqNum))
                            seqNum = 0;

                        // update the sequence number
                        seqNum++;
                        siteToSeqNum[ParentSite] = seqNum;
                        OutSequenceNumbers[publisher] = siteToSeqNum;

                        // store the publication
                        messageToStore[4] = seqNum.ToString();
                        History.StorePublication(ParentSite, messageToStore);
                    }
                } else if (matchList != null)
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

                        List<IBroker> brokers;
                        if (Children.TryGetValue(match.Value, out brokers))
                        {
                            SentSites.Add(match.Value);

                            // determine the correct sequence number
                            IDictionary<string, int> siteToSeqNum;
                            if (!OutSequenceNumbers.TryGetValue(publisher, out siteToSeqNum))
                                siteToSeqNum = new ConcurrentDictionary<string, int>();

                            int seqNum;
                            if (!siteToSeqNum.TryGetValue(match.Value, out seqNum))
                                seqNum = 0;

                            // update the sequence number
                            seqNum++;
                            siteToSeqNum[match.Value] = seqNum;
                            OutSequenceNumbers[publisher] = siteToSeqNum;

                            // store the publication
                            messageToStore[4] = seqNum.ToString();
                            History.StorePublication(match.Value, messageToStore);
                        }

                        if (ParentSite.Equals(match.Value))
                        {
                            // determine the correct sequence number
                            IDictionary<string, int> siteToSeqNum;
                            if (!OutSequenceNumbers.TryGetValue(publisher, out siteToSeqNum))
                                siteToSeqNum = new ConcurrentDictionary<string, int>();

                            int seqNum;
                            if (!siteToSeqNum.TryGetValue(match.Value, out seqNum))
                                seqNum = 0;

                            // update the sequence number
                            seqNum++;
                            siteToSeqNum[match.Value] = seqNum;
                            OutSequenceNumbers[publisher] = siteToSeqNum;

                            // store the publication
                            messageToStore[4] = seqNum.ToString();
                            History.StorePublication(ParentSite, messageToStore);

                            SentSites.Add(match.Value);
                        }
                    }
                }
                PublicationProcessed(publisher, fromSite, sequenceNumber);
            }
        }

        private void ProcessPublication(string publisher, string topic, string publication, string fromSite, int sequenceNumber,
            string deliverProcess)
        {
            // creates a subscriber lock if needed
            object procLock;
            if (!ProcessLocks.TryGetValue(publisher, out procLock))
            {
                ProcessLocks[publisher] = new object();
            }

            lock (ProcessLocks[publisher])
            {
                if (!PublicationReceived(publisher, topic, publication, fromSite, sequenceNumber, deliverProcess))
                    return;

                int lastNumber;
                if (!InSequenceNumbers.TryGetValue(publisher, out lastNumber))
                    lastNumber = 0;

                // just in case
                if (this.OrderingGuarantee == OrderingGuarantee.Fifo && sequenceNumber == lastNumber)
                    return;

                // to be stored later 
                string[] messageToStore = {publisher, topic, publication, fromSite, ""};

                Utility.DebugLog("Receiving publication " + publication + " from " + publisher + " with seq " +
                                 sequenceNumber);

                IDictionary<string, string> matchList = GetTopicMatchList(topic);

                // If there are any subscriptions
                if (matchList != null)
                {
                    // determine the correct sequence number for the process
                    IDictionary<string, int> siteToSeqNum;
                    if (!OutSequenceNumbers.TryGetValue(publisher, out siteToSeqNum))
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
                            // update the sequence number for the local process
                            seqNum++;
                            siteToSeqNum[SiteName] = seqNum;
                            OutSequenceNumbers[publisher] = siteToSeqNum;

                            // store the publication
                            messageToStore[4] = seqNum.ToString();
                            History.StorePublication(SiteName, messageToStore);

                            Utility.DebugLog("Sending publication '" + publication + "' to " + match.Key +
                                             " with seq " +
                                             seqNum);

                            ISubscriber subscriber = (ISubscriber) proc;

                            // modified closure
                            int s1 = seqNum;
                            Thread thread =
                                new Thread(() =>
                                {
                                    try
                                    {
                                        subscriber.DeliverPublication(publisher, topic, publication, s1);
                                    } catch (RemotingException)
                                    {
                                    } catch (SocketException)
                                    {
                                    }
                                });
                            thread.Start();
                        }
                    }
                }

                if (this.RoutingPolicy == RoutingPolicy.Flood)
                {
                    foreach (KeyValuePair<string, List<IBroker>> child in Children)
                    {
                        List<IBroker> childBrokers = child.Value;

                        // we don't send the SubscriptionSet to where it came from
                        if (!child.Key.Equals(fromSite))
                        {
                            // determine the correct sequence number for the broker 
                            IDictionary<string, int> siteToSeqNum;
                            if (!OutSequenceNumbers.TryGetValue(publisher, out siteToSeqNum))
                                siteToSeqNum = new ConcurrentDictionary<string, int>();

                            int seqNum;
                            if (!siteToSeqNum.TryGetValue(child.Key, out seqNum))
                                seqNum = 0;

                            // update the sequence number
                            seqNum++;
                            siteToSeqNum[child.Key] = seqNum;
                            OutSequenceNumbers[publisher] = siteToSeqNum;

                            // store the publication
                            messageToStore[4] = seqNum.ToString();
                            History.StorePublication(child.Key, messageToStore);

                            Utility.DebugLog("Sending pub " + publication + " to site " + child.Key + " with seq " + seqNum);

                            // send log
                            if (this.LoggingLevel == LoggingLevel.Full)
                            {
                                new Thread(
                                    () =>
                                        PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + publisher + ", " +
                                                                topic)).Start();
                            }
                            Thread thread = new Thread(() =>
                            {
                                bool retry = true;
                                while (retry)
                                {
                                    // picks a random broker for load-balancing purposes
                                    int childIndex = this.Random.Next(0, childBrokers.Count);
                                    IBroker childBroker = childBrokers[childIndex];

                                    Thread subThread =
                                        new Thread(() =>
                                        {
                                            try
                                            {
                                                childBroker.DeliverPublication(publisher, topic, publication, SiteName, seqNum);
                                                retry = false;
                                            } catch (RemotingException)
                                            {
                                            } catch (SocketException)
                                            {
                                            }
                                        });

                                    subThread.Start();
                                    subThread.Join();
                                }
                            });
                            thread.Start();
                        }
                    }

                    // we don't send the subscription to where it came from
                    if (!ParentSite.Equals(fromSite) && !ParentSite.Equals("none"))
                    {
                        // determine the correct sequence number for the parent broker
                        IDictionary<string, int> siteToSeqNum;
                        if (!OutSequenceNumbers.TryGetValue(publisher, out siteToSeqNum))
                            siteToSeqNum = new ConcurrentDictionary<string, int>();

                        int seqNum;
                        if (!siteToSeqNum.TryGetValue(ParentSite, out seqNum))
                            seqNum = 0;

                        // update the sequence number
                        seqNum++;
                        siteToSeqNum[ParentSite] = seqNum;
                        OutSequenceNumbers[publisher] = siteToSeqNum;

                        // store the publication
                        messageToStore[4] = seqNum.ToString();
                        History.StorePublication(ParentSite, messageToStore);
                        Utility.DebugLog("Sending pub " + publication + " to site " + ParentSite + " with seq " + seqNum);

                        if (this.LoggingLevel == LoggingLevel.Full)
                        {
                            new Thread(
                                () =>
                                    PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + publisher + ", " + topic)).Start();
                        }

                        Thread thread = new Thread(() =>
                        {
                            bool retry = true;
                            while (retry)
                            {
                                IBroker parent;
                                lock (ParentBrokers)
                                {
                                    // picks a random broker for load-balancing purposes
                                    int parentIndex = this.Random.Next(0, ParentBrokers.Count);
                                    parent = ParentBrokers[parentIndex];
                                }

                                Thread subThread =
                                    new Thread(() =>
                                    {
                                        try
                                        {
                                            parent.DeliverPublication(publisher, topic, publication, SiteName, seqNum);
                                            retry = false;
                                        } catch (RemotingException)
                                        {
                                        } catch (SocketException)
                                        {
                                        }
                                    });
                                subThread.Start();
                                subThread.Join();
                            }
                        });

                        thread.Start();
                    }
                } else if (matchList != null)
                {
                    // if there are subscriptions to the topic, send to the appropriate sites
                    List<string> sentSites = new List<string>();

                    foreach (KeyValuePair<string, string> match in matchList)
                    {
                        // we don't want to sent multiple messages to the same site
                        if (sentSites.Contains(match.Value))
                            continue;

                        // don't send publication to where it came from
                        if (match.Value.Equals(fromSite))
                            continue;

                        List<IBroker> brokers;
                        if (Children.TryGetValue(match.Value, out brokers))
                        {
                            sentSites.Add(match.Value);

                            // determine the correct sequence number
                            IDictionary<string, int> siteToSeqNum;
                            if (!OutSequenceNumbers.TryGetValue(publisher, out siteToSeqNum))
                                siteToSeqNum = new ConcurrentDictionary<string, int>();

                            int seqNum;
                            if (!siteToSeqNum.TryGetValue(match.Value, out seqNum))
                                seqNum = 0;

                            // update the sequence number
                            seqNum++;
                            siteToSeqNum[match.Value] = seqNum;
                            OutSequenceNumbers[publisher] = siteToSeqNum;

                            // store the publication
                            messageToStore[4] = seqNum.ToString();
                            History.StorePublication(match.Value, messageToStore);

                            Utility.DebugLog("Sending pub " + publication + " to site " + match.Value +
                                             " with seq " +
                                             seqNum);

                            // send log
                            if (this.LoggingLevel == LoggingLevel.Full)
                            {
                                    new Thread(
                                        () =>
                                            PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + publisher + ", " +
                                                                    topic)).Start();
                            }

                            Thread thread = new Thread(() =>
                            {
                                bool retry = true;
                                while (retry)
                                {
                                    IBroker broker;
                                    lock (brokers)
                                    {
                                        int brokerIndex = this.Random.Next(0, brokers.Count);
                                        broker = brokers[brokerIndex];
                                    }

                                    Thread subThread =
                                        new Thread(() =>
                                        {
                                            try
                                            {
                                                broker.DeliverPublication(publisher, topic, publication, SiteName, seqNum);
                                                retry = false;
                                            } catch (RemotingException)
                                            {
                                            } catch (SocketException)
                                            {
                                            }
                                        });
                                    subThread.Start();
                                    subThread.Join();
                                }
                            });
                            thread.Start();
                        }

                        if (ParentSite.Equals(match.Value))
                        {
                            // determine the correct sequence number
                            IDictionary<string, int> siteToSeqNum;
                            if (!OutSequenceNumbers.TryGetValue(publisher, out siteToSeqNum))
                                siteToSeqNum = new ConcurrentDictionary<string, int>();

                            int seqNum;
                            if (!siteToSeqNum.TryGetValue(match.Value, out seqNum))
                                seqNum = 0;

                            // update the sequence number
                            seqNum++;
                            siteToSeqNum[match.Value] = seqNum;
                            OutSequenceNumbers[publisher] = siteToSeqNum;

                            // store the publication
                            messageToStore[4] = seqNum.ToString();
                            History.StorePublication(ParentSite, messageToStore);

                            // send log
                            if (this.LoggingLevel == LoggingLevel.Full)
                            {
                                    new Thread(
                                        () =>
                                            PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + publisher + ", " +
                                                                    topic)).Start();
                            }

                            Thread thread = new Thread(() =>
                            {
                                bool retry = true;
                                while (retry)
                                {
                                    IBroker parent;
                                    lock (ParentBrokers)
                                    {
                                        int brokerIndex = this.Random.Next(0, ParentBrokers.Count);
                                        parent = ParentBrokers[brokerIndex];
                                    }

                                    Thread subThread =
                                        new Thread(() =>
                                        {
                                            try
                                            {
                                                parent.DeliverPublication(publisher, topic, publication, SiteName, seqNum);
                                                retry = false;
                                            } catch (RemotingException)
                                            {
                                            } catch (SocketException)
                                            {
                                            }
                                        });

                                    subThread.Start();
                                    subThread.Join();
                                }
                            });

                            thread.Start();
                            sentSites.Add(match.Value);
                        }
                    }
                }
                PublicationProcessed(publisher, fromSite, sequenceNumber);
            }
        }

        /// <summary>
        ///     Decides what to do with the publication.
        /// </summary>
        /// <returns> Returns true if the message should be further subscriber, false otherwise </returns>
        private bool PublicationReceived(string publisher, string topic, string publication, string fromSite,
            int sequenceNumber, string deliverProcess)
        {
            string[] blockedMessage = new string[6];
            blockedMessage[0] = publisher;
            blockedMessage[1] = topic;
            blockedMessage[2] = publication;
            blockedMessage[3] = fromSite;
            blockedMessage[4] = sequenceNumber.ToString();
            blockedMessage[5] = deliverProcess;

            // creates a subscriber lock if needed
            object procLock;
            if (!ProcessLocks.TryGetValue(publisher, out procLock))
            {
                ProcessLocks[publisher] = new object();
            }

            lock (ProcessLocks[publisher])
            {
                if (Status.Equals(Status.Frozen))
                {
                    FrozenMessages.Add(blockedMessage);
                    return false;
                }

                int lastNumber;
                if (!InSequenceNumbers.TryGetValue(publisher, out lastNumber))
                    lastNumber = 0;

                // if we received the publication from the broker we ignore it's sequence number
                // and process it immediately.
                /* if (OrderingGuarantee == OrderingGuarantee.No && fromSite.Equals(SiteName))
                    return true;*/

                // if we're using FIFO or we're receiving from another broker, then we need to
                // deliver in order (fault tolerance)
                if (sequenceNumber > lastNumber + 1)
                {
                    MessageQueue queue;
                    if (HoldbackQueue.TryGetValue(publisher, out queue))
                    {
                        if (queue.GetSequenceNumbers().Contains(sequenceNumber))
                        {
                            return false;
                        }
                        queue.Add(blockedMessage, sequenceNumber);
                    } else
                    {
                        queue = new MessageQueue();
                        queue.Add(blockedMessage, sequenceNumber);
                        HoldbackQueue[publisher] = queue;
                    }

                    Utility.DebugLog("Delayed message detected. Queueing message '" + publication +
                                     "' with seqNo " + sequenceNumber);
                    new Thread(() =>
                    {
                        while (true)
                        {
                            // waits half a second before checking the queue.  if the minimum sequence
                            // number is this one, then our message didn't arrive and it's the one blocking 
                            // the queue. In that case, we request it
                            Thread.Sleep(500);
                            lock (HoldbackQueue)
                            {
                                MessageQueue checkQueue;
                                if (HoldbackQueue.TryGetValue(publisher, out checkQueue) &&
                                    checkQueue.GetSequenceNumbers().Any())
                                {
                                    int minSeqNo = checkQueue.GetSequenceNumbers().Min();
                                    if (minSeqNo == sequenceNumber)
                                    {
                                        RequestPublication(publisher, fromSite, sequenceNumber - 1);
                                        return;
                                    }
                                    // ifD the minimum sequence number is smaller than ours 
                                    // we wait and repeat
                                    if (minSeqNo < sequenceNumber)
                                        continue;

                                    return;
                                }
                                return;
                            }
                        }
                    }).Start();

                    return false;
                } else if (sequenceNumber <= lastNumber)
                    return false;
            }
            return true;
        }

        /// <summary>
        ///     Local function that requests a resend of a publication from a publisher
        /// or a broker
        /// </summary>
        /// <param name="publisher"></param>
        /// <param name="fromSite"></param>
        /// <param name="sequenceNumber"></param>
        private void RequestPublication(string publisher, string fromSite, int sequenceNumber)
        {
            List<IBroker> brokers = null;

            IProcess proc;
            if (fromSite.Equals(SiteName) && LocalProcesses.TryGetValue(publisher, out proc))
            {
                Utility.DebugLog("Requesting resend of pub " + sequenceNumber + " from local publisher");

                IPublisher pub = (IPublisher) proc;
                new Thread(() => pub.RequestPublication(sequenceNumber)).Start();
                return;
            }

            Children.TryGetValue(fromSite, out brokers);

            if (brokers == null && fromSite.Equals(ParentSite))
                brokers = ParentBrokers;
            else
            {
                Utility.DebugLog("WARNING: The requesting site '"+fromSite+"' couldn't be found.");
                return;
            }

            Utility.DebugLog("Requesting resend of pub " + sequenceNumber + " from remote broker");

            new Thread(() =>
            {
                bool retry = true;
                while (retry)
                {
                    IBroker broker;
                    lock (brokers)
                    {
                        // picks a random broker for load-balancing purposes
                        int brokerIndex = this.Random.Next(0, brokers.Count);
                        broker = brokers[brokerIndex];
                    }

                    Thread subThread =
                        new Thread(() =>
                        {
                            try
                            {
                                broker.ResendPublication(publisher, SiteName, sequenceNumber);
                                retry = false;
                            } catch (RemotingException)
                            {
                            } catch (SocketException)
                            {
                            }
                        });
                    subThread.Start();
                    subThread.Join();
                }
            }).Start();
        }

        /// <summary>
        ///     Updates this broker's state. Updates sequence numbers, unblocks delayed messages (if any), etc
        /// </summary>
        /// <param name="topic"> The subscribed topic </param>
        /// <param name="process"> The subscriber </param>
        /// <param name="sequenceNumber"> The message's sequence number </param>
        private void PublicationProcessed(string publisher, string fromSite, int sequenceNumber)
        {
            // creates a subscriber lock if needed
            object procLock;
            if (!ProcessLocks.TryGetValue(publisher, out procLock))
            {
                ProcessLocks[publisher] = new object();
            }

            lock (ProcessLocks[publisher])
            {
                // if we don't care about ordering and we receive directly from a broker
                // then we just assign it a seq no and stop because there won't be any delayed messages
                /*    if (OrderingGuarantee == OrderingGuarantee.No && fromSite.Equals(SiteName))
                {
                    ++InSequenceNumbers[publisher];
                    return;
                }*/

                InSequenceNumbers[publisher] = sequenceNumber;

                MessageQueue queue;
                if (HoldbackQueue.TryGetValue(publisher, out queue))
                {
                    string[] message = queue.GetAndRemove(sequenceNumber + 1);
                    if (message == null)
                    {
                        return;
                    }

                    Utility.DebugLog("Unblocking message with sequence number: " + (sequenceNumber + 1));

                    if (message[5].Equals(ProcessName))
                        ProcessPublication(message[0], message[1], message[2], message[3], int.Parse(message[4]), message[5]);
                    else
                        StorePublicationInHistory(message[0], message[1], message[2], message[3], int.Parse(message[4]),
                            message[5]);
                }
            }
        }

        /// <summary>
        ///     Request a resend for a specific publication. Can be called by a broker or a subscriber
        /// </summary>
        /// <param name="publisher"></param>
        /// <param name="requestingSite"></param>
        /// <param name="sequenceNumber"></param>
        /// <param name="subscriber"></param>
        public void ResendPublication(string publisher, string requestingSite, int sequenceNumber, string subscriber = null)
        {
            lock (ProcessLocks[publisher])
            {
                string[] message = History.GetPublication(requestingSite, publisher, sequenceNumber);

                if (message == null)
                {
                    Utility.DebugLog("No pub stored for " + publisher + " with seq no " + sequenceNumber);
                    return;
                }
                Utility.DebugLog("Resending message " + message[2] + " with seq no " + int.Parse(message[4]));

                Thread thread;
                // in this case, a subscriber requested a resend
                if (requestingSite.Equals(SiteName) && subscriber != null)
                {
                    IProcess proc;
                    ISubscriber sub;
                    if (LocalProcesses.TryGetValue(subscriber, out proc))
                    {
                        sub = (ISubscriber) proc;
                    } else
                        return;

                    thread = new Thread(() =>
                    {
                        try
                        {
                            sub.DeliverPublication(message[0], message[1], message[2], int.Parse(message[4]));
                        } catch (RemotingException)
                        {
                        } catch (SocketException)
                        {
                        }
                    });
                    thread.Start();
                    return;
                }

                List<IBroker> brokers;

                if (requestingSite.Equals(ParentSite))
                    brokers = ParentBrokers;

                else if (!Children.TryGetValue(requestingSite, out brokers))
                {
                    Utility.DebugLog("WARNING: The requesting site '" + requestingSite + "' couldn't be found.");
                    return;
                }

                if (this.LoggingLevel == LoggingLevel.Full)
                {
                    PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + publisher + ", " +
                                            message[1]);
                    /*thread =
                        new Thread(
                            () =>
                                PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + subscriber + ", " +
                                                        message[1]));
                    thread.Start();*/
                }

                Utility.DebugLog("Resending pub to " + requestingSite);
                thread =
                    new Thread(() =>
                    {
                        bool retry = true;
                        while (retry)
                        {
                            IBroker broker;
                            lock (brokers)
                            {
                                // picks a random broker for load-balancing purposes
                                int childIndex = this.Random.Next(0, brokers.Count);
                                broker = brokers[childIndex];
                            }

                            Thread subThread =
                                new Thread(() =>
                                {
                                    try
                                    {
                                        broker.DeliverPublication(message[0], message[1], message[2], message[3],
                                            int.Parse(message[4]));
                                        retry = false;
                                    } catch (RemotingException)
                                    {
                                    } catch (SocketException)
                                    {
                                    }
                                });
                            subThread.Start();
                            subThread.Join();
                        }
                    });
                thread.Start();
            }
        }

        /// <summary>
        ///     Notifies the broker of the last sequence number received. If a missing message is detected then
        /// it is retransmitted. After that it's sent back up to the publisher for further checking
        /// </summary>
        public void NotifyOfLast(string publisher, string askingSite, int sequenceNumber, string subscriber = null)
        {
            IDictionary<string, int> seqNoTable;
            if (OutSequenceNumbers.TryGetValue(publisher, out seqNoTable))
            {
                int lastSeqNo;
                if (seqNoTable.TryGetValue(askingSite, out lastSeqNo) && lastSeqNo > sequenceNumber)
                {
                    for (int i = sequenceNumber + 1; i <= lastSeqNo; i++)
                    {
                        string[] message = History.GetPublication(askingSite, publisher, i);

                        if (message == null)
                        {
                            Utility.DebugLog("No message in history for notif");
                            return;
                        }
                        // the variable is modified so we must keep it local
                        int i1 = i;

                        Utility.DebugLog("Resending message " + message[2] + " with seq no " + i1 + " after notif");

                        IProcess proc;
                        if (subscriber != null && LocalProcesses.TryGetValue(subscriber, out proc))
                        {
                            // TODO: refactor this to use the SendPublication
                            ISubscriber subProc = (ISubscriber) proc;
                            new Thread(() => subProc.DeliverPublication(message[0], message[1], message[2], i1)).Start();
                        } else if (askingSite.Equals(ParentSite))
                        {
                            new Thread(() =>
                            {
                                bool retry = true;
                                while (retry)
                                {
                                    IBroker parentBroker;
                                    lock (ParentBrokers)
                                    {
                                        // picks a random broker for load-balancing purposes
                                        int childIndex = this.Random.Next(0, ParentBrokers.Count);
                                        parentBroker = ParentBrokers[childIndex];
                                    }

                                    Thread subThread =
                                        new Thread(() =>
                                        {
                                            try
                                            {
                                                parentBroker.DeliverPublication(message[0], message[1], message[2], message[3],
                                                    i1);
                                                retry = false;
                                            } catch (RemotingException)
                                            {
                                            } catch (SocketException)
                                            {
                                            }
                                        });
                                    subThread.Start();
                                    subThread.Join();
                                }
                            }).Start();
                        }
                        List<IBroker> childList;
                        if (Children.TryGetValue(askingSite, out childList))
                        {
                            if (this.LoggingLevel == LoggingLevel.Full)
                            {
                                new Thread(
                                    () =>
                                        PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + publisher + ", " + message[1])).Start();
                            }

                            new Thread(() =>
                            {
                                bool retry = true;
                                while (retry)
                                {
                                    IBroker childBroker;
                                    lock (childList)
                                    {
                                        // picks a random broker for load-balancing purposes
                                        int childIndex = this.Random.Next(0, childList.Count);
                                        childBroker = childList[childIndex];
                                    }

                                    Thread subThread =
                                        new Thread(() =>
                                        {
                                            try
                                            {
                                                childBroker.DeliverPublication(message[0], message[1], message[2], message[3],
                                                    i1);
                                                retry = false;
                                            } catch (RemotingException)
                                            {
                                            } catch (SocketException)
                                            {
                                            }
                                        });
                                    subThread.Start();
                                    subThread.Join();
                                }
                            }).Start();
                        }
                    }
                    // we updated the broker/subscriber until the lastSeqNo
                    sequenceNumber = lastSeqNo;
                } else
                {
                    Utility.DebugLog("Received up to date notif of " + sequenceNumber);
                }

                string[] messageInfo = History.GetPublication(askingSite, publisher, sequenceNumber);
                if (messageInfo == null)
                    return;

                string fromSite = messageInfo[3];

                Utility.DebugLog("Sending up the chain");

                // TODO: when the notif arrives at site0 it somehow goes unnoticed
                // check for lost messages up the chain 
                if (fromSite.Equals(SiteName))
                {

                    int outSeqNo, intSeqNo, diff;
                    IDictionary<string, int> siteToSeqNo;
                    if (OutSequenceNumbers.TryGetValue(publisher, out siteToSeqNo) &&
                        siteToSeqNo.TryGetValue(askingSite, out outSeqNo) &&
                        InSequenceNumbers.TryGetValue(publisher, out intSeqNo))
                    {
                        diff = intSeqNo - outSeqNo;
                        if (diff < 0)
                            return;
                    }
                    else
                    {
                        return;
                    }

                    Utility.DebugLog("Asking publisher about "+(sequenceNumber+diff));

                    // Notify publisher directly
                    IProcess proc;
                    if (LocalProcesses.TryGetValue(publisher, out proc))
                    {
                        IPublisher pub = (IPublisher) proc;

                        Thread pubThread = new Thread(() =>
                        {
                            bool retry = true;
                            while (retry)
                            {
                                Thread subThread =
                                    new Thread(() =>
                                    {
                                        try
                                        {
                                            pub.NotifyOfLast(publisher, SiteName, sequenceNumber + diff);
                                            retry = false;
                                        } catch (RemotingException)
                                        {
                                        } catch (SocketException)
                                        {
                                        }
                                    });
                                subThread.Start();
                                subThread.Join();
                            }
                        });

                        pubThread.Start();
                    }
                } else
                {
                    List<IBroker> brokerList = null;
                    Utility.DebugLog("Notif: Site is " + fromSite);
                    if (ParentSite.Equals(fromSite))
                    {
                        Utility.DebugLog("ASK PARENT");
                        lock (ParentBrokers)
                        {
                            brokerList = ParentBrokers;
                        }
                    } else
                    {
                        Utility.DebugLog("ASK CHILD");

                        if (!Children.TryGetValue(fromSite, out brokerList))
                            return;
                    }

                    new Thread(() =>
                    {
                        bool retry = true;
                        while (retry)
                        {
                            Thread subThread =
                                new Thread(() =>
                                {
                                    try
                                    {
                                        brokerList[this.Random.Next(0, brokerList.Count)].NotifyOfLast(publisher, SiteName, sequenceNumber);
                                        retry = false;
                                    } catch (RemotingException)
                                    {
                                    } catch (SocketException)
                                    {
                                    }
                                });
                            subThread.Start();
                            subThread.Join();
                        }
                    }).Start();
                }
            }
        }

        //************************************************
        //
        //          Subscription Methods
        //
        //************************************************

        public void DeliverSubscription(string subscriber, string topic, string siteName, int sequenceNumber)
        {
            lock (this)
            {
                object objLock;
                if (!ProcessLocks.TryGetValue(subscriber, out objLock))
                {
                    ProcessLocks[subscriber] = new object();
                }
            }

            lock (Subscribing)
            {
                    // if we're using more than one broker
                    if (SiblingBrokers.Count != 0)
                    {
                        foreach (var broker in SiblingBrokers)
                        {
                            Thread thread =
                                new Thread(() =>
                                {
                                    try
                                    {
                                        broker.InformOfSubscription(subscriber, topic, siteName, sequenceNumber, ProcessName, InSequenceNumbers, HoldbackQueue, OutSequenceNumbers);
                                    } catch (RemotingException)
                                    {
                                    } catch (SocketException)
                                    {
                                    }
                                });
                            thread.Start();
                        }
                    } else
                    {
                        ProcessSubscription(subscriber, topic, siteName, sequenceNumber, ProcessName);
                    }
                }
        }

        public void InformOfSubscription(string subscriber, string topic, string siteName, int sequenceNumber,
            string deliverProcess, IDictionary<string, int> state, IDictionary<string, MessageQueue> queue, IDictionary<string, IDictionary<string, int>> outSeqs)
        {
            lock (Subscribing)
            {
                InSequenceNumbers = state;
                HoldbackQueue = queue;
                OutSequenceNumbers = outSeqs;

                // TODO: remove this to enable the load balancing for subs
               // deliverProcess = ProcessName;
                if (deliverProcess.Equals(ProcessName))
                {
                    // if the subscription was already processed, we don't multicast it
                    if (!ProcessSubscription(subscriber, topic, siteName, sequenceNumber, deliverProcess))
                        return;
                }
                else
                {
                    // if the subscription was already added, we don't multicast it
                    if (!StoreSubscription(subscriber, topic, siteName, sequenceNumber, deliverProcess))
                        return;
                }

                lock (SiblingBrokers)
                {
                    // multicast the subscription
                    foreach (var broker in SiblingBrokers)
                    {
                        Thread thread =
                            new Thread(() =>
                            {
                                try
                                {
                                    broker.InformOfSubscription(subscriber, topic, siteName, sequenceNumber, deliverProcess, state, queue, outSeqs);
                                }
                                catch (RemotingException)
                                {
                                }
                                catch (SocketException)
                                {
                                }
                            });
                        thread.Start();
                    }
                }
           }
            
        }

        private void SubscriptionProcessed(string subscriber, int sequenceNumber)
        {
            if (this.OrderingGuarantee != OrderingGuarantee.Fifo)
                return;

            // creates a subscriber lock if needed
            object procLock;
            if (!ProcessLocks.TryGetValue(subscriber, out procLock))
            {
                ProcessLocks[subscriber] = new object();
            }

            lock (ProcessLocks[subscriber])
            {
                InSequenceNumbers[subscriber] = sequenceNumber;
                MessageQueue queue;
                if (HoldbackQueue.TryGetValue(subscriber, out queue))
                {
                    string[] message = queue.GetAndRemove(sequenceNumber + 1);
                    if (message == null)
                    {
                        return;
                    }

                    Utility.DebugLog("Unblocking message with sequence number: " + (sequenceNumber + 1));

                    if (message[5].Equals("Sub"))
                    {
                        if (message[4].Equals(ProcessName))
                            ProcessSubscription(message[0], message[1], message[2], int.Parse(message[3]), message[4]);
                        else
                            StoreSubscription(message[0], message[1], message[2], int.Parse(message[3]), message[4]);
                    } else
                    {
                      //  if (message[4].Equals(ProcessName))
                            ProcessUnsubscription(message[0], message[1], message[2], int.Parse(message[3]));

                    }

                }
            }
        }

        /// <summary>
        ///     Stores the subscription and sends it to other sites. Returns true
        /// if the subscription was successfuly added, false if it already existed
        /// </summary>
        private bool ProcessSubscription(string subscriber, string topic, string siteName, int sequenceNumber,
            string deliverProcess)
        {
            // creates a subscriber lock if needed
            lock (this)
            {
                object procLock;
                if (!ProcessLocks.TryGetValue(subscriber, out procLock))
                {
                    ProcessLocks[subscriber] = new object();
                }
            }

            lock (ProcessLocks[subscriber])
            {
                // stop if the subscription is already there
                if (!StoreSubscription(subscriber, topic, siteName, sequenceNumber, deliverProcess))
                    return false;

                foreach (KeyValuePair<string, List<IBroker>> child in Children)
                {
                    List<IBroker> childBrokers = child.Value;

                    // we don't send the SubscriptionSet to where it came from
                    if (!child.Key.Equals(siteName))
                    {
                        Utility.DebugLog("Sending subcription of " + subscriber + " to site " + siteName +
                                         " with seq " + sequenceNumber);

                        Thread thread =
                            new Thread(() =>
                            {
                                bool retry = true;
                                while (retry)
                                {
                                    IBroker childBroker;
                                    lock (childBrokers)
                                    {
                                        // picks a random broker for load-balancing purposes
                                        int childIndex = this.Random.Next(0, childBrokers.Count);
                                        childBroker = childBrokers[childIndex];
                                    }

                                    Thread subThread =
                                        new Thread(() =>
                                        {
                                            try
                                            {
                                                childBroker.DeliverSubscription(subscriber, topic, SiteName, sequenceNumber);
                                                retry = false;
                                            } catch (RemotingException)
                                            {
                                            } catch (SocketException)
                                            {
                                            }
                                        });
                                    subThread.Start();
                                    subThread.Join();
                                }
                            });
                        thread.Start();
                    }
                }

                // we don't send the subscription to where it came from
                if (!ParentSite.Equals(siteName) && !ParentSite.Equals("none"))
                {
                    Utility.DebugLog("Sending subcription of " + subscriber + " to site " + siteName +
                                     " with seq " + sequenceNumber);
                    Thread thread = new Thread(() =>
                    {
                        bool retry = true;
                        while (retry)
                        {
                            IBroker parent;
                            lock (ParentBrokers)
                            {
                                // picks a random broker for load-balancing purposes
                                int parentIndex = this.Random.Next(0, ParentBrokers.Count);
                                parent = ParentBrokers[parentIndex];
                            }

                            Thread subThread =
                                new Thread(() =>
                                {
                                    try
                                    {
                                        parent.DeliverSubscription(subscriber, topic, SiteName, sequenceNumber);
                                        retry = false;
                                    } catch (RemotingException)
                                    {
                                    } catch (SocketException)
                                    {
                                    }
                                });
                            subThread.Start();
                            subThread.Join();
                        }
                    });
                    thread.Start();
                }
            }
            return true;
        }

        /// <summary>
        ///     Stores the subscriptions in the appropriate structures. Returns true if 
        /// the subscription was added, false if it already existed (or if there is a delayed message)
        /// </summary>
        /// <returns> </returns>
        private bool StoreSubscription(string subscriber, string topic, string siteName, int sequenceNumber, string deliveProcess)
        {
            // TODO: enable this for load balancing
              if (!SubscriptionReceived(subscriber, topic, siteName, sequenceNumber, deliveProcess))
                return false;

            // get or create the subscription for this topic
            SubscriptionSet subscriptionSet;
            if (!RoutingTable.TryGetValue(topic, out subscriptionSet))
            {
                subscriptionSet = new SubscriptionSet(topic);
            }

            // this prevents the same message from being processed twice
            if (subscriptionSet.IsSubscribed(subscriber))
            {
                //Utility.DebugLog("Already subscribed. Discarding");
                return false;
            }

            // add to routing table
            if (deliveProcess.Equals(ProcessName))
                Utility.DebugLog("Receiving subscription on topic " + topic + " from  " + subscriber);
            else
                Utility.DebugLog("Receiving subscription update from  " + subscriber + " with seq no " + sequenceNumber);
            subscriptionSet.AddSubscriber(subscriber, siteName);
            RoutingTable[topic] = subscriptionSet;

            // stores the subscription in the history - for resending later if needed
            // TODO: uncomment this to enable the load balancing for subs
             History.StoreSubscription(subscriber, topic, sequenceNumber);
            SubscriptionProcessed(subscriber, sequenceNumber);
            return true;
        }

        private bool SubscriptionReceived(string subscriber, string topic, string siteName, int sequenceNumber,
            string deliverProcess)
        {
            string[] eventMessage = new string[6];
            eventMessage[0] = subscriber;
            eventMessage[1] = topic;
            eventMessage[2] = siteName;
            eventMessage[3] = sequenceNumber.ToString();
            eventMessage[4] = deliverProcess;
            eventMessage[5] = "Sub";

            // creates a subscriber lock if needed
            object procLock;
            if (!ProcessLocks.TryGetValue(subscriber, out procLock))
            {
                ProcessLocks[subscriber] = new object();
            }

            lock (ProcessLocks[subscriber])
            {
                //lock (HoldbackQueue)
                //{
                if (Status.Equals(Status.Frozen))
                {
                    FrozenMessages.Add(eventMessage);
                    return false;
                }

                // TODO: important tests!
                //      - freeze a broker and see if it blocks the other by hiding a message
                // and how they deal with that
                //      - start documenting these tests and the systems response to them
                //      - make this run with IPs 
                //      - put the ordering to NO and make the necessary changes for the system 
                // to behave in a fault tolerance way

                int lastNumber;
                if (!InSequenceNumbers.TryGetValue(subscriber, out lastNumber))
                    lastNumber = 0;

                if (this.OrderingGuarantee == OrderingGuarantee.Fifo && sequenceNumber > lastNumber + 1)
                {
                    MessageQueue queue;
                    if (HoldbackQueue.TryGetValue(subscriber, out queue))
                    {
                        if (queue.GetSequenceNumbers().Contains(sequenceNumber))
                        {
                            return false;
                        }
                        queue.Add(eventMessage, sequenceNumber);
                    } else
                    {
                        queue = new MessageQueue();
                        queue.Add(eventMessage, sequenceNumber);
                        HoldbackQueue[subscriber] = queue;
                    }

                    Utility.DebugLog("Delayed message detected. Queueing message subscription by '" + subscriber +
                                     "' with seqNo " + sequenceNumber);

                    // TODO: uncomment this to enable the load balancing for subs
                    /*new Thread(() =>
                    {
                        bool retry = true;
                        while (retry)
                        {
                            // waits half a second before checking the queue.  if the minimum sequence
                            // number is this one, then our message didn't arrive and it's the one blocking 
                            // the queue. In that case, we request it
                            Thread.Sleep(500);
                            lock (HoldbackQueue)
                            {
                                MessageQueue checkQueue;
                                if (HoldbackQueue.TryGetValue(subscriber, out checkQueue) &&
                                    checkQueue.GetSequenceNumbers().Any())
                                {
                                    int minSeqNo = checkQueue.GetSequenceNumbers().Min();
                                    if (minSeqNo == sequenceNumber)
                                    {
                                        RequestSubscription(subscriber, siteName, sequenceNumber);
                                        return;
                                    }
                                    // if the minimum sequence number is smaller than ours 
                                    // we wait and repeat
                                    if (minSeqNo < sequenceNumber)
                                        continue;

                                    return;
                                }
                                return;
                            }
                        }
                    }).Start();*/

                    return false;
                }
            }
            return true;
        }

        /// <summary>
        ///     Requests a resend of a subscription by another process
        /// </summary>
        /// <param name="subscriber"></param>
        /// <param name="fromSite"></param>
        /// <param name="sequenceNumber"></param>
        private void RequestSubscription(string subscriber, string fromSite, int sequenceNumber)
        {
            List<IBroker> brokers = null;

            IProcess proc;
            if (fromSite.Equals(SiteName) && LocalProcesses.TryGetValue(subscriber, out proc))
            {
                Utility.DebugLog("Requesting resend of sub " + (sequenceNumber - 1) + " from local subscriber");

                IPublisher pub = (IPublisher) proc;
                new Thread(() => pub.RequestPublication(sequenceNumber - 1)).Start();
                return;
            }

            Children.TryGetValue(fromSite, out brokers);

            if (brokers == null && fromSite.Equals(ParentSite))
                brokers = ParentBrokers;
            else
            {
                Utility.DebugLog("WARNING: The requesting site couldn't be found.");
                return;
            }

            Utility.DebugLog("Requesting resend sub " + (sequenceNumber - 1) + " from remote broker");
            new Thread(() =>
            {
                bool retry = true;
                while (retry)
                {
                    IBroker broker;
                    lock (brokers)
                    {
                        // picks a random broker for load-balancing purposes
                        int brokerIndex = this.Random.Next(0, brokers.Count);
                        broker = brokers[brokerIndex];
                    }

                    Thread subThread =
                        new Thread(() =>
                        {
                            try
                            {
                                broker.ResendSubscription(subscriber, SiteName, sequenceNumber - 1);
                                retry = false;
                            } catch (RemotingException)
                            {
                            } catch (SocketException)
                            {
                            }
                        });
                    subThread.Start();
                    subThread.Join();
                }
            }).Start();
        }

        /// <summary>
        ///     Resends the requested subscription to the requesting site
        /// </summary>
        /// <param name="subscriber"></param>
        /// <param name="requestingSite"></param>
        /// <param name="sequenceNumber"></param>
        public void ResendSubscription(string subscriber, string requestingSite, int sequenceNumber)
        {
            lock (ProcessLocks[subscriber])
            {
                string[] message = History.GetSubscription(subscriber, sequenceNumber);

                if (message == null)
                {
                    Utility.DebugLog("No subscription stored for " + subscriber + " with seq no " + sequenceNumber);
                    return;
                }

                List<IBroker> brokers;
                if (requestingSite.Equals(ParentSite))
                    brokers = ParentBrokers;

                else if (!Children.TryGetValue(requestingSite, out brokers))
                {
                    Utility.DebugLog("WARNING: The requesting site '" + requestingSite + "' couldn't be found.");
                    return;
                }

                Utility.DebugLog("Resending sub to " + requestingSite);
                new Thread(() =>
                {
                    bool retry = true;
                    while (retry)
                    {
                        IBroker broker;
                        lock (brokers)
                        {
                            // picks a random broker for load-balancing purposes
                            int childIndex = this.Random.Next(0, brokers.Count);
                            broker = brokers[childIndex];
                        }

                        Thread subThread =
                            new Thread(() =>
                            {
                                try
                                {
                                    broker.DeliverSubscription(message[0], message[1], SiteName, int.Parse(message[2]));
                                    retry = false;
                                } catch (RemotingException)
                                {
                                } catch (SocketException)
                                {
                                }
                            });
                        subThread.Start();
                        subThread.Join();
                    }
                }).Start();
            }
        }

        //************************************************
        //
        //          Unsubscription Methods
        //
        //************************************************

        public void DeliverUnsubscription(string subscriber, string topic, string siteName, int sequenceNumber)
        {
            lock (this)
            {
                object objLock;
                if (!ProcessLocks.TryGetValue(subscriber, out objLock))
                {
                    ProcessLocks[subscriber] = new object();
                }
            }

            lock (ProcessLocks[subscriber])
            {
                Console.Out.WriteLine("Deliver unsub");

                lock (SiblingBrokers)
                {
                    // if we're using more than one broker
                    if (SiblingBrokers.Count != 0)
                    {
                        // multicast the unsubscription
                        foreach (IBroker broker in SiblingBrokers)
                        {
                            Thread thread = new Thread(() =>
                            {
                                try
                                {
                                    broker.InformOfUnsubscription(subscriber, topic, siteName, sequenceNumber);
                                } catch (RemotingException)
                                {
                                } catch (SocketException)
                                {
                                }
                            });
                            thread.Start();
                        }
                    } else
                    {
                        ProcessUnsubscription(subscriber, topic, siteName, sequenceNumber);
                    }
                }
            }
        }

        private bool UnsubscriptionReceived(string subscriber, string topic, string siteName, int sequenceNumber,
            string deliverProcess)
        {
            string[] eventMessage = new string[6];
            eventMessage[0] = subscriber;
            eventMessage[1] = topic;
            eventMessage[2] = siteName;
            eventMessage[3] = sequenceNumber.ToString();
            eventMessage[4] = deliverProcess;
            eventMessage[5] = "Unsub";

            // creates a subscriber lock if needed
            object procLock;
            if (!ProcessLocks.TryGetValue(subscriber, out procLock))
            {
                ProcessLocks[subscriber] = new object();
            }

            lock (ProcessLocks[subscriber])
            {
                if (Status.Equals(Status.Frozen))
                {
                    FrozenMessages.Add(eventMessage);
                    return false;
                }

                int lastNumber;
                if (!InSequenceNumbers.TryGetValue(subscriber, out lastNumber))
                    lastNumber = 0;

                if (this.OrderingGuarantee == OrderingGuarantee.Fifo && sequenceNumber > lastNumber + 1)
                {
                    MessageQueue queue;
                    if (HoldbackQueue.TryGetValue(subscriber, out queue))
                    {
                        if (queue.GetSequenceNumbers().Contains(sequenceNumber))
                        {
                            return false;
                        }
                        queue.Add(eventMessage, sequenceNumber);
                    }
                    else
                    {
                        queue = new MessageQueue();
                        queue.Add(eventMessage, sequenceNumber);
                        HoldbackQueue[subscriber] = queue;
                    }

                    Utility.DebugLog("Delayed message detected. Queueing message unsubscription by '" + subscriber +
                                     "' with seqNo " + sequenceNumber);

                    // TODO: uncomment this to enable the load balancing for subs
                    /*new Thread(() =>
                    {
                        bool retry = true;
                        while (retry)
                        {
                            // waits half a second before checking the queue.  if the minimum sequence
                            // number is this one, then our message didn't arrive and it's the one blocking 
                            // the queue. In that case, we request it
                            Thread.Sleep(500);
                            lock (HoldbackQueue)
                            {
                                MessageQueue checkQueue;
                                if (HoldbackQueue.TryGetValue(subscriber, out checkQueue) &&
                                    checkQueue.GetSequenceNumbers().Any())
                                {
                                    int minSeqNo = checkQueue.GetSequenceNumbers().Min();
                                    if (minSeqNo == sequenceNumber)
                                    {
                                        RequestSubscription(subscriber, siteName, sequenceNumber);
                                        return;
                                    }
                                    // if the minimum sequence number is smaller than ours 
                                    // we wait and repeat
                                    if (minSeqNo < sequenceNumber)
                                        continue;

                                    return;
                                }
                                return;
                            }
                        }
                    }).Start();*/

                    return false;
                }
            }
            return true;
        }

        public void ProcessUnsubscription(string subscriber, string topic, string siteName, int sequenceNumber)
        {
            // creates a subscriber lock if needed
            lock (this)
            {
                object procLock;
                if (!ProcessLocks.TryGetValue(subscriber, out procLock))
                {
                    ProcessLocks[subscriber] = new object();
                }
            }

            lock (ProcessLocks[subscriber])
            {
                // this prevents the same message from being processed twice
                SubscriptionSet subscriptionSet;
                if (!RoutingTable.TryGetValue(topic, out subscriptionSet) || !subscriptionSet.IsSubscribed(subscriber))
                {
                    Utility.DebugLog("Already unsubscribed. Discarding");
                    return;
                }

                Utility.DebugLog("Receiving unsubscription on topic " + topic + " from  " + subscriber);

                subscriptionSet.RemoveSubscriber(subscriber);
                RoutingTable[topic] = subscriptionSet;

                foreach (KeyValuePair<string, List<IBroker>> child in Children)
                {
                    List<IBroker> childBrokers = child.Value;

                    // we don't send the SubscriptionSet to where it came from
                    if (!child.Key.Equals(siteName))
                    {
                        Thread thread = new Thread(() =>
                        {
                            bool retry = true;
                            while (retry)
                            {
                                // picks a random broker for load-balancing purposes
                                int childIndex = this.Random.Next(0, childBrokers.Count);
                                IBroker childBroker = childBrokers[childIndex];

                                Thread subThread = new Thread(() =>
                                {
                                    try
                                    {
                                        childBroker.DeliverUnsubscription(subscriber, topic, SiteName, sequenceNumber);
                                        retry = false;
                                    } catch (RemotingException)
                                    {
                                    } catch (SocketException)
                                    {
                                    }
                                });
                                subThread.Start();
                                subThread.Join();
                            }
                        });
                        thread.Start();
                    }
                }

                // we don't send the subscription to where it came from
                if (!ParentSite.Equals(siteName) && !ParentSite.Equals("none"))
                {
                    Thread thread = new Thread(() =>
                    {
                        bool retry = true;
                        while (retry)
                        {
                            IBroker parent;
                            lock (ParentBrokers)
                            {
                                // picks a random broker for load-balancing purposes
                                int parentIndex = this.Random.Next(0, ParentBrokers.Count);
                                parent = ParentBrokers[parentIndex];
                            }

                            Thread subThread = new Thread(() =>
                            {
                                try
                                {
                                    parent.DeliverUnsubscription(subscriber, topic, SiteName, sequenceNumber);
                                    retry = false;
                                } catch (RemotingException)
                                {
                                } catch (SocketException)
                                {
                                }
                            });
                            subThread.Start();
                            subThread.Join();
                        }
                    });

                    thread.Start();
                }
                UnsubscriptionProcessed(subscriber, sequenceNumber);
            }
        }

        public void InformOfUnsubscription(string subscriber, string topic, string siteName, int sequenceNumber)
        {
            // creates a subscriber lock if needed
            lock (this)
            {
                object procLock;
                if (!ProcessLocks.TryGetValue(subscriber, out procLock))
                {
                    ProcessLocks[subscriber] = new object();
                }
            }

            lock (ProcessLocks[subscriber])
            {
                // check if the subscription wasn't already removed
                SubscriptionSet set;
                if (!RoutingTable.TryGetValue(topic, out set) || (!set.Processes.ContainsKey(subscriber)))
                {
                    Console.Out.WriteLine("ALREADY UNSUBBED");
                    return;
                }

                // multicast the unsubscription
                foreach (IBroker broker in SiblingBrokers)
                {
                    Thread thread = new Thread(() =>
                    {
                        try
                        {
                            broker.InformOfUnsubscription(subscriber, topic, siteName, sequenceNumber);
                        } catch (RemotingException)
                        {
                        } catch (SocketException)
                        {
                        }
                    });
                    thread.Start();
                }

                // and deliver it
                ProcessUnsubscription(subscriber, topic, siteName, sequenceNumber);
            }
        }

        private void UnsubscriptionProcessed(string subscriber, int sequenceNumber)
        {
            if (this.OrderingGuarantee != OrderingGuarantee.Fifo)
                return;

            // creates a subscriber lock if needed
            object procLock;
            if (!ProcessLocks.TryGetValue(subscriber, out procLock))
            {
                ProcessLocks[subscriber] = new object();
            }

            lock (ProcessLocks[subscriber])
            {
                InSequenceNumbers[subscriber] = sequenceNumber;
                MessageQueue queue;
                if (HoldbackQueue.TryGetValue(subscriber, out queue))
                {
                    string[] message = queue.GetAndRemove(sequenceNumber + 1);
                    if (message == null)
                    {
                        return;
                    }

                    Utility.DebugLog("Unblocking message with sequence number: " + (sequenceNumber + 1));

                    if (message[5].Equals(ProcessName))
                        ProcessSubscription(message[0], message[1], message[2], int.Parse(message[3]), message[4]);
                    else
                        StoreSubscription(message[0], message[1], message[2], int.Parse(message[3]), message[4]);
                }
            }
        }

        public void AddSiblingBroker(string siblingUrl)
        {
            Utility.DebugLog("Received sibling " + siblingUrl);
            IBroker sibling = (IBroker) Activator.GetObject(typeof (IBroker), siblingUrl);

            try
            {
                sibling.Ping();
            } catch (Exception)
            {
                Utility.DebugLog("\tERROR: The sibling broker " + siblingUrl + " is down.");
                return;
            }
            lock (SiblingBrokers)
            {
                SiblingBrokers.Add(sibling);
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
                    return false;
            }
            return true;
        }

        /// <summary>
        ///     Obtains a match list (sites and processes) that are subscribed to the given topic
        /// </summary>
        /// <param name="topic"> The published topic </param>
        /// <returns></returns>
        private IDictionary<string, string> GetTopicMatchList(string topic)
        {
            IDictionary<string, string> matchList = null;

            SubscriptionSet subs;
            foreach (string subject in RoutingTable.Keys)
            {
                if (subject.Contains("/*"))
                {
                    string baseTopic = topic.Remove(subject.IndexOf("/*"));

                    if (Utility.StringEquals(subject, baseTopic) && RoutingTable.TryGetValue(subject, out subs))
                    {
                        if (matchList == null)
                            matchList = new ConcurrentDictionary<string, string>();

                        foreach (KeyValuePair<string, string> match in subs.GetMatchList())
                        {
                            matchList[match.Key] = match.Value;
                        }
                    }
                } else
                {
                    if (RoutingTable.TryGetValue(subject, out subs))
                    {
                        if (matchList == null)
                            matchList = new ConcurrentDictionary<string, string>();

                        foreach (KeyValuePair<string, string> match in subs.GetMatchList())
                        {
                            matchList[match.Key] = match.Value;
                        }
                    }
                }
            }
            return matchList;
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

        public void ProcessFrozenListCommands()
        {
            string[] command;
            while (CommandBacklog.TryDequeue(out command))
            {
                if (!DeliverCommand(command))
                {
                    DeliverPublication(command[1], command[2], command[3], command[4], int.Parse(command[5]));
                }
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