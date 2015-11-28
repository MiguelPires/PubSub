#region

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        public IDictionary<string, int> InSequenceNumbers { get; } = new ConcurrentDictionary<string, int>();
        // sequence numbers sent to other processes - example: int seq = OutSeq [proc][site] 
        public IDictionary<string, IDictionary<string, int>> OutSequenceNumbers { get; } =
            new ConcurrentDictionary<string, IDictionary<string, int>>();

        // hold-back queue used for storing delayed messages
        public IDictionary<string, MessageQueue> HoldbackQueue { get; } =
            new ConcurrentDictionary<string, MessageQueue>();

        // the prevents sending the same message multiple times by the same process
        public IDictionary<string, object> ProcessLocks { get; } = new ConcurrentDictionary<string, object>();
        // history of messages sent by publishers to each site
        public MessageHistory History { get; } = new MessageHistory();

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

                IBroker parBroker = UtilityFunctions.TryConnection(fun, brokerUrl);
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
                Console.WriteLine("There already is a subscriber named " + procName + " at this broker (replaced anyway)");

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
                                    broker.InformOfPublication(publisher, topic, publication, fromSite, sequenceNumber,
                                        ProcessName);
                                    // TODO: for testing purposes only!!
                                    // hangs the process that should deliver the publication
                                    /* if (ProcessName.Equals("broker3"))
                                            Process.GetCurrentProcess().Kill();*/
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
                // check if the publication was already delivered
                int lastNumber;
                if (InSequenceNumbers.TryGetValue(publisher, out lastNumber) && lastNumber >= sequenceNumber)
                    return;

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
                } else
                {
                    // stores the publication for the appropriate processes
                    StorePublicationInHistory(publisher, topic, publication, fromSite, sequenceNumber, deliverProcess);
                }

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

                Console.Out.WriteLine("Update pub'" + publication + "' seq " + sequenceNumber);
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
                PublicationProcessed(publisher, sequenceNumber);
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

                Console.Out.WriteLine("Receiving publication " + publication + " from " + publisher + " with seq " +
                                      sequenceNumber);

                // TODO: refactorizar este metodo !!!

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

                            Console.Out.WriteLine("Sending publication '" + publication + "' to " + match.Key +
                                                  " with seq " +
                                                  seqNum);

                            ISubscriber subscriber = (ISubscriber) proc;

                            Thread thread =
                                new Thread(() =>
                                {
                                    try
                                    {
                                        subscriber.DeliverPublication(publisher, topic, publication, seqNum);
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

                            // send log
                            if (this.LoggingLevel == LoggingLevel.Full)
                            {
                                Thread logThread =
                                    new Thread(
                                        () =>
                                            PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + publisher + ", " +
                                                                    topic));
                                logThread.Start();
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


                        if (this.LoggingLevel == LoggingLevel.Full)
                        {
                            Thread logThread =
                                new Thread(
                                    () =>
                                        PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + publisher + ", " + topic));
                            logThread.Start();
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

                            Console.Out.WriteLine("Sending pub " + publication + " to site " + match.Value +
                                                  " with seq " +
                                                  seqNum);

                            // send log
                            if (this.LoggingLevel == LoggingLevel.Full)
                            {
                                Thread logThread =
                                    new Thread(
                                        () =>
                                            PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + publisher + ", " +
                                                                    topic));
                                logThread.Start();
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
                                Thread logThread =
                                    new Thread(
                                        () =>
                                            PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + publisher + ", " +
                                                                    topic));
                                logThread.Start();
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
                            SentSites.Add(match.Value);
                        }
                    }
                }
                PublicationProcessed(publisher, sequenceNumber);
            }
        }

        /// <summary>
        ///     Decides what to do with the publication.
        /// </summary>
        /// <param name="publisher"></param>
        /// <param name="topic"></param>
        /// <param name="publication"></param>
        /// <param name="sequenceNumber"></param>
        /// <returns> Returns true if the message should be further subscriber, false otherwise </returns>
        private bool PublicationReceived(string publisher, string topic, string publication, string fromSite,
            int sequenceNumber, string deliverProcess)
        {
            string[] eventMessage = new string[7];
            eventMessage[0] = "DeliverPublication";
            eventMessage[1] = publisher;
            eventMessage[2] = topic;
            eventMessage[3] = publication;
            eventMessage[4] = fromSite;
            eventMessage[5] = sequenceNumber.ToString();
            eventMessage[6] = deliverProcess;

            // creates a subscriber lock if needed
            object procLock;
            if (!ProcessLocks.TryGetValue(publisher, out procLock))
            {
                ProcessLocks[publisher] = new object();
            }

            lock (ProcessLocks[publisher])
            {
                //lock (HoldbackQueue)
                //{
                if (Status.Equals(Status.Frozen))
                {
                    FrozenMessages.Add(eventMessage);
                    return false;
                }

                int lastNumber;
                if (!InSequenceNumbers.TryGetValue(publisher, out lastNumber))
                    lastNumber = 0;

                if (this.OrderingGuarantee == OrderingGuarantee.Fifo && sequenceNumber > lastNumber + 1)
                {
                    MessageQueue queue;
                    if (HoldbackQueue.TryGetValue(publisher, out queue))
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
                        HoldbackQueue[publisher] = queue;
                    }

                    Console.Out.WriteLine("Delayed message detected. Queueing message '" + publication +
                                          "' with seqNo " + sequenceNumber);
                    new Thread(() =>
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
                                if (HoldbackQueue.TryGetValue(publisher, out checkQueue) &&
                                    checkQueue.GetSequenceNumbers().Any())
                                {
                                    int minSeqNo = checkQueue.GetSequenceNumbers().Min();
                                    if (minSeqNo == sequenceNumber)
                                    {
                                        Request(publisher, fromSite, sequenceNumber);
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
                    }).Start();

                    return false;
                }
            }
            return true;
        }

        private void Request(string publisher, string fromSite, int sequenceNumber)
        {
            List<IBroker> brokers = null;

            IProcess proc;
            if (fromSite.Equals(SiteName) && LocalProcesses.TryGetValue(publisher, out proc))
            {
                Console.Out.WriteLine("Requesting resend from local publisher");

                IPublisher pub = (IPublisher) proc;
                new Thread(() => pub.RequestPublication(sequenceNumber - 1)).Start();
                return;
            }

            Children.TryGetValue(fromSite, out brokers);

            if (brokers == null && fromSite.Equals(ParentSite))
                brokers = ParentBrokers;
            else
            {
                Console.Out.WriteLine("WARNING: The requesting site couldn't be found.");
                return;
            }

            Console.Out.WriteLine("Requesting resend from remote broker");
            Thread thread =
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
                                    broker.RequestPublication(publisher, SiteName, sequenceNumber - 1);
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

        /// <summary>
        ///     Updates this broker's state. Updates sequence numbers, unblocks delayed messages (if any), etc
        /// </summary>
        /// <param name="topic"> The subscribed topic </param>
        /// <param name="process"> The publisher </param>
        /// <param name="sequenceNumber"> The message's sequence number </param>
        private void PublicationProcessed(string publisher, int sequenceNumber)
        {
            if (this.OrderingGuarantee != OrderingGuarantee.Fifo)
                return;

            // creates a subscriber lock if needed
            object procLock;
            if (!ProcessLocks.TryGetValue(publisher, out procLock))
            {
                ProcessLocks[publisher] = new object();
            }

            lock (ProcessLocks[publisher])
            {
                //lock (HoldbackQueue)
                //{
                InSequenceNumbers[publisher] = sequenceNumber;
                MessageQueue queue;
                if (HoldbackQueue.TryGetValue(publisher, out queue))
                {
                    string[] message = queue.GetAndRemove(sequenceNumber + 1);
                    if (message == null)
                    {
                        return;
                    }
                    Console.Out.WriteLine("Unblocking message with sequence number: " + (sequenceNumber + 1));

                    if (message[6].Equals(ProcessName))
                        ProcessPublication(message[1], message[2], message[3], message[4], int.Parse(message[5]), message[6]);
                    else
                        StorePublicationInHistory(message[1], message[2], message[3], message[4], int.Parse(message[5]),
                            message[6]);
                }
            }
        }

        public void RequestPublication(string publisher, string requestingSite, int sequenceNumber, string subscriber=null)
        {
            lock (ProcessLocks[publisher])
            {
                string[] message = History.GetPublication(requestingSite, publisher, sequenceNumber);

                if (message == null)
                {
                    Console.Out.WriteLine("No pub stored for " + publisher + " with seq no " + sequenceNumber);
                    return;
                }

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

                    thread = new Thread(() => {
                     try
                     {
                         sub.DeliverPublication(message[0], message[1], message[2], int.Parse(message[4]));
                     }
                     catch (RemotingException)
                     {
                     }
                     catch (SocketException)
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
                    Console.Out.WriteLine("WARNING: The requesting site '" + requestingSite + "' couldn't be found.");
                    return;
                }

                if (this.LoggingLevel == LoggingLevel.Full)
                {
                    PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + publisher + ", " +
                                            message[1]);
                    /*thread =
                        new Thread(
                            () =>
                                PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + publisher + ", " +
                                                        message[1]));
                    thread.Start();*/
                }

                Console.Out.WriteLine("Resending pub to " + requestingSite);
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

        //************************************************
        //
        //          Subscription Methods
        //
        //************************************************

        public void DeliverSubscription(string subscriber, string topic, string siteName)
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
                Console.Out.WriteLine("Deliver sub");

                lock (SiblingBrokers)
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
                                        broker.InformOfSubscription(subscriber, topic, siteName);
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
                        ProcessSubscription(subscriber, topic, siteName);
                        // test only
                        /*if (ProcessName.Equals("broker3"))
                            Process.GetCurrentProcess().Kill();*/
                    }
                }
            }
        }

        public void InformOfSubscription(string subscriber, string topic, string siteName)
        {
            // creates a subscriber lock if needed
            object procLock;
            if (!ProcessLocks.TryGetValue(subscriber, out procLock))
            {
                ProcessLocks[subscriber] = new object();
            }

            lock (ProcessLocks[subscriber])
            {
                Console.Out.WriteLine("Inform");
                // the subscription is already registered
                SubscriptionSet set;
                if (RoutingTable.TryGetValue(topic, out set) && set.Processes.ContainsKey(subscriber))
                {
                    Console.Out.WriteLine("ALREADY SUBBED - " + subscriber);
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
                                    broker.InformOfSubscription(subscriber, topic, siteName);
                                } catch (RemotingException)
                                {
                                } catch (SocketException)
                                {
                                }
                            });
                        thread.Start();
                    }
                }

                // and deliver it
                ProcessSubscription(subscriber, topic, siteName);
            }
        }

        private void ProcessSubscription(string subscriber, string topic, string siteName)
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
                // get or create the subscription for this topic
                SubscriptionSet subscriptionSet;
                if (!RoutingTable.TryGetValue(topic, out subscriptionSet))
                {
                    subscriptionSet = new SubscriptionSet(topic);
                }

                // this prevents the same message from being processed twice
                if (subscriptionSet.IsSubscribed(subscriber))
                {
                    Console.Out.WriteLine("Already subscribed. Discarding");
                    return;
                }

                Console.Out.WriteLine("Receiving subscription on topic " + topic + " from  " + subscriber);
                subscriptionSet.AddSubscriber(subscriber, siteName);
                RoutingTable[topic] = subscriptionSet;

                foreach (KeyValuePair<string, List<IBroker>> child in Children)
                {
                    List<IBroker> childBrokers = child.Value;

                    // we don't send the SubscriptionSet to where it came from
                    if (!child.Key.Equals(siteName))
                    {
                       /* if (this.LoggingLevel == LoggingLevel.Full)
                        {
                           // PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + subscriber + ", " + topic);
                            Thread logThread =
                                new Thread(
                                    () => PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + subscriber + ", " + topic));
                            logThread.Start();
                        }*/

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
                                                childBroker.DeliverSubscription(subscriber, topic, SiteName);
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
                    /*if (this.LoggingLevel == LoggingLevel.Full)
                    {
                        //PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + subscriber + ", " + topic);
                        Thread logThread =
                            new Thread(
                                () => PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + subscriber + ", " + topic));
                        logThread.Start();
                    }*/

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
                                        parent.DeliverSubscription(subscriber, topic, SiteName);
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
        }

        //************************************************
        //
        //          Unsubscription Methods
        //
        //************************************************

        public void DeliverUnsubscription(string subscriber, string topic, string siteName)
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
                                    broker.InformOfUnsubscription(subscriber, topic, siteName);
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
                        ProcessUnsubscription(subscriber, topic, siteName);
                    }
                }
            }
        }

        public void ProcessUnsubscription(string subscriber, string topic, string siteName)
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
                    Console.Out.WriteLine("Already unsubscribed. Discarding");
                    return;
                }

                Console.Out.WriteLine("Receiving unsubscription on topic " + topic + " from  " + subscriber);

                subscriptionSet.RemoveSubscriber(subscriber);
                RoutingTable[topic] = subscriptionSet;

                foreach (KeyValuePair<string, List<IBroker>> child in Children)
                {
                    List<IBroker> childBrokers = child.Value;

                    // we don't send the SubscriptionSet to where it came from
                    if (!child.Key.Equals(siteName))
                    {
                       /* if (this.LoggingLevel == LoggingLevel.Full)
                        {
                            //PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + subscriber + ", " + topic);
                            Thread logThread =
                                new Thread(
                                    () =>
                                        PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + subscriber + ", " + topic));
                            logThread.Start();
                        }*/

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
                                        childBroker.DeliverUnsubscription(subscriber, topic, SiteName);
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
                  /*  if (this.LoggingLevel == LoggingLevel.Full)
                    {
                        //PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + subscriber + ", " + topic);
                        Thread logThread =
                            new Thread(
                                () => PuppetMaster.DeliverLog("BroEvent " + ProcessName + ", " + subscriber + ", " + topic));
                        logThread.Start();
                    }*/
                    
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
                                    parent.DeliverUnsubscription(subscriber, topic, SiteName);
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
        }

        public void InformOfUnsubscription(string subscriber, string topic, string siteName)
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
                            broker.InformOfUnsubscription(subscriber, topic, siteName);
                        } catch (RemotingException)
                        {
                        } catch (SocketException)
                        {
                        }
                    });
                    thread.Start();
                }

                // and deliver it
                ProcessUnsubscription(subscriber, topic, siteName);
            }
        }

        public void AddSiblingBroker(string siblingUrl)
        {
            Console.Out.WriteLine("Received sibling " + siblingUrl);
            IBroker sibling = (IBroker) Activator.GetObject(typeof (IBroker), siblingUrl);

            try
            {
                sibling.Ping();
            } catch (Exception)
            {
                Console.Out.WriteLine("\tERROR: The sibling broker " + siblingUrl + " is down.");
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

                    if (UtilityFunctions.StringEquals(subject, baseTopic) && RoutingTable.TryGetValue(subject, out subs))
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