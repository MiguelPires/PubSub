using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using CommonTypes;

namespace PuppetMaster
{
    public abstract class BasePuppet : MarshalByRefObject
    {
        // this site's name
        public string SiteName { get; protected set; }
        // this site's parent 
        public string ParentSite { get; protected set; }
        // this site's url
        public string Url { get; protected set; }
        // maps a process name to the process instance
        public IDictionary<string, IProcess> LocalProcesses { get; protected set; }
        // maps a process name to a process type - necessary to find the brokers
        public IDictionary<string, ProcessType> LocalProcessesTypes { get; protected set; }
        // maps a process name to it's url
        public IDictionary<string, string> LocalProcessesUrls { get; protected set; }
        // the logging setting
        public LoggingLevel LoggingLevel = LoggingLevel.Light;
        // the ordering setting
        public OrderingGuarantee OrderingGuarantee = OrderingGuarantee.Fifo;
        // the routing setting
        public RoutingPolicy RoutingPolicy = RoutingPolicy.Flood;
        // for ordering the log entries
        protected int eventNumber;

        protected BasePuppet(string siteName)
        {
            SiteName = siteName;
            Url = UtilityFunctions.GetUrl(SiteName);
            LocalProcesses = new ConcurrentDictionary<string, IProcess>();
            LocalProcessesTypes = new ConcurrentDictionary<string, ProcessType>();
            LocalProcessesUrls = new ConcurrentDictionary<string, string>();
            eventNumber = 0;
        }

        /// <summary>
        ///     Returns every Broker at this site - user by brokers to connect to the parent site's brokers
        /// </summary>
        /// <returns></returns>
        protected List<string> GetBrokers()
        {
            List<string> brokerUrls = new List<string>();

            foreach (string procName in LocalProcesses.Keys)
            {
                if (LocalProcessesTypes[procName].Equals(ProcessType.Broker))
                    brokerUrls.Add(LocalProcessesUrls[procName]);
            }

            return brokerUrls;
        }

        /// <summary>
        ///     Launches a process at this site
        /// </summary>
        /// <param name="processName"> The process name </param>
        /// <param name="processType"> The process type </param>
        /// <param name="processUrl"> The process Url </param>
        protected void LaunchProcess(string processName, string processType, string processUrl)
        {
            Console.WriteLine("Launching " + processName);
           
            ProcessStartInfo startInfo = new ProcessStartInfo
            {
                UseShellExecute = true,
                WorkingDirectory = AppDomain.CurrentDomain.BaseDirectory,
                Arguments = processName + " " + processUrl + " " + Url+" " + SiteName
            };

            switch (processType)
            {
                case "broker":
                    startInfo.Arguments += " " + ParentSite;
                    startInfo.FileName = "Broker";
                    Process.Start(startInfo);
                    LocalProcesses[processName] = (IBroker) Activator.GetObject(typeof (IBroker), processUrl);
                    LocalProcessesTypes[processName] = ProcessType.Broker;
                    LocalProcessesUrls[processName] = processUrl;
                    return;

                case "publisher":
                    startInfo.FileName = "Publisher";
                    LocalProcessesTypes[processName] = ProcessType.Publisher;
                    break;

                case "subscriber":
                    startInfo.FileName = "Subscriber";
                    LocalProcessesTypes[processName] = ProcessType.Subscriber;
                    break;

                default:
                    Console.Out.WriteLine(processName + ": Process type " + processType + " doesn't exist");
                    return;
            }

            Process.Start(startInfo);
            LocalProcesses[processName] = (IProcess) Activator.GetObject(typeof (IProcess), processUrl);
            LocalProcessesUrls[processName] = processUrl;

        }

        public override object InitializeLifetimeService()
        {
            return null;
        }
    }

    public enum ProcessType
    {
        Broker,
        Publisher,
        Subscriber
    }
}