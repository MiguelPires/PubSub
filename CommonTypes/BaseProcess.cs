using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Messaging;

namespace CommonTypes
{
    public abstract class BaseProcess : MarshalByRefObject, IProcess
    {
        // this process's name
        public string ProcessName { get; private set; }
        // this broker's url
        public string Url { get; }
        // this site's name
        public string SiteName { get; private set; }
        // 
        public IPuppetMaster PuppetMaster { get; private set; }
        // system status (frozen, unfrozen)
        public Status Status { get; protected set; }
        // a queue of actions saved when process state is frozen (events that are yet to be processed)
        public ConcurrentQueue<string[]> CommandBacklog { get; set; } = new ConcurrentQueue<string[]>();
        // a list with messages received in the frozen state
        public List<string[]> FrozenMessages { get; set; } = new List<string[]>();
        // the logging setting
        public LoggingLevel LoggingLevel;
        // the ordering setting
        public OrderingGuarantee OrderingGuarantee;
        // the routing setting
        public RoutingPolicy RoutingPolicy;
        // this class' random instance. Since the default seed is time dependent we don«t
        // want to instantiate every time we send a message
        protected readonly Random Random = new Random();

        protected BaseProcess(string processName, string processUrl, string puppetMasterUrl, string siteName)
        {
            ProcessName = processName;
            Url = processUrl;
            SiteName = siteName;

            // connects to this site's puppetMaster
            ConnectToPuppetMaster(puppetMasterUrl);

            this.LoggingLevel = PuppetMaster.GetLoggingLevel();
            this.OrderingGuarantee = PuppetMaster.GetOrderingGuarantee();
            this.RoutingPolicy = PuppetMaster.GetRoutingPolicy();
        }
        /// <summary>
        /// Delivers a command to a process
        /// </summary>
        /// <param name="command"> The command to pass to the process</param>
        public virtual bool DeliverCommand(string[] command)
        {
            if (Status == Status.Frozen)
            {
                //saving command
                switch (command[0])
                {
                    case "Freeze":
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.BackgroundColor = ConsoleColor.Black;
                        Console.Out.WriteLine("Already frozen!");
                        Console.ResetColor();
                        break;
                    default:
                        CommandBacklog.Enqueue(command);
                        break;
                }
            }
            else
            {
                switch (command[0])
                {
                    case "Status":
                        Console.Out.WriteLine("**** Status ********\t\n");
                        break;

                    case "Crash":
                        Console.WriteLine("Crashing");
                        Process.GetCurrentProcess().Kill();
                        break;

                    case "Freeze":
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.BackgroundColor = ConsoleColor.Black;
                        Console.Out.WriteLine("Freezing");
                        Console.ResetColor();
                        Status = Status.Frozen;
                        break;

                    case "Unfreeze":
                        Console.Out.WriteLine("The process isn't frozen");
                        Status = Status.Unfrozen;
                        break;

                    default:
                        Console.Out.WriteLine("Command: " + command[0] + " doesn't exist!");
                        return false;
                }
            }
            return true;
        }

        public void Ping()
        {
            // used for testing a connection
            ;
        }

        void IProcess.DeliverSetting(string settingType, string settingValue)
        {
            switch (settingType)
            {
                case "RoutingPolicy":
                    if (settingValue.Equals("Flood"))
                        this.RoutingPolicy = RoutingPolicy.Flood;
                    else if (settingValue.Equals("Filter"))
                        this.RoutingPolicy = RoutingPolicy.Filter;
                    else
                    {
                        Console.Out.WriteLine("Unknown setting for Routing Policy");
                        return;
                    }

                    break;

                case "LoggingLevel":
                    if (settingValue.Equals("Full"))
                        this.LoggingLevel = LoggingLevel.Full;
                    else if (settingValue.Equals("Light"))
                        this.LoggingLevel = LoggingLevel.Light;
                    else
                    {
                        Console.Out.WriteLine("Unknown setting for Logging Level");
                        return;
                    }
                    break;

                case "OrderingGuarantee":
                    if (settingValue.Equals("No"))
                        this.OrderingGuarantee = OrderingGuarantee.No;
                    else if (settingValue.Equals("Fifo"))
                        this.OrderingGuarantee = OrderingGuarantee.Fifo;
                    else if (settingValue.Equals("Total"))
                        this.OrderingGuarantee = OrderingGuarantee.Total;
                    else
                    {
                        Console.Out.WriteLine("Unknown setting for Ordering Guarantee");
                        return;
                    }
                    break;
            }
            Console.Out.WriteLine(settingType + " set to " + settingValue);
        }

        /// <summary>
        ///     Returns the list of brokers running at a given site
        /// </summary>
        /// <param name="puppetMasterUrl"></param>
        /// <returns></returns>
        public List<string> GetBrokers(string puppetMasterUrl)
        {
            UtilityFunctions.ConnectFunction<List<string>> fun = (string url) =>
            {
                List<string> brokerUrls = null;

                // connects to the specified site's puppetMaster
                IPuppetMasterSlave puppetMasterSlave =
                    (IPuppetMasterSlave)Activator.GetObject(typeof(IPuppetMasterSlave), url);
                brokerUrls = puppetMasterSlave.GetBrokers();

                return brokerUrls;
            };

            List<string> brokersUrlsResult = UtilityFunctions.TryConnection<List<string>>(fun, puppetMasterUrl);
            return brokersUrlsResult;
        }
        
        private void ConnectToPuppetMaster(string puppetMasterUrl)
        {
            // connects to the specified site's puppetMaster
            UtilityFunctions.ConnectFunction<IPuppetMaster> fun = (string url) =>
            {
                IPuppetMaster puppet = (IPuppetMaster)Activator.GetObject(typeof(IPuppetMaster), url);
                puppet.Ping();

                return puppet;
            };

            IPuppetMaster puppetMaster = UtilityFunctions.TryConnection<IPuppetMaster>(fun, puppetMasterUrl);
            try
            {
                PuppetMaster = (IPuppetMasterSlave) puppetMaster;
            }
            catch (Exception)
            {
                    PuppetMaster = (IPuppetMasterMaster)puppetMaster; 
            }
        }

        public override object InitializeLifetimeService()
        {
            return null;
        }
    }
}