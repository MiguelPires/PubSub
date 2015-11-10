using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.Remoting;

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
        public ConcurrentQueue<string[]> CommandBacklog { get; set; }
        // a list with messages received in the frozen state
        public List<string[]> FrozenMessages { get; set; }
        // the logging setting
        public LoggingLevel LoggingLevel = LoggingLevel.Light;
        // the ordering setting
        public OrderingGuarantee OrderingGuarantee = OrderingGuarantee.Fifo;
        // the routing setting
        public RoutingPolicy RoutingPolicy = RoutingPolicy.Flood;

        protected BaseProcess(string processName, string processUrl, string puppetMasterUrl, string siteName)
        {
            ProcessName = processName;
            Url = processUrl;
            CommandBacklog = new ConcurrentQueue<string[]>();
            FrozenMessages = new List<string[]>();
            SiteName = siteName;

            // connects to this site's puppetMaster
            ConnectToPuppetMaster(puppetMasterUrl);

            this.LoggingLevel = PuppetMaster.GetLoggingLevel();
            this.OrderingGuarantee = PuppetMaster.GetOrderingGuarantee();
            this.RoutingPolicy = PuppetMaster.GetRoutingPolicy();
        }

        public virtual void DeliverCommand(string[] command)
        {
            if (Status == Status.Frozen)
            {
                //saving command
                switch (command[0])
                {
                    case "Freeze":
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.BackgroundColor = ConsoleColor.Black;
                        Console.Out.WriteLine("I'm already frozen!");
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
                        Console.Out.WriteLine("Status information:\r\nSubscriptions, etc...");
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
                        break;
                }
            }
        }

        void IProcess.SendLog(string log)
        {
            throw new NotImplementedException();
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
            // TODO: Refactor this 

            // connects to the specified site's puppetMaster
            IPuppetMasterSlave puppetMasterSlave =
                (IPuppetMasterSlave) Activator.GetObject(typeof (IPuppetMasterSlave), puppetMasterUrl);

            List<string> brokerUrls;
            try
            {
                // obtains the broker urls at that site - these urls are probably going to be stored for reconnection later
                brokerUrls = puppetMasterSlave.GetBrokers();
            }
            catch (RemotingException)
            {
                IPuppetMasterMaster newPuppetMaster =
                    (IPuppetMasterMaster) Activator.GetObject(typeof (IPuppetMasterMaster), puppetMasterUrl);
                brokerUrls = newPuppetMaster.GetBrokers();
            }
            return brokerUrls;
        }

        private void ConnectToPuppetMaster(string puppetMasterUrl)
        {
            // connects to the specified site's puppetMaster
            PuppetMaster = (IPuppetMasterSlave) Activator.GetObject(typeof (IPuppetMasterSlave), puppetMasterUrl);

            try
            {
                PuppetMaster.Ping();
            }
            catch (Exception)
            {
                PuppetMaster =
                    (IPuppetMasterMaster) Activator.GetObject(typeof (IPuppetMasterMaster), puppetMasterUrl);
                PuppetMaster.Ping();
            }
        }

        public override object InitializeLifetimeService()
        {
            return null;
        }
    }
}