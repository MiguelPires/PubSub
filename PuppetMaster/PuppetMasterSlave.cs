using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Net.Sockets;
using System.Runtime.Hosting;
using System.Runtime.Remoting;
using System.Security.Policy;
using System.Threading;
using CommonTypes;

namespace PuppetMaster
{
    public class PuppetMasterSlave : BasePuppet, IPuppetMasterSlave
    {
        // GUI
        public LoggingForm Form { get; set; }
        // the PuppetMasterMaster instance
        public IPuppetMasterMaster Master { get; private set; }

        public PuppetMasterSlave(string siteName) : base(siteName)
        {
            LocalProcesses = new Dictionary<string, IProcess>();
            InitializeLogWriter();
            InitializeCommandSender();
        }

        /// <summary>
        ///     Initializes a thread reads log messages from a buffer
        /// writes them in the GUI and also sends them to the PuppetMasterMaster
        /// </summary>
        private void InitializeLogWriter()
        {
            new Thread(() =>
            {
                Monitor.Enter(this.LogQueue);
                while (true)
                {
                    string logMessage;
                    if (this.LogQueue.TryDequeue(out logMessage))
                    {
                        this.eventNumber++;
                        Form.Invoke(LogDelegate, logMessage + ", " + this.eventNumber);
                        Master.DeliverLog(logMessage);
                    } else
                    {
                        Monitor.Wait(this.LogQueue);
                    }
                }
            }).Start();
        }

        /// <summary>
        ///     Initializes a thread that reads commands from a buffer
        /// and sends them to the processes
        /// </summary>
        private void InitializeCommandSender()
        {
            new Thread(() =>
            {
                Monitor.Enter(CommandQueue);
                while (true)
                {
                    string[] command;
                    if (CommandQueue.TryDequeue(out command))
                    {
                        string processName = command[0];
                        if (processName.Equals("all"))
                        {
                            foreach (var proc in LocalProcesses.Values)
                            {
                                // the process doesn't need to receive it's own name (first index in commandArgs)
                                try
                                {
                                    proc.DeliverCommand(new string[1] { command[1] });
                                }
                                catch (RemotingException)
                                {
                                }
                                catch (SocketException)
                                {
                                }
                            }
                        }
                        else
                        {
                            // it doesn't need to receive it's own name here as well..
                            string[] processArgs = new string[command.Length - 1];
                            Array.Copy(command, 1, processArgs, 0, command.Length - 1);
                            IProcess process = LocalProcesses[processName];

                            process.DeliverCommand(processArgs);
                        }
                    }
                    else
                        Monitor.Wait(CommandQueue);
                }
            }).Start();
        }

        void IPuppetMaster.DeliverLog(string message)
        {
            if (!string.IsNullOrEmpty(message))
            {
                Monitor.Enter(LogQueue);
                LogQueue.Enqueue(message);
                Monitor.Pulse(LogQueue);
                Monitor.Exit(LogQueue);
            }
            else
                Console.WriteLine(@"Problem - SendLog: The log line shouldn't be empty");
        }

        /// <summary>
        ///     Launches a process at this site
        /// </summary>
        /// <param name="processName"> The process name </param>
        /// <param name="processType"> The process type </param>
        /// <param name="processUrl"> The process Url </param>
        void IPuppetMasterSlave.LaunchProcess(string processName, string processType, string processUrl)
        {
            base.LaunchProcess(processName, processType, processUrl);
        }

        void IPuppetMasterSlave.DeliverSetting(string settingType, string settingValue)
        {
            switch (settingType)
            {
                case "RoutingPolicy":
                    if (settingValue.Equals("flooding"))
                        this.RoutingPolicy = RoutingPolicy.Flood;
                    else if (settingValue.Equals("filter"))
                        this.RoutingPolicy = RoutingPolicy.Filter;
                    else
                    {
                        Console.Out.WriteLine("Unknown setting for Routing Policy");
                        return;
                    }

                    break;

                case "LoggingLevel":
                    if (settingValue.Equals("full"))
                        this.LoggingLevel = LoggingLevel.Full;
                    else if (settingValue.Equals("light"))
                        this.LoggingLevel = LoggingLevel.Light;
                    else
                    {
                        Console.Out.WriteLine("Unknown setting for Logging Level");
                        return;
                    }
                    break;

                case "OrderingGuarantee":
                    if (settingValue.Equals("NO"))
                        this.OrderingGuarantee = OrderingGuarantee.No;
                    else if (settingValue.Equals("FIFO"))
                        this.OrderingGuarantee = OrderingGuarantee.Fifo;
                    else if (settingValue.Equals("TOTAL"))
                        this.OrderingGuarantee = OrderingGuarantee.Total;
                    else
                    {
                        Console.Out.WriteLine("Unknown setting for Ordering Guarantee");
                        return;
                    }
                    break;
            }
            Console.Out.WriteLine(settingType+": "  + settingValue);
        }

        void IPuppetMasterSlave.DeliverCommand(string[] commandArgs)
        {
            Monitor.Enter(CommandQueue);
            CommandQueue.Enqueue(commandArgs);
            Monitor.Pulse(CommandQueue);
            Monitor.Exit(CommandQueue);
        }

        /// <summary>
        ///     Connects this PuppetMasterSlave to the PuppetMasterMaster
        /// </summary>
        /// <param name="siteParent"></param>
        /// <param name="masterSite"></param>
        void IPuppetMasterSlave.RegisterWithMaster(string siteParent, string masterSite)
        {
            ParentSite = siteParent;
            string url = "tcp://localhost:" + UtilityFunctions.GetPort(masterSite) + "/" + masterSite;
            Master = (IPuppetMasterMaster)Activator.GetObject(typeof(IPuppetMasterMaster), url);
        }


        /// <summary>
        /// Returns every Broker at this site - user by brokers to connect to the parent site's brokers
        /// </summary>
        /// <returns></returns>
        public new List<string> GetBrokers()
        {
            return base.GetBrokers();
        }

        public RoutingPolicy GetRoutingPolicy()
        {
            return RoutingPolicy;
        }

        public LoggingLevel GetLoggingLevel()
        {
            return LoggingLevel;
        }

        public OrderingGuarantee GetOrderingGuarantee()
        {
            return OrderingGuarantee;
        }

        /// <summary>
        ///     This method is just here for testing purposes.
        ///     If it fails then there is a connection problem
        /// </summary>
        public void Ping()
        {
        }

        public override string ToString()
        {
            return "PuppetMasterSlave";
        }
    }
}