#region

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using CommonTypes;

#endregion

namespace PuppetMaster
{
    public class PuppetMasterMaster : BasePuppet, IPuppetMasterMaster
    {
        // GUI
        public InteractionForm Form { get; set; }
        // maps a site to it's puppetMaster
        public IDictionary<string, IPuppetMasterSlave> Slaves { get; set; }
        // every broker in the system - only used at startup
        public List<string[]> BrokersStartup { get; }
        // maps a process to it's site name
        public IDictionary<string, string> SiteProcesses;
        //
        public Delegate LogDelegate { get; set; }

        public PuppetMasterMaster(string siteName) : base(siteName)
        {
            Slaves = new Dictionary<string, IPuppetMasterSlave>();
            this.SiteProcesses = new Dictionary<string, string>();
            BrokersStartup = new List<string[]>();

            StreamReader reader = File.OpenText(AppDomain.CurrentDomain.BaseDirectory + "/master.config");

            string line;
            while ((line = reader.ReadLine()) != null)
            {
                ParseConfig(line);
            }

            reader.Close();

            // inform every broker of it's siblings
            foreach (string[] brokerArgs in BrokersStartup)
            {
                IBroker broker = (IBroker) Activator.GetObject(typeof (IBroker), brokerArgs[1]);
                foreach (string[] siblingArgs in BrokersStartup)
                {
                    if (brokerArgs[0] == siblingArgs[0] && brokerArgs[1] != siblingArgs[1])
                    {
                        UtilityFunctions.ConnectFunction<Object> fun = (string _) =>
                        {
                            broker.AddSiblingBroker(siblingArgs[1]);
                            return null;
                        };

                        Thread thread = new Thread(() => UtilityFunctions.TryConnection(fun, ""));
                        thread.Start();
                    }
                }
            }
        }


        void IPuppetMaster.DeliverLog(string message)
        {
            this.eventNumber++;
            Form.BeginInvoke(LogDelegate, message + ", " + this.eventNumber);
        }

        void IPuppetMasterMaster.SendCommand(string command)
        {
            string[] puppetArgs;
            string processName;
            try
            {
                ParseCommand(command, out puppetArgs);
                processName = puppetArgs[0];
            }
            catch (CommandParsingException e)
            {
                Console.WriteLine(e.Message);
                return;
            }

            // the status command 
            if (processName.Equals("all"))
            {
                // deliver command to every remote PuppetMaster
                foreach (IPuppetMasterSlave slave in Slaves.Values)
                {
                    Thread thread = new Thread(() => slave.DeliverCommand(puppetArgs));
                    thread.Start();
                }

                // deliver command to every local process
                foreach (IProcess proc in LocalProcesses.Values)
                {
                    Thread thread = new Thread(() => proc.DeliverCommand(new[] {puppetArgs[1]}));
                    thread.Start();
                }
            }
            else
            {
                // find the process's site
                string site = null;
                try
                {
                    site = this.SiteProcesses[processName];
                }
                catch (KeyNotFoundException)
                {
                    Console.Out.WriteLine("WARNING: The process " + processName + " couldn't be found.");
                    return;
                }

                if (site.Equals(SiteName))
                {
                    // the process doesn't need to receive it's own name (first index in puppetArgs)
                    string[] processArgs = new string[puppetArgs.Length - 1];
                    Array.Copy(puppetArgs, 1, processArgs, 0, puppetArgs.Length - 1);
                    IProcess process = LocalProcesses[processName];

                    /*     Thread thread = new Thread(() => process.DeliverCommand(processArgs));
                    thread.Start();*/
                    try
                    {
                        process.DeliverCommand(processArgs);
                    }
                    catch (Exception)
                    {
                        if (!processArgs[0].Equals("Crash"))
                            throw;
                    }
                }
                else
                {
                    IPuppetMasterSlave puppetMaster = Slaves[site];

                    /*   Thread thread = new Thread(() => puppetMaster.DeliverCommand(puppetArgs));
                    thread.Start();*/
                    try
                    {
                        puppetMaster.DeliverCommand(puppetArgs);
                    }
                    catch (Exception)
                    {
                        if (!puppetArgs[0].Equals("Crash"))
                            throw;
                    }
                }
            }
        }

        /// <summary>
        ///     Returns every Broker at this site - used by PuppetMasters
        /// </summary>
        /// <returns></returns>
        public new List<string> GetBrokers()
        {
            return base.GetBrokers();
        }

        public void Ping()
        {
        }

        public RoutingPolicy GetRoutingPolicy()
        {
            return this.RoutingPolicy;
        }

        public LoggingLevel GetLoggingLevel()
        {
            return this.LoggingLevel;
        }

        public OrderingGuarantee GetOrderingGuarantee()
        {
            return this.OrderingGuarantee;
        }

        public override string ToString()
        {
            return "PuppetMasterMaster";
        }

        /// <summary>
        ///     Parses a user's command
        /// </summary>
        /// <param name="command">The full command line</param>
        /// <param name="args">An output parameter with the arguments to be passed to the process, if any</param>
        private static void ParseCommand(string command, out string[] args)
        {
            string[] tokens = command.Split(' ');
            args = new string[5];
            switch (tokens[0])
            {
                case "Subscriber":
                    args[0] = tokens[1]; // process name
                    args[1] = tokens[2]; // Subscribe/Unsubsribe
                    args[2] = tokens[3]; // topic
                    break;

                case "Publisher":
                    if (tokens.Length != 8)
                    {
                        throw new CommandParsingException("Unknown command: " + command);
                    }
                    args[0] = tokens[1]; // process name
                    args[1] = "Publish";
                    args[2] = tokens[3]; // number of events
                    args[3] = tokens[5]; // topic name
                    args[4] = tokens[7]; // time interval (ms)
                    break;

                case "Status":
                    args[0] = "all"; // process name
                    args[1] = "Status";
                    break;

                case "Crash":
                    args[0] = tokens[1]; // process name
                    args[1] = "Crash";
                    break;

                case "Freeze":
                    args[0] = tokens[1]; // process name
                    args[1] = "Freeze";
                    break;

                case "Unfreeze":
                    args[0] = tokens[1]; // process name
                    args[1] = "Unfreeze";
                    break;

                default:
                    throw new CommandParsingException("WARNING: Unknown command: " + command);
            }
        }

        /// <summary>
        ///     Parses a line of config file
        /// </summary>
        /// <param name="line"> A line of the config file </param>
        private void ParseConfig(string line)
        {
            string[] tokens = line.Split(null);
            for (int i = 0; i < tokens.Length; ++i) 
            {
                tokens[i] = tokens[i].Trim();
            }

            if (tokens[0].Equals("Process"))
            {
                ParseProcess(tokens);
            }
            else if (tokens[0].Equals("Site"))
            {
                ParseSite(tokens);
            }
            else if (tokens[0].Equals("Ordering"))
            {
                ParseOrdering(tokens);
            }
            else if (tokens[0].Equals("RoutingPolicy"))
            {
                ParseRouting(tokens);
            }
            else if (tokens[0].Equals("LoggingLevel"))
            {
                ParseLogging(tokens);
            }
        }

        //  Parsing functions
        //  Each of the following functions parses a specific type of entry in the config file
        //
        private void ParseProcess(string[] tokens)
        {
            string processUrl = tokens[7];
            string processType = tokens[3];
            string processName = tokens[1];
            string siteName = tokens[5];

            this.SiteProcesses[processName] = siteName;

            // we need to keep track of all brokers to inform them of the other brokers at their site
            // this can only be done in the end of the parsing 
            if (processType == "broker")
            {
                BrokersStartup.Add(new[] {siteName, processUrl});
            }

            // if the site is this site
            if (siteName.Equals(SiteName))
            {
                LaunchProcess(processName, processType, processUrl);
                return;
            }

            try
            {
                IPuppetMasterSlave slave = Slaves[siteName];
                slave.LaunchProcess(processName, processType, processUrl);
            }
            catch (KeyNotFoundException)
            {
                Console.WriteLine("WARNING: Config wasn't delivered to the site '" + siteName + "'");
            }
        }

        private void ParseSite(string[] tokens)
        {
            string siteParent = tokens[3];
            string siteName = tokens[1];

            if (tokens[1].Equals(SiteName))
                ParentSite = siteParent;
            else
            {
                ConnectToSite(siteName, siteParent);
            }
        }

        private void ParseOrdering(string[] tokens)
        {
            switch (tokens[1])
            {
                case "NO":
                    this.OrderingGuarantee = OrderingGuarantee.No;
                    break;

                case "FIFO":
                    this.OrderingGuarantee = OrderingGuarantee.Fifo;
                    break;

                case "TOTAL":
                    this.OrderingGuarantee = OrderingGuarantee.Total;
                    break;

                default:
                    Console.Out.WriteLine("WARNING: " + tokens[1] + " isn't a valid ordering garantee.");
                    break;
            }
        }

        private void ParseRouting(string[] tokens)
        {
            switch (tokens[1])
            {
                case "flooding":
                    this.RoutingPolicy = RoutingPolicy.Flood;
                    break;

                case "filter":
                case "filtering":
                    this.RoutingPolicy = RoutingPolicy.Filter;
                    break;

                default:
                    Console.Out.WriteLine("WARNING: " + tokens[1] + " isn't a valid routing policy.");
                    break;
            }
        }

        private void ParseLogging(string[] tokens)
        {
            switch (tokens[1])
            {
                case "full":
                    this.LoggingLevel = LoggingLevel.Full;
                    break;

                case "light":
                    this.LoggingLevel = LoggingLevel.Light;
                    break;

                default:
                    Console.Out.WriteLine("WARNING: " + tokens[1] + " isn't a valid logging level.");
                    break;
            }
        }

        /// <summary>
        ///     Connects to the PuppetMasterSlave at the specified site
        /// </summary>
        /// <param name="name"> The machine's name </param>
        /// <param name="siteParent"> The site's parent in the tree </param>
        /// <returns> A puppetMaster instance or null if the site is down </returns>
        private IPuppetMasterSlave ConnectToSite(string name, string siteParent)
        {
            string siteUrl = "tcp://localhost:" + UtilityFunctions.GetPort(name) + "/" + name;

            try
            {
                Console.WriteLine("Connecting to " + siteUrl);

                UtilityFunctions.ConnectFunction<IPuppetMasterSlave> fun = (string urlToConnect) =>
                {
                    IPuppetMasterSlave puppetMasterSlave =
                        (IPuppetMasterSlave) Activator.GetObject(typeof (IPuppetMasterSlave), urlToConnect);
                    puppetMasterSlave.Ping();
                    puppetMasterSlave.RegisterWithMaster(siteParent, SiteName);
                    puppetMasterSlave.DeliverSetting("RoutingPolicy",
                        this.RoutingPolicy == RoutingPolicy.Filter ? "filter" : "flooding");
                    puppetMasterSlave.DeliverSetting("LoggingLevel", this.LoggingLevel == LoggingLevel.Full ? "full" : "light");

                    switch (this.OrderingGuarantee)
                    {
                        case OrderingGuarantee.Fifo:
                            puppetMasterSlave.DeliverSetting("OrderingGuarantee", "FIFO");
                            break;
                        case OrderingGuarantee.No:
                            puppetMasterSlave.DeliverSetting("OrderingGuarantee", "NO");
                            break;
                        case OrderingGuarantee.Total:
                            puppetMasterSlave.DeliverSetting("OrderingGuarantee", "TOTAL");
                            break;
                    }
                    return puppetMasterSlave;
                };

                var slave = UtilityFunctions.TryConnection(fun, siteUrl);
                Slaves.Add(name, slave);

                return slave;
            }
            catch (SocketException)
            {
                Console.WriteLine(@"Couldn't connect to " + siteUrl);
                return null;
            }
            catch (ArgumentException)
            {
                Console.WriteLine(@"The slave at " + name + @" already exists");
                return null;
            }
        }
    }
}