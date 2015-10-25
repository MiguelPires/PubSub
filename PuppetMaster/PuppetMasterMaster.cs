using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using CommonTypes;

namespace PuppetMaster
{
    public class PuppetMasterMaster : MarshalByRefObject, IPuppetMasterMaster, IProcessMaster
    {
        // GUI
        public Form1 Form { get; set; }
        // this site's name
        public string Site { get; }
        // this site's parent
        public string Parent { get; private set; }
        // maps a site to it's puppetMaster
        public IDictionary<string, IPuppetMaster> Slaves { get; set; }
        // the list of processes that run in this site
        public List<IProcess> LocalProcesses { get; set; }
        // the logging setting
        public LoggingLevel LoggingLevel = LoggingLevel.Light;
        // the ordering setting
        public OrderingGuarantee OrderingGuarantee = OrderingGuarantee.FIFO;
        // maps a process to it's site name
        public IDictionary<string, string> Processes;
        // the routing setting
        public RoutingPolicy RoutingPolicy = RoutingPolicy.Flood;

        private delegate void DelegateDeliverMessage(string message);


        public PuppetMasterMaster(string siteName)
        {
            Site = siteName;
            Slaves = new Dictionary<string, IPuppetMaster>();
            LocalProcesses = new List<IProcess>();
            Processes = new Dictionary<string, string>();


            StreamReader reader = File.OpenText(AppDomain.CurrentDomain.BaseDirectory + "/master.config");

            string line;
            while ((line = reader.ReadLine()) != null)
            {
                ParseConfig(line);
            }
        }

        void IProcessMaster.DeliverLogToPuppetMaster(string log)
        {
            throw new NotImplementedException();
        }

        void IPuppetMasterMaster.DeliverLog(string message)
        {
            Form.Invoke(new DelegateDeliverMessage(Form.DeliverMessage), message);
        }

        void IPuppetMasterMaster.SendCommand(string command)
        {
            string[] args;
            string process;

            try
            {
                process = ParseCommand(command, out args);
            }
            catch (CommandParsingException e)
            {
                Console.WriteLine(e.Message);
                return;
            }

            if (process.Equals("all"))
            {
                foreach (IPuppetMaster slave in Slaves.Values)
                {
                    slave.DeliverCommand(args);
                }
            }
            else
            {
                /* try
                {*/
                Slaves[process].DeliverCommand(args);
                //} catch()
            }
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
        /// <returns> The process that will receive the command</returns>
        private string ParseCommand(string command, out string[] args)
        {
            string[] tokens = command.Split(' ');
            string process;
            args = new string[4];
            switch (tokens[0])
            {
                case "Subscriber":
                    process = tokens[1]; // process name
                    args[0] = tokens[2]; // subscribe/unsubsribe
                    args[1] = tokens[3]; // topic
                    break;

                case "Publisher":
                    process = tokens[1]; // process name
                    args[0] = "Publish";
                    args[1] = tokens[3]; // number of events
                    args[2] = tokens[5]; // topic name
                    args[3] = tokens[7]; // time interval (ms)
                    break;

                case "Status":
                    process = "all"; // all processes
                    args[0] = "Status";
                    break;

                case "Crash":
                    process = tokens[1]; // process name
                    args[0] = "Crash";
                    break;

                case "Freeze":
                    process = tokens[1]; // process name
                    args[0] = "Freeze";
                    break;

                case "Unfreeze":
                    process = tokens[1]; // process name
                    args[0] = "Unfreeze";
                    break;

                // the wait command has a different purpose, but should also be parsed

                default:
                    throw new CommandParsingException("Unknown command: " + command);
            }
            return process;
        }

        /// <summary>
        ///     Parses a line of config file
        /// </summary>
        /// <param name="line"> A line of the config file </param>
        private void ParseConfig(string line)
        {
            string[] tokens = line.Split(' ');

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

        // Each of the following functions parses a specific type of entry in the config file
        private void ParseProcess(string[] tokens)
        {
            string processUrl = tokens[7];
            string processType = tokens[3];
            string processName = tokens[1];
            string siteName = tokens[5];

            // if the site is this site
            if (siteName.Equals(Site))
            {
                DeliverLocalConfig(processName, processType, processUrl);
                return;
            }

            try
            {
                IPuppetMaster slave = Slaves[siteName];
                slave.DeliverConfig(processName, processType, processUrl);
                Processes[processName] = siteName;
            }
            catch (KeyNotFoundException)
            {
                Console.WriteLine("Config wasn't delivered to the site '" + siteName + "'");
            }
        }

        private void ParseSite(string[] tokens)
        {
            string siteParent = tokens[3];
            string siteName = tokens[1];

            if (tokens[1].Equals(Site))
                Parent = siteParent;
            else
                ConnectToSite(siteName, siteParent);
        }

        private void ParseOrdering(string[] tokens)
        {
            switch (tokens[1])
            {
                case "NO":
                    this.OrderingGuarantee = OrderingGuarantee.No;
                    break;

                case "FIFO":
                    this.OrderingGuarantee = OrderingGuarantee.FIFO;
                    break;

                case "TOTAL":
                    this.OrderingGuarantee = OrderingGuarantee.Total;
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
                    this.RoutingPolicy = RoutingPolicy.Filter;
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
            }
        }

        /// <summary>
        ///     Connects to the puppetMaster at the specified site
        /// </summary>
        /// <param name="name"> The machine's name </param>
        /// <param name="siteParent"> The site's parent in the tree </param>
        /// <returns> A puppetMaster instance or null if the site is down </returns>
        private IPuppetMaster ConnectToSite(string name, string siteParent)
        {
            string siteUrl = "tcp://localhost:" + UtilityFunctions.GetPort(name) + "/" + name;

            try
            {
                Console.WriteLine("Connecting to " + siteUrl);

                IPuppetMaster slave = (IPuppetMaster) Activator.GetObject(typeof (IPuppetMaster), siteUrl);
                slave.Ping();
                slave.Register(siteParent, Site);

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

        private void DeliverLocalConfig(string processName, string processType, string processUrl)
        {
            Console.WriteLine("Running a " + processType + " at " + processUrl);

            switch (processType)
            {
                case "broker":
                    Broker broker = new Broker(processName, processUrl, this);
                    LocalProcesses.Add(broker);
                    break;

                case "publisher":
                    break;

                case "subscriber":
                    break;
            }
        }
    }
}