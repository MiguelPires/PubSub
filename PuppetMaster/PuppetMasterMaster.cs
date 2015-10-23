using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Net.Sockets;
using System.Security.Policy;
using CommonTypes;

namespace PuppetMaster
{
    public class PuppetMasterMaster : MarshalByRefObject, IPuppetMasterMaster
    {
        // GUI
        public Form1 Form { get; set; }        
        private delegate void DelegateDeliverMessage(string message);

        // this site's name
        public string Site {get; private set; }

        // maps a site to it's puppetMaster
        public IDictionary<string, IPuppetMaster> Slaves { get; set; }

        // maps a process to it's site
        public IDictionary<string, string> Processes;


        public PuppetMasterMaster(string siteName)
        {
            Site = siteName;
            Slaves = new Dictionary<string, IPuppetMaster>();
            
            StreamReader reader = File.OpenText(AppDomain.CurrentDomain.BaseDirectory + "/master.config");

            string line;
            while ((line = reader.ReadLine()) != null)
            {
                ParseConfig(line);
            }
        }

        public void DeliverCommand(string message)
        {
            Form.Invoke(new DelegateDeliverMessage(Form.DeliverMessage), message);
        }

        public void SendCommand(string command)
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

        /// <summary>
        /// Parses a user's command 
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
                    process = "all";    // all processes
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
                    throw new CommandParsingException("Unknown command: "+command);
            }
            return process;
        }

        /// <summary>
        /// Parses a line of config file
        /// </summary>
        /// <param name="line"></param>
        private void ParseConfig(string line)
        {
            string[] tokens = line.Split(' ');

            if (tokens[0].Equals("Process"))
            {
                string processUrl = tokens[7];
                string processType = tokens[3];
                string processName = tokens[1];
                string siteName = tokens[5];

                if (siteName.Equals(Site))
                {
                    DeliverLocalConfig(processName, processType, processUrl);
                    return;
                }

                try
                {
                    IPuppetMaster slave = Slaves[siteName];
                    slave.DeliverConfig(processName, processType, processUrl);

                }
                catch (KeyNotFoundException)
                {
                    Console.WriteLine("Config wasn't delivered to the site '"+siteName+"'");
                }

            }
            else if (tokens[0].Equals("Site") && !tokens[1].Equals(Site))
            {
                string siteParent = tokens[3];
                string siteName = tokens[1];

                ConnectToSite(siteName, siteParent);
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
                Console.WriteLine(@"The slave at "+name+@" already exists");
                return null;
            }

        }

        private void DeliverLocalConfig(string processName, string processType, string processUrl)
        {
            //throw new NotImplementedException();
        }
    }
}