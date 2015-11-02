using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Remoting;
using System.Text;
using System.Threading.Tasks;

namespace CommonTypes
{
    abstract public class BaseProcess : MarshalByRefObject, IProcess
    {
        // this process's name
        public string ProcessName { get; private set; }
        // this broker's url
        public string Url { get; }
        // public string PuppetMasterUrl { get; }
        public IProcessMaster PuppetMaster { get; private set; }
        // system status (frozen, unfrozen)
        public Status Status { get; protected set; }

        //List of actions saved when process state is frozen
        public List<String[]> FrozenStateList { get; set; }

        protected BaseProcess(string processName, string processUrl, string puppetMasterUrl)
        {
            ProcessName = processName;
            Url = processUrl;
            // connects to this site's puppetMaster
            PuppetMaster = (IProcessMaster)Activator.GetObject(typeof(IProcessMaster), puppetMasterUrl);
            FrozenStateList = new List<string[]>();
        }

        /// <summary>
        /// Returns the list of brokers running at a given site
        /// </summary>
        /// <param name="puppetMasterUrl"></param>
        /// <returns></returns>
        public List<string> GetBrokers(string puppetMasterUrl)
        {
            //TODO: implementar uma clausula para os processos nao estoirarem

            // connects to the specified site's puppetMaster
            IPuppetMasterSlave puppetMasterSlave = (IPuppetMasterSlave)Activator.GetObject(typeof(IPuppetMasterSlave), puppetMasterUrl);

            List<string> brokerUrls;
            try
            {
                // obtains the broker urls at that site - these urls are probably going to be stored for reconnection later
                brokerUrls = puppetMasterSlave.GetBrokers();
            }
            catch (RemotingException)
            {
                IPuppetMasterMaster newPuppetMaster =
                    (IPuppetMasterMaster)Activator.GetObject(typeof(IPuppetMasterMaster), puppetMasterUrl);
                brokerUrls = newPuppetMaster.GetBrokers();
            }
            return brokerUrls;
        }

        public virtual void ProcessFrozenListCommands()
        {
            foreach (String[] command in FrozenStateList)
            { 
                switch (command[0])
                {
                    case "Status":
                        Console.Out.WriteLine("Printing status information:\r\nSubscriptions, etc...");
                        break;

                    case "Crash":
                        Console.WriteLine("Crashing");
                        Process.GetCurrentProcess().Kill();
                        break;

                }
            }
        }

        public virtual void DeliverCommand(string[] command)
        {
            if (Status == Status.Frozen)
            {
                //saving command
                switch (command[0])
                {
                    case "Unfreeze":
                        Console.Out.WriteLine("Unfreezing");
                        ProcessFrozenListCommands();
                        Status = Status.Unfrozen;
                        FrozenStateList.Clear();
                        break;
                    case "Freeze":

                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.BackgroundColor = ConsoleColor.Black;
                        Console.Out.WriteLine("I'm already frozen!");
                        Console.ResetColor();
                        break;
                    default:
                        FrozenStateList.Add(command);
                        break;
                }

            }
            else
            {
                switch (command[0])
                {
                    case "Status":
                        Console.Out.WriteLine("Printing status information:\r\nSubscriptions, etc...");
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
                        Console.Out.WriteLine("Unfreezing");
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

        public override object InitializeLifetimeService()
        {
            return null;
        }
    }

}
