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

        protected BaseProcess(string processName, string processUrl, string puppetMasterUrl)
        {
            ProcessName = processName;
            Url = processUrl;
            // connects to this site's puppetMaster
            PuppetMaster = (IProcessMaster)Activator.GetObject(typeof(IProcessMaster), puppetMasterUrl);
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

        public virtual void DeliverCommand(string[] command)
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
                    Console.Out.WriteLine("Freezing");
                    Status = Status.Frozen;
                    break;

                case "Unfreeze":
                    Console.Out.WriteLine("Unfreezing");
                    Status = Status.Unfrozen;
                    break;
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
