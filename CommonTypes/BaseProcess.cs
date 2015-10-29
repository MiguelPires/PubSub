using System;
using System.Collections.Generic;
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

        public BaseProcess(string processName, string processUrl, string puppetMasterUrl)
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
            catch (RemotingException e)
            {
                Console.Out.WriteLine("URL: "+puppetMasterUrl);
                IPuppetMasterMaster newPuppetMaster =
                    (IPuppetMasterMaster)Activator.GetObject(typeof(IPuppetMasterMaster), puppetMasterUrl);
                brokerUrls = newPuppetMaster.GetBrokers();
            }
            return brokerUrls;
        }

        public abstract void DeliverCommand(string[] command);
        public abstract void SendLog(string log);

        public override object InitializeLifetimeService()
        {
            return null;
        }
    }
}
