using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Serialization.Formatters;
using CommonTypes;

namespace PuppetMaster
{
    public class PuppetMaster : MarshalByRefObject, IPuppetMaster, IProcessMaster
    {
        public string SiteName { get; private set; }
        public string ParentSite { get; private set; }
        public IPuppetMasterMaster Master { get; private set; }

        // maps a process name to the process instance
        public IDictionary<string, IProcess> Processes { get; private set; }


        public PuppetMaster(string siteName)
        {
            SiteName = siteName;
        }

        public override string ToString()
        {
            return "PuppetMaster";
        }

        void IPuppetMaster.DeliverConfig(string processName, string processType, string processUrl)
        {
            switch (processType)
            {
                case "broker":
                    Broker broker = new Broker(processName, processUrl, this);
                    PublishService(processName, processUrl, broker);
                    break;

                case "publisher":
                    break;

                case "subscriber":
                    break;
            }
        }

        void IPuppetMaster.DeliverCommand(string[] commandArgs)
        {
            throw new NotImplementedException();
        }

        void IPuppetMaster.SendCommand(string log)
        {
            if (!string.IsNullOrEmpty(log))
                Master.DeliverLog(log);
            else
                Console.WriteLine(@"Problem - SendCommand: The log line shouldn't be empty");
        }

        void IPuppetMaster.Register(string siteParent, string masterSite)
        {
            ParentSite = siteParent;
            string url = "tcp://localhost:" + UtilityFunctions.GetPort(SiteName) + "/PuppetMasterMaster";
            Master = (IPuppetMasterMaster) Activator.GetObject(typeof(IPuppetMasterMaster), url);
        }

        /// <summary>
        /// This method is just here for testing purposes.
        /// If it fails then there is a connection problem
        /// </summary>
        public void Ping()
        {
        }


        private void PublishService(string processName, string url, IProcess process)
        {
            PuppetMaster puppet = new PuppetMaster(processName);
            var serverProv = new BinaryServerFormatterSinkProvider();
            serverProv.TypeFilterLevel = TypeFilterLevel.Full;

            IDictionary prop = new Hashtable();

            int port;
            string serviceName;
            if (!UtilityFunctions.DivideUrl(url, out port, out serviceName))
            {
                Console.WriteLine("Invalid URL");
                return;
            }

            prop["port"] = port;
            prop["name"] = serviceName;

            var channel = new TcpChannel(prop, null, serverProv);
            ChannelServices.RegisterChannel(channel, false);
            RemotingServices.Marshal(puppet, prop["name"].ToString(), typeof(IPuppetMaster));

            Console.WriteLine(@"Running a "+process+" at " + url);
        }


        void IProcessMaster.DeliverLogToPuppetMaster(string log)
        {
            throw new NotImplementedException();
        }
    }
}