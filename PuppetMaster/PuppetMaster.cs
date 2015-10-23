using System;
using CommonTypes;

namespace PuppetMaster
{
    public class PuppetMaster : MarshalByRefObject, IPuppetMaster
    {
        public string Site { get; private set; }
        public string ParentSite { get; private set; }
        public IPuppetMasterMaster Master { get; private set; }

        public PuppetMaster(string siteName)
        {
            Site = siteName;
        }
        public void DeliverConfig(string processName, string processType, string processUrl)
        {
           // throw new NotImplementedException();

        }

        public void DeliverCommand(string[] commandArgs)
        {
            throw new NotImplementedException();
        }

        public void SendCommand(string log)
        {
            throw new NotImplementedException();
        }
        
        public void Register(string siteParent, string masterSite)
        {
            ParentSite = siteParent;
            string url = "tcp://localhost:" + UtilityFunctions.GetPort(Site) + "/PuppetMasterMaster";
            Master = (IPuppetMasterMaster) Activator.GetObject(typeof(IPuppetMasterMaster), url);
        }

        /// <summary>
        /// This method is just here for testing purposes.
        /// If it fails then there is a connection problem
        /// </summary>
        public void Ping()
        {
        }
    }
}