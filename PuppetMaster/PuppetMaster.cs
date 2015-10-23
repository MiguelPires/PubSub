using System;
using CommonTypes;

namespace PuppetMaster
{
    public class PuppetMaster : MarshalByRefObject, IPuppetMaster
    {
        public void DeliverConfig(string processType, string processName, string processUrl)
        {

        }

        public void DeliverCommand(string[] commandArgs)
        {
            throw new NotImplementedException();
        }

        public void SendCommand(string log)
        {
            throw new NotImplementedException();
        }
        
        public void Register(string siteParent)
        {
            Console.WriteLine("Register");
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