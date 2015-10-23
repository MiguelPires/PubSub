using System;
using CommonTypes;

namespace PuppetMaster
{
    public class PuppetMaster : MarshalByRefObject, IPuppetMaster
    {
        public void DeliverConfig(string processType, string processName, string processUrl)
        {

        }

        public void DeliverCommand(string command)
        {
            
        }

        public void SendCommand(string log)
        {
            throw new NotImplementedException();
        }



        /// <summary>
        ///     This method is used just to check if the site is up
        ///     If it's not, the call will fail
        /// </summary>
        public void Ping()
        {
        }
    }
}