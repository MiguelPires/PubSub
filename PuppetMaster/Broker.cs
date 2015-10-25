using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Threading.Tasks;
using CommonTypes;

namespace PuppetMaster
{
    public class Broker : MarshalByRefObject, IProcess
    {
        public string ProcessName { get; private set; }
        public IProcessMaster PuppetMaster { get; private set; }

        public Broker(string name, string url, IProcessMaster master)
        {
            ProcessName = name;
            PuppetMaster = master;
        }

        public override string ToString()
        {
            return "Broker";
        }

        public void DeliverCommand(string[] command)
        {
            throw new NotImplementedException();
        }

        public void SendLog(string log)
        {
            throw new NotImplementedException();
        }
    }
}
