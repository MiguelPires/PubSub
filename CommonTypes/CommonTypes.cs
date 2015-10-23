using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CommonTypes
{
    public interface IPuppetMaster
    {
        void DeliverConfig(string processType, string processName, string processUrl);
        void DeliverCommand(string command);
        void SendCommand(string log);
        void Ping();
    }

    public interface IPuppetMasterMaster
    {
        void DeliverCommand(string log);
        void SendCommand(string command);
    }
}
