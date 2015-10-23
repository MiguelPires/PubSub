using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CommonTypes
{
    public interface IPuppetMaster
    {
        void DeliverConfig(string processName, string processType, string processUrl);
        void DeliverCommand(string[] commandArgs);
        void SendCommand(string log);
        void Register(string siteParent, string masterName);
        void Ping();
    }

    public interface IPuppetMasterMaster
    {
        void DeliverCommand(string log);
        void SendCommand(string command);
    }

    public class UtilityFunctions
    {
        /// <summary>
        /// Returns the port used for a given site
        /// </summary>
        /// <param name="siteName"> The site's name </param>
        /// <returns> The port </returns>
        public static int GetPort(string siteName)
        {
            // the site's port is given by the sum of  8080 and the site number (e.g., 0 for site0)
            return 8080 + int.Parse(siteName[siteName.Length - 1].ToString());
        }
    }
}
