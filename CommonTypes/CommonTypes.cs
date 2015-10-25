using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CommonTypes
{
    public enum LoggingLevel
    {
        Light,
        Full
    }

    public enum RoutingPolicy
    {
        Flood,
        Filter
    }

    public enum OrderingGuarantee
    {
        No,
        FIFO,
        Total
    }
    /// <summary>
    /// The interface for the PuppetMaster for the PuppetMaster - PuppetMasterMaster communication
    /// </summary>
    public interface IPuppetMaster
    {
        void DeliverConfig(string processName, string processType, string processUrl);
        void DeliverCommand(string[] commandArgs);
        void SendCommand(string log);
        void Register(string siteParent, string masterName);
        void Ping();
    }
    /// <summary>
    /// The interface for the PuppetMasterMaster for the PuppetMaster - PuppetMasterMaster communication
    /// </summary>
    public interface IPuppetMasterMaster
    {
        void DeliverLog(string log);
        void SendCommand(string command);
    }

    /// <summary>
    /// The interface for the Process for the PuppetMaster - Process communication
    /// </summary>
    public interface IProcess
    {
        void DeliverCommand(string[] command);
        void SendLog(string log);
    }

    /// <summary>
    /// The interface for the PuppetMaster for the PuppetMaster - Process communication
    /// </summary>
    public interface IProcessMaster
    {
        void DeliverLogToPuppetMaster(string log);
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


        /// <summary>
        /// Parses the URL to extract the port and service name, if possible
        /// </summary>
        /// <param name="url"> The URL to parse </param>
        /// <param name="port"> An output parameter - the port </param>
        /// <param name="name"> An output parameter - the service name </param>
        /// <returns> True if the url can be parsed, false otherwise </returns>
        public static bool DivideUrl(string url, out int port, out string name)
        {
            port = -1;
            name = "";

            if (string.IsNullOrEmpty(url))
                return false;

            string[] portParts = url.Split(':');

            // there should always be three substrings 
            if (portParts.Length != 3)
                return false;

            // extract the port (8080) and name ("service") out of "8080/service"
            string[] lastParts = portParts.Last().Split('/');

            if (lastParts.Length != 2)
                return false;

            port = int.Parse(lastParts.First()); 
            name = lastParts.Last();

            return true;
        }
    }
}
