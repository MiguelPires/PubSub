using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace CommonTypes
{
    [Serializable]
    public enum LoggingLevel
    {
        Light,
        Full
    }

    [Serializable]
    public enum RoutingPolicy
    {
        Flood,
        Filter
    }

    [Serializable]
    public enum OrderingGuarantee
    {
        No,
        Fifo,
        Total
    }

    public enum Status
    {
        Unfrozen,
        Frozen
    }

    /// <summary>
    ///     The interface for the PuppetMaster for the PuppetMaster - PuppetMasterMaster communication
    /// </summary>
    public interface IPuppetMasterSlave : IPuppetMaster
    {
        void LaunchProcess(string processName, string processType, string processUrl);
        void DeliverSetting(string settingType, string settingValue);
        void DeliverCommand(string[] commandArgs);
        void SendLog(string log);
        void RegisterWithMaster(string siteParent, string masterName);
        void DeliverSettingsToLocalProcesses(string routingPolicy, string loggingLevel, string orderingGuarantee);
    }

    /// <summary>
    ///     The interface for the PuppetMasterMaster for the PuppetMaster - PuppetMasterMaster communication
    /// </summary>
    public interface IPuppetMasterMaster : IPuppetMaster
    {
        void DeliverLog(string log);
        void SendCommand(string command);
    }

    /// <summary>
    ///     Holds the common methods between both types of PuppetMasters
    /// </summary>
    public interface IPuppetMaster
    {
        List<string> GetBrokers();
        void Ping();
        RoutingPolicy GetRoutingPolicy();
        LoggingLevel GetLoggingLevel();
        OrderingGuarantee GetOrderingGuarantee();
    }

    /// <summary>
    ///     The interface for the Process for the PuppetMaster - Process communication
    /// </summary>
    public interface IProcess
    {
        void DeliverSetting(string settingType, string settingValue);
        void DeliverCommand(string[] command);
        void SendLog(string log);
    }

    /// <summary>
    ///     The interface for the PuppetMaster for the PuppetMaster - Process communication
    /// </summary>
    public interface IProcessMaster
    {
        void DeliverLogToPuppetMaster(string log);
    }

    /// <summary>
    ///     The interface for the Brokers
    /// </summary>
    public interface IBroker : IProcess, IReplica
    {
        void RegisterBroker(string siteName, string brokerUrl);
        void RegisterPubSub(string procName, string procUrl);
        void DeliverSubscription(string origin, string topic, string siteName, int sequenceNumber);
        void DeliverUnsubscription(string origin, string topic,string siteName, int sequenceNumber);
        void DeliverPublication(string origin, string topic, string publication, string siteName, int sequenceNumber);
        void AddSiblingBroker(string siblingUrl);
    }

    /// <summary>
    ///     The interface used by the Brokers to replicate their collective state
    /// </summary>
    public interface IReplica
    {
        void AddLocalSubscription(string process, string topic, string siteName, int sequenceNumber);
        void RemoveLocalSubscription(string process, string topic, string siteName, int sequenceNumber);
       /* void AddRemoteSubscription(string topic, string process);
        void RemoveRemoteSubscription(string topic, string process);*/
    }

    public interface IPublisher : IProcess
    {
        void SendPublication(string topic, string publication);
    }

    public interface ISubscriber : IProcess
    {
        void DeliverPublication(string publication, int sequenceNumber);
    }

    /// <summary>
    /// A library of useful functions shared between various entities
    /// </summary>
    public class UtilityFunctions
    {
        /// <summary>
        ///     Returns the port used for the PupperMaster at a given site
        /// </summary>
        /// <param name="siteName"> The site's name </param>
        /// <returns> The port </returns>
        public static int GetPort(string siteName)
        {
            // the site's port is given by the sum of  8080 and the site number (e.g., 0 for site0)
            var a = Regex.Match(siteName, @"\d+").Value;
            var siteNumber = Int32.Parse(a);
            return (8080 + siteNumber);
        }

        /// <summary>
        ///     Returns the url used for the PuppetMaster at given site
        /// </summary>
        /// <param name="siteName"></param>
        /// <returns></returns>
        public static string GetUrl(string siteName)
        {
            return "tcp://localhost:" + GetPort(siteName) + "/" + siteName;
        }

        /// <summary>
        ///     Parses the URL to extract the port and service name, if possible
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