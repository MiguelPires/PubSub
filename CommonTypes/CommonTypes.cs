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
        void RegisterWithMaster(string siteParent, string masterName);
    }

    /// <summary>
    ///     The interface for the PuppetMasterMaster for the PuppetMaster - PuppetMasterMaster communication
    /// </summary>
    public interface IPuppetMasterMaster : IPuppetMaster
    {
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
        void DeliverLog(string log);
    }

    /// <summary>
    ///     The interface for the Process for the PuppetMaster - Process communication
    /// </summary>
    public interface IProcess
    {
        void DeliverSetting(string settingType, string settingValue);
        bool DeliverCommand(string[] command);
        void Ping();
    }

    /// <summary>
    ///     The interface for the Brokers
    /// </summary>
    public interface IBroker : IProcess, IReplica
    {
        void RegisterBroker(string siteName, string brokerUrl);
        void RegisterPubSub(string procName, string procUrl);
        void DeliverSubscription(string subscriber, string topic, string siteName);
        void DeliverUnsubscription(string subscriber, string topic, string siteName);
        void DeliverPublication(string publisher, string topic, string publication, string siteName, int sequenceNumber);
        void AddSiblingBroker(string siblingUrl);
    }

    /// <summary>
    ///     The interface used by the Brokers to replicate their collective state
    /// </summary>
    public interface IReplica
    {
        void InformOfPublication(string publisher, string topic, string publication, string fromSite, int sequenceNumber, string process);
        void InformOfSubscription(string subscriber, string topic, string siteName);
        void InformOfUnsubscription(string subscriber, string topic, string siteName);
        void RequestPublication(string publisher, string requestingSite, int sequenceNumber, string subscriber=null);
    }

    public interface IPublisher : IProcess
    {
        void SendPublication(string topic, string publication, int sequenceNumber);
        void RequestPublication(int sequenceNumber);
    }

    public interface ISubscriber : IProcess
    {
        void DeliverPublication(string publisher, string topic, string publication, int sequenceNumber);
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
        public delegate T ConnectFunction<T>(string url);
        /// <summary>
        /// Tries to execute the connection function for maximumTries waiting sleepInterval (miliseconds) between each try
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connectionFunction">A connectFunction delegate </param>
        /// <param name="sleepInterval">time between each try in ms</param>
        /// <param name="maximumTries">the number of tries</param>
        /// <param name="url">the url to connect</param>
        /// <returns></returns>
        public static T TryConnection<T>(ConnectFunction<T> connectionFunction, string url, int maximumTries = 15,  int sleepInterval = -1)
        {
            if (sleepInterval == -1)
            {
                Random rand = new Random();
                sleepInterval = rand.Next(100, 2000);
            }


            int retryCount = maximumTries;

            var result = default(T);
            while (retryCount > 0)
            {
                try
                {
                    result = connectionFunction(url);
                    break;
                }
                catch (Exception)
                {
                    retryCount--;

                    if (retryCount == 0)
                    {
                        Console.Out.WriteLine("Error: Couldn't connect to "+url+" after " + maximumTries + " tries. ");
                        throw; 
                    }

                    if (sleepInterval != 0)
                        System.Threading.Thread.Sleep(sleepInterval);

                }
            }
            return result;
        }

        public static bool StringEquals(string s1, string s2)
        {
            int minLength = s1.Length < s2.Length ? s1.Length : s2.Length;

            for (int i = 0; i < minLength; i++)
            {
                if (s1[i] != s2[i])
                    return false;
            }
            return true;
        }
    }
}