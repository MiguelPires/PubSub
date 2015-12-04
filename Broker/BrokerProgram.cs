#region

using System;
using System.Collections;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Serialization.Formatters;
using CommonTypes;

#endregion

namespace Broker
{
    internal class BrokerProgram
    {
        private static void Main(string[] args)
        {
            if (args.Length != 5)
            {
                Console.Out.WriteLine("Broker - Incorrect number of arguments: " + args.Length);
                Console.ReadLine();
                return;
            }

            string processName = args[0];
            string processUrl = args[1];
            string puppetMasterUrl = args[2];
            string siteName = args[3];
            string parentSite = args[4];

            Broker broker = new Broker(processName, processUrl, puppetMasterUrl, siteName, parentSite);
            BinaryServerFormatterSinkProvider serverProv = new BinaryServerFormatterSinkProvider();
            serverProv.TypeFilterLevel = TypeFilterLevel.Full;

            IDictionary prop = new Hashtable();

            int port;
            string serviceName;
            if (!Utility.DivideUrl(processUrl, out port, out serviceName))
            {
                Console.WriteLine("Invalid process URL");
                Console.ReadLine();
                return;
            }

            prop["port"] = port;
            prop["name"] = serviceName;

            try
            {
                TcpChannel channel = new TcpChannel(prop, null, serverProv);
                ChannelServices.RegisterChannel(channel, false);
                RemotingServices.Marshal(broker, prop["name"].ToString(), typeof (IBroker));
            } catch (Exception ex)
            {
                Console.Out.WriteLine("********************************************");
                Console.Out.WriteLine("*\tERROR: A problem occured while registering this service");
                Console.Out.WriteLine("*\t" + ex.Message);
                Console.Out.WriteLine("*********************************************");
                Console.ReadLine();
            }

            Console.WriteLine(@"Running " + processName + " at " + processUrl + " - " + siteName);
            Console.ReadLine();
        }
    }
}