#region

using System;
using System.Collections;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Serialization.Formatters;
using CommonTypes;

#endregion

namespace Subscriber
{
    internal class SubscriberProgram
    {
        private static void Main(string[] args)
        {
            if (args.Length != 4)
            {
                Console.Out.WriteLine("Subscriber - Incorrect number of arguments");
                Console.ReadLine();
                return;
            }

            string processName = args[0];
            string processUrl = args[1];
            string puppetMasterUrl = args[2];
            string siteName = args[3];

            Subscriber subscriber = new Subscriber(processName, processUrl, puppetMasterUrl, siteName);
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
                RemotingServices.Marshal(subscriber, prop["name"].ToString(), typeof (ISubscriber));
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