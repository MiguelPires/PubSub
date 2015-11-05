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

namespace Subscriber
{
    class SubscriberProgram
    {
        static void Main(string[] args)
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
            if (!UtilityFunctions.DivideUrl(processUrl, out port, out serviceName))
            {
                Console.WriteLine("Invalid process URL");
                Console.ReadLine();
                return;
            }

            prop["port"] = port;
            prop["name"] = serviceName;

            TcpChannel channel = new TcpChannel(prop, null, serverProv);
            ChannelServices.RegisterChannel(channel, false);
            RemotingServices.Marshal(subscriber, prop["name"].ToString(), typeof(IProcess));

            Console.WriteLine(@"Running a " + subscriber + " at " + processUrl);
            Console.ReadLine();
        }
    }
}
