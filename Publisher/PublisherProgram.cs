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

namespace Publisher
{
    class PublisherProgram
    {
        static void Main(string[] args)
        {
            if (args.Length != 4)
            {
                Console.Out.WriteLine("Publisher - Incorrect number of arguments");
                Console.ReadLine();
                return;
            }

            string processName = args[0];
            string processUrl = args[1];
            string puppetMasterUrl = args[2];
            string site = args[3];

            Publisher publisher = new Publisher(processName, processUrl, puppetMasterUrl, site);
            Console.Out.WriteLine("Config:");
            Console.Out.WriteLine("OrderingGuarantee: {0}", publisher.OrderingGuarantee);
            Console.Out.WriteLine("RoutingPolicy: {0}", publisher.RoutingPolicy);
            Console.Out.WriteLine("LoggingLevel: {0}", publisher.LoggingLevel);
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
            RemotingServices.Marshal(publisher, prop["name"].ToString(), typeof(IProcess));

            Console.WriteLine(@"Running a " + publisher+ " at " + processUrl + " [Site:" + site + "]");
            Console.ReadLine();
        }
    }
}
