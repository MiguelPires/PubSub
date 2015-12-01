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
                RemotingServices.Marshal(publisher, prop["name"].ToString(), typeof (IPublisher));
            }
            catch (Exception)
            {
                Console.Out.WriteLine("********************************************\r\n");
                Console.Out.WriteLine("\tERROR: A problem occured while registering this service");
                Console.Out.WriteLine("\r\n********************************************");
                Console.ReadLine();
            }
            
            Console.WriteLine(@"Running " + processName+ " at " + processUrl + " - " + site );
            Console.ReadLine();
        }
    }
}
