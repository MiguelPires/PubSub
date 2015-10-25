using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Serialization.Formatters;
using System.Security.Policy;
using System.Threading.Tasks;
using System.Windows.Forms;
using CommonTypes;

namespace PuppetMaster
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main(string[] args)
        {
            if (args[0].Equals("-m"))
                InitializePuppetMasterMaster(args[1]);
            else
            {
                InitializePuppetMasterSlave(args[0]);
            }
        }

        private static void InitializePuppetMasterSlave(string siteName)
        {
            PuppetMaster puppet = new PuppetMaster(siteName);
            var serverProv = new BinaryServerFormatterSinkProvider();
            serverProv.TypeFilterLevel = TypeFilterLevel.Full;

            IDictionary prop = new Hashtable();
            int port = UtilityFunctions.GetPort(siteName);
            prop["port"] = port;
            prop["name"] = siteName;

            var channel = new TcpChannel(prop, null, serverProv);
            ChannelServices.RegisterChannel(channel, false);
            RemotingServices.Marshal(puppet, prop["name"].ToString(), typeof(IPuppetMaster));
            
            string url = "tcp://localhost:" + port + "/" + siteName;
            Console.WriteLine(@"Running a "+puppet+" at " + url);
            Console.WriteLine(@"Press any key to exit");
            Console.ReadLine();
        }

        private static void InitializePuppetMasterMaster(string siteName)
        {
            PuppetMasterMaster master = new PuppetMasterMaster(siteName);
            var serverProv = new BinaryServerFormatterSinkProvider();
            serverProv.TypeFilterLevel = TypeFilterLevel.Full;

            IDictionary prop = new Hashtable();
            int port = UtilityFunctions.GetPort(siteName);
            prop["port"] = port;
            prop["name"] = "PuppetMasterMaster";

            var channel = new TcpChannel(prop, null, serverProv);
            ChannelServices.RegisterChannel(channel, false);
            RemotingServices.Marshal(master, prop["name"].ToString(), typeof(IPuppetMasterMaster));

            string url = "tcp://localhost:" + port + "/" + prop["name"];
            Console.WriteLine(@"Running a " + master + " at " + url);

            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Form1 form = new Form1(master);
            master.Form = form;
            Application.Run(form);

            Console.WriteLine(@"Press any key to exit");
            Console.ReadLine();
        }
    }
}
