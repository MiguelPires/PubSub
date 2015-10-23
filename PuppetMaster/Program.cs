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
            if (!args.Any() || args[0].Equals("-m"))
                InitializePuppetMasterMaster();
            else
            {
                InitializePuppetMasterSlave(port: int.Parse(args[0]), name: args[1]);
            }
        }

        private static void InitializePuppetMasterSlave(int port, string name)
        {
            PuppetMaster master = new PuppetMaster();

            var serverProv = new BinaryServerFormatterSinkProvider();
            serverProv.TypeFilterLevel = TypeFilterLevel.Full;

            IDictionary prop = new Hashtable();
            prop["port"] = port;
            prop["name"] = name;
            var channel = new TcpChannel(prop, null, serverProv);

            ChannelServices.RegisterChannel(channel, false);

            RemotingServices.Marshal(master, name, typeof(IPuppetMaster));

            Console.WriteLine(@"Press any key to exit");
            Console.ReadLine();
        }

        private static void InitializePuppetMasterMaster()
        {
            PuppetMasterMaster master = new PuppetMasterMaster();

            var serverProv = new BinaryServerFormatterSinkProvider();
            serverProv.TypeFilterLevel = TypeFilterLevel.Full;

            IDictionary prop = new Hashtable();
            // the puppet master master port is fixed
            prop["port"] = 8081;
            prop["name"] = "PuppetMasterMaster";
            var channel = new TcpChannel(prop, null, serverProv);

            ChannelServices.RegisterChannel(channel, false);

            RemotingServices.Marshal(master, "PuppetMasterMaster", typeof(IPuppetMasterMaster));

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
