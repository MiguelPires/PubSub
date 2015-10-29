using System;
using System.Collections;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Serialization.Formatters;
using System.Windows.Forms;
using CommonTypes;

namespace PuppetMaster
{
    internal static class PuppetMasterProgram
    {
        /// <summary>
        ///     The main entry point for the application.
        /// </summary>
        [STAThread]
        private static void Main(string[] args)
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
            PuppetMasterSlaveSlave puppet = new PuppetMasterSlaveSlave(siteName);
            BinaryServerFormatterSinkProvider serverProv = new BinaryServerFormatterSinkProvider();
            serverProv.TypeFilterLevel = TypeFilterLevel.Full;

            IDictionary prop = new Hashtable();
            int port = UtilityFunctions.GetPort(siteName);
            prop["port"] = port;
            prop["name"] = siteName;

            TcpChannel channel = new TcpChannel(prop, null, serverProv);
            ChannelServices.RegisterChannel(channel, false);
            RemotingServices.Marshal(puppet, prop["name"].ToString(), typeof (IPuppetMasterSlave));

            string url = "tcp://localhost:" + port + "/" + siteName;
            Console.WriteLine(@"Running a " + puppet + " at " + url);

            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            LoggingForm form = new LoggingForm();
            puppet.Form = form;
            Application.Run(form);

            Console.WriteLine(@"Press any key to exit");
            Console.ReadLine();
        }

        private static void InitializePuppetMasterMaster(string siteName)
        {
            PuppetMasterMaster master = new PuppetMasterMaster(siteName);
            BinaryServerFormatterSinkProvider serverProv = new BinaryServerFormatterSinkProvider();
            serverProv.TypeFilterLevel = TypeFilterLevel.Full;

            IDictionary prop = new Hashtable();
            int port = UtilityFunctions.GetPort(siteName);
            prop["port"] = port;
            prop["name"] = siteName;

            TcpChannel channel = new TcpChannel(prop, null, serverProv);
            ChannelServices.RegisterChannel(channel, false);
            RemotingServices.Marshal(master, prop["name"].ToString(), typeof (IPuppetMasterMaster));

            string url = "tcp://localhost:" + port + "/" + prop["name"];
            Console.WriteLine(@"Running a " + master + " at " + url);

            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            InteractionForm form = new InteractionForm(master);
            master.Form = form;
            Application.Run(form);

            Console.WriteLine(@"Press any key to exit");
            Console.ReadLine();
        }
    }
}