using System;
using System.Windows.Forms;
using CommonTypes;

namespace PuppetMaster
{
    public partial class LoggingForm : Form
    {
        public IPuppetMasterSlave PuppetMasterSlave { get; set; }

        public LoggingForm(string siteName)
        {
            InitializeComponent();
            this.Text = "Log - " + siteName;
        }

        public void DeliverMessage(string log)
        {
            LogBox.Text += log + "\r\n";
        }

        private void label1_Click(object sender, EventArgs e)
        {
        }

        private void LogBox_TextChanged(object sender, EventArgs e)
        {
        }
    }
}