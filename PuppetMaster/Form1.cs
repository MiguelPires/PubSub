using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace PuppetMaster
{
    public partial class Form1 : Form
    {
        private PuppetMasterMaster master;

        public Form1(PuppetMasterMaster master)
        {
            this.master = master;
            InitializeComponent();
        }

        public void DeliverMessage(string message)
        {
            logBox.Text += message + "\r\n";
        }



        /// <summary>
        /// TODO: Ver dos metodos vazios
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void textBox1_TextChanged(object sender, EventArgs e)
        {
            //textBox1.Text
        }

        private void richTextBox2_TextChanged(object sender, EventArgs e)
        {
            
        }

        private void IndividualButton_Click(object sender, EventArgs e)
        {
            master.SendCommand(IndividualBox.Text);
        }
    }
}
