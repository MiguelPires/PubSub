using System;
using System.Windows.Forms;
using CommonTypes;

namespace PuppetMaster
{
    public partial class InteractionForm : Form
    {
        private readonly IPuppetMasterMaster master;

        public InteractionForm(PuppetMasterMaster master)
        {
            this.master = master;
            InitializeComponent();
        }
        
        public void DeliverMessage(string message)
        {
            this.logBox.Text += message + "\r\n";
        }


        // TODO: Ver dos metodos vazios
        private void textBox1_TextChanged(object sender, EventArgs e)
        {
        }

        private void richTextBox2_TextChanged(object sender, EventArgs e)
        {
        }

        private void IndividualButton_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrWhiteSpace(this.IndividualBox.Text))
                return;

            this.master.SendCommand(this.IndividualBox.Text);
            this.IndividualBox.Clear();
        }

        private void GroupBox_TextChanged(object sender, EventArgs e)
        {
        }

        private void GroupButton_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrWhiteSpace(this.GroupBox.Text))
                return;

            string[] lines = this.GroupBox.Text.Split('\n');

            //TODO: SEND EACH LINE TO THE PUPPETMASTER
            foreach (string line in lines)
                Console.WriteLine(line);

            this.GroupBox.Clear();
        }
    }
}