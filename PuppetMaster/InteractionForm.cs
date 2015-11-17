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
            this.logBox.Text += message.Trim() + "\r\n";
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
            this.master.SendCommand(this.IndividualBox.Text.Trim());
            this.logBox.Text += this.IndividualBox.Text + "\r\n";
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

            foreach (string line in lines)

            {
                string[] tokens = line.Split(' ');
                if (tokens[0].Equals("Wait"))
                {
                    if (tokens.Length != 2)
                        continue;
                    int numVal = Int32.Parse(tokens[1]);
                    System.Threading.Thread.Sleep(numVal);
                }
                else
                  this.master.SendCommand(line);
                Console.WriteLine(line);
            }

            this.logBox.Text += this.GroupBox.Text + "\r\n";
            this.GroupBox.Clear();
        }
    }
}