using System;
using System.Drawing;
using System.Linq;
using System.Threading;
using System.Windows.Forms;
using CommonTypes;

namespace PuppetMaster
{
    public partial class InteractionForm : Form
    {
        private readonly IPuppetMasterMaster master;

        public InteractionForm(PuppetMasterMaster master, string siteName)
        {
            this.master = master;
            this.Text = "Command - " + siteName;
            InitializeComponent();
            IndividualBox.KeyDown += iKeyDown;
            GroupBox.KeyDown += gKeyDown;

        }

        void iKeyDown(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Enter)
            {
                IndividualButton_Click(sender, e);
            }
        }

        void gKeyDown(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Enter)
            {
                GroupButton_Click(sender, e);
            }
        }

        public void DeliverMessage(string message)
        {
            this.logBox.Text += message.Trim() + "\r\n";
        }

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

            string command = IndividualBox.Text;
            try
            {
                Thread thread = new Thread(() => this.master.SendCommand(command.Trim()));
                thread.Start();
                //this.master.SendCommand(command.Trim());
            }
            catch (CommandParsingException ex)
            {
                // TODO :this is not working properly
                Console.Out.WriteLine(ex.Message);
                logBox.SelectionStart = logBox.Text.Length;
                logBox.SelectionLength = IndividualBox.Text.Length;
                logBox.SelectionColor = Color.Red;
                this.logBox.AppendText(this.IndividualBox.Text);
                logBox.SelectionColor = logBox.ForeColor;
                this.IndividualBox.Clear();
                return;
            }

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
                {
                    this.master.SendCommand(line);/*
                    Thread thread = new Thread(() => this.master.SendCommand(line));
                    thread.Start();*/
                }
                this.logBox.Text += line + "\r\n";
                Console.WriteLine(line);
            }

            this.GroupBox.Clear();
        }
    }
}