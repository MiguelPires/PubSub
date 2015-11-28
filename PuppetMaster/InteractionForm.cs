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
            if (e.KeyCode == Keys.Enter && Control.ModifierKeys != Keys.Shift)
            {
                GroupButton_Click(sender, e);
            }
        }

        public void DeliverMessage(string message)
        {
            int start = logBox.Text.Length;
            int end = start + message.Length;
            this.logBox.AppendText(message.Trim() + "\r\n");
            logBox.Select(start, end);
            logBox.SelectionColor = Color.Black;
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

            int start = logBox.Text.Length;
            int end = start + IndividualBox.Text.Length;

            string command = IndividualBox.Text;
            try
            {
                this.master.SendCommand(command.Trim());
                this.logBox.AppendText(this.IndividualBox.Text + "\r\n");
                logBox.Select(start, end);
                logBox.SelectionColor = Color.Black;
            }
            catch (CommandParsingException ex)
            {
                Console.Out.WriteLine(ex.Message);
                this.logBox.AppendText(this.IndividualBox.Text+"\r\n");
                logBox.Select(start, end);
                logBox.SelectionColor = Color.Red;
            }
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
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                int start = logBox.Text.Length;
                int end = start + line.Length;
                this.logBox.AppendText(line + "\r\n");

                string[] tokens = line.Split(' ');
                if (tokens[0].Equals("Wait"))
                {
                    if (tokens.Length != 2)
                    {
                        logBox.Select(start, end);
                        logBox.SelectionColor = Color.Red;
                        continue;
                    }
                    int numVal = Int32.Parse(tokens[1]);
                    Thread.Sleep(numVal);
                }
                else
                {
                    try
                    {
                        this.master.SendCommand(line);
                    } catch (CommandParsingException ex)
                    {
                        Console.Out.WriteLine(ex.Message);
                        logBox.Select(start, end);
                        logBox.SelectionColor = Color.Red;
                        continue;
                    }
                }

                logBox.Select(start, end);
                logBox.SelectionColor = Color.Black;
            }

            this.GroupBox.Clear();
        }
    }
}