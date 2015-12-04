#region

using System;
using System.Drawing;
using System.Windows.Forms;
using CommonTypes;

#endregion

namespace PuppetMaster
{
    public partial class InteractionForm : Form
    {
        private readonly IPuppetMasterMaster _master;

        public InteractionForm(PuppetMasterMaster master, string siteName)
        {
            InitializeComponent();
            this._master = master;
            Text = "Command - " + siteName;
            this.IndividualBox.KeyDown += iKeyDown;
            this.GroupBox.KeyDown += gKeyDown;
        }

        private void iKeyDown(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Enter)
            {
                IndividualButton_Click(sender, e);
            }
        }

        private void gKeyDown(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Enter && ModifierKeys != Keys.Shift)
            {
                GroupButton_Click(sender, e);
            }
        }

        public void DeliverMessage(string message)
        {
            int start = this.logBox.Text.Length;
            int end = start + message.Length;
            this.logBox.AppendText(message.Trim() + "\r\n");
            this.logBox.Select(start, end);
            this.logBox.SelectionColor = Color.Black;
            logBox.SelectionStart = logBox.Text.Length;
            logBox.ScrollToCaret();
        }

        private void IndividualButton_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrWhiteSpace(this.IndividualBox.Text))
                return;

            int start = this.logBox.Text.Length;
            int end = start + this.IndividualBox.Text.Length;

            string command = this.IndividualBox.Text;
            try
            {
                this._master.SendCommand(command.Trim());
                this.logBox.AppendText(this.IndividualBox.Text + "\r\n");
                this.logBox.Select(start, end);
                this.logBox.SelectionColor = Color.Black;
            } catch (CommandParsingException ex)
            {
                Console.Out.WriteLine(ex.Message);
                this.logBox.AppendText(this.IndividualBox.Text + "\r\n");
                this.logBox.Select(start, end);
                this.logBox.SelectionColor = Color.Red;
            }
            this.IndividualBox.Clear();
            logBox.SelectionStart = logBox.Text.Length;
            logBox.ScrollToCaret();
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

                int start = this.logBox.Text.Length;
                int end = start + line.Length;
                this.logBox.AppendText(line + "\r\n");
                try
                {
                    this._master.SendCommand(line);
                } catch (CommandParsingException ex)
                {
                    Console.Out.WriteLine(ex.Message);
                    this.logBox.Select(start, end);
                    this.logBox.SelectionColor = Color.Red;
                    continue;
                }

                this.logBox.Select(start, end);
                this.logBox.SelectionColor = Color.Black;
            }

            this.GroupBox.Clear();
            logBox.SelectionStart = logBox.Text.Length;
            logBox.ScrollToCaret();
        }

        private void GroupBox_TextChanged(object sender, EventArgs e)
        {
        }

        private void textBox1_TextChanged(object sender, EventArgs e)
        {
        }

        private void richTextBox2_TextChanged(object sender, EventArgs e)
        {
        }
    }
}