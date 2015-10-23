namespace PuppetMaster
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.IndividualBox = new System.Windows.Forms.TextBox();
            this.GroupBox = new System.Windows.Forms.RichTextBox();
            this.IndividualButton = new System.Windows.Forms.Button();
            this.GroupButton = new System.Windows.Forms.Button();
            this.logBox = new System.Windows.Forms.RichTextBox();
            this.SuspendLayout();
            // 
            // IndividualBox
            // 
            this.IndividualBox.Location = new System.Drawing.Point(29, 65);
            this.IndividualBox.Name = "IndividualBox";
            this.IndividualBox.Size = new System.Drawing.Size(277, 20);
            this.IndividualBox.TabIndex = 0;
            this.IndividualBox.TextChanged += new System.EventHandler(this.textBox1_TextChanged);
            // 
            // GroupBox
            // 
            this.GroupBox.Location = new System.Drawing.Point(29, 237);
            this.GroupBox.Name = "GroupBox";
            this.GroupBox.Size = new System.Drawing.Size(277, 255);
            this.GroupBox.TabIndex = 1;
            this.GroupBox.Text = "";
            // 
            // IndividualButton
            // 
            this.IndividualButton.Location = new System.Drawing.Point(312, 62);
            this.IndividualButton.Name = "IndividualButton";
            this.IndividualButton.Size = new System.Drawing.Size(75, 23);
            this.IndividualButton.TabIndex = 2;
            this.IndividualButton.Text = "Send";
            this.IndividualButton.UseVisualStyleBackColor = true;
            this.IndividualButton.Click += new System.EventHandler(this.IndividualButton_Click);
            // 
            // GroupButton
            // 
            this.GroupButton.Location = new System.Drawing.Point(322, 469);
            this.GroupButton.Name = "GroupButton";
            this.GroupButton.Size = new System.Drawing.Size(75, 23);
            this.GroupButton.TabIndex = 3;
            this.GroupButton.Text = "Send";
            this.GroupButton.UseVisualStyleBackColor = true;
            // 
            // logBox
            // 
            this.logBox.Location = new System.Drawing.Point(416, 62);
            this.logBox.Name = "logBox";
            this.logBox.Size = new System.Drawing.Size(263, 430);
            this.logBox.TabIndex = 4;
            this.logBox.Text = "";
            this.logBox.TextChanged += new System.EventHandler(this.richTextBox2_TextChanged);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(691, 526);
            this.Controls.Add(this.logBox);
            this.Controls.Add(this.GroupButton);
            this.Controls.Add(this.IndividualButton);
            this.Controls.Add(this.GroupBox);
            this.Controls.Add(this.IndividualBox);
            this.Name = "Form1";
            this.Text = "Form1";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox IndividualBox;
        private System.Windows.Forms.RichTextBox GroupBox;
        private System.Windows.Forms.Button IndividualButton;
        private System.Windows.Forms.Button GroupButton;
        private System.Windows.Forms.RichTextBox logBox;
    }
}

