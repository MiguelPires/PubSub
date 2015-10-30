namespace PuppetMaster
{
    partial class InteractionForm
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
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
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
            this.GroupBox.TextChanged += new System.EventHandler(this.GroupBox_TextChanged);
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
            this.GroupButton.Click += new System.EventHandler(this.GroupButton_Click);
            // 
            // logBox
            // 
            this.logBox.Location = new System.Drawing.Point(416, 62);
            this.logBox.Name = "logBox";
            this.logBox.ReadOnly = true;
            this.logBox.Size = new System.Drawing.Size(263, 430);
            this.logBox.TabIndex = 4;
            this.logBox.Text = "";
            this.logBox.TextChanged += new System.EventHandler(this.richTextBox2_TextChanged);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(29, 46);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(107, 13);
            this.label1.TabIndex = 5;
            this.label1.Text = "Individual Commands";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(32, 218);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(90, 13);
            this.label2.TabIndex = 6;
            this.label2.Text = "Batch Commands";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(416, 46);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(25, 13);
            this.label3.TabIndex = 7;
            this.label3.Text = "Log";
            // 
            // InteractionForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(691, 526);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.logBox);
            this.Controls.Add(this.GroupButton);
            this.Controls.Add(this.IndividualButton);
            this.Controls.Add(this.GroupBox);
            this.Controls.Add(this.IndividualBox);
            this.Name = "InteractionForm";
            this.Text = "Interaction Form";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox IndividualBox;
        private System.Windows.Forms.RichTextBox GroupBox;
        private System.Windows.Forms.Button IndividualButton;
        private System.Windows.Forms.Button GroupButton;
        private System.Windows.Forms.RichTextBox logBox;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label3;
    }
}

