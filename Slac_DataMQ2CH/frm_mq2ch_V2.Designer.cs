namespace Slac_DataMQ2CH
{
    partial class frm_mq2ch_V2
    {
        /// <summary>
        /// 必需的设计器变量。
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// 清理所有正在使用的资源。
        /// </summary>
        /// <param name="disposing">如果应释放托管资源，为 true；否则为 false。</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows 窗体设计器生成的代码

        /// <summary>
        /// 设计器支持所需的方法 - 不要修改
        /// 使用代码编辑器修改此方法的内容。
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(frm_mq2ch_V2));
            this.listBox1 = new System.Windows.Forms.ListBox();
            this.btn_Save2DB = new System.Windows.Forms.Button();
            this.menu_icon = new System.Windows.Forms.ContextMenuStrip(this.components);
            this.btn_show = new System.Windows.Forms.ToolStripMenuItem();
            this.btn_closeme = new System.Windows.Forms.ToolStripMenuItem();
            this.notifyIcon1 = new System.Windows.Forms.NotifyIcon(this.components);
            this.checkBox1 = new System.Windows.Forms.CheckBox();
            this.label1 = new System.Windows.Forms.Label();
            this.CheckBox_IsShowLog = new System.Windows.Forms.CheckBox();
            this.checkBox3 = new System.Windows.Forms.CheckBox();
            this.checkBox4 = new System.Windows.Forms.CheckBox();
            this.checkBox5 = new System.Windows.Forms.CheckBox();
            this.TextBox_Log3 = new System.Windows.Forms.TextBox();
            this.TextBox_Log1 = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.menu_icon.SuspendLayout();
            this.SuspendLayout();
            // 
            // listBox1
            // 
            this.listBox1.BackColor = System.Drawing.SystemColors.MenuText;
            this.listBox1.BorderStyle = System.Windows.Forms.BorderStyle.None;
            this.listBox1.ForeColor = System.Drawing.Color.Lime;
            this.listBox1.FormattingEnabled = true;
            this.listBox1.ItemHeight = 17;
            this.listBox1.Location = new System.Drawing.Point(3, 50);
            this.listBox1.Name = "listBox1";
            this.listBox1.Size = new System.Drawing.Size(951, 17);
            this.listBox1.TabIndex = 7;
            this.listBox1.Visible = false;
            // 
            // btn_Save2DB
            // 
            this.btn_Save2DB.Font = new System.Drawing.Font("宋体", 13.8F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.btn_Save2DB.Location = new System.Drawing.Point(12, 12);
            this.btn_Save2DB.Name = "btn_Save2DB";
            this.btn_Save2DB.Size = new System.Drawing.Size(161, 37);
            this.btn_Save2DB.TabIndex = 6;
            this.btn_Save2DB.Text = "重置处理";
            this.btn_Save2DB.UseVisualStyleBackColor = true;
            this.btn_Save2DB.Click += new System.EventHandler(this.btn_Save2DB_Click);
            // 
            // menu_icon
            // 
            this.menu_icon.Font = new System.Drawing.Font("微软雅黑", 10.5F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.menu_icon.ImageScalingSize = new System.Drawing.Size(20, 20);
            this.menu_icon.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.btn_show,
            this.btn_closeme});
            this.menu_icon.Name = "menu_icon";
            this.menu_icon.Size = new System.Drawing.Size(188, 60);
            // 
            // btn_show
            // 
            this.btn_show.Image = ((System.Drawing.Image)(resources.GetObject("btn_show.Image")));
            this.btn_show.ImageScaling = System.Windows.Forms.ToolStripItemImageScaling.None;
            this.btn_show.Name = "btn_show";
            this.btn_show.Size = new System.Drawing.Size(187, 28);
            this.btn_show.Text = "显示主窗口";
            this.btn_show.Click += new System.EventHandler(this.btn_show_Click);
            // 
            // btn_closeme
            // 
            this.btn_closeme.Image = ((System.Drawing.Image)(resources.GetObject("btn_closeme.Image")));
            this.btn_closeme.ImageScaling = System.Windows.Forms.ToolStripItemImageScaling.None;
            this.btn_closeme.Name = "btn_closeme";
            this.btn_closeme.Size = new System.Drawing.Size(187, 28);
            this.btn_closeme.Text = "退出";
            this.btn_closeme.Click += new System.EventHandler(this.btn_closeme_Click);
            // 
            // notifyIcon1
            // 
            this.notifyIcon1.BalloonTipTitle = "Slac_MQ2File";
            this.notifyIcon1.ContextMenuStrip = this.menu_icon;
            this.notifyIcon1.Icon = ((System.Drawing.Icon)(resources.GetObject("notifyIcon1.Icon")));
            this.notifyIcon1.Text = "Slac_MQ2CH";
            this.notifyIcon1.Visible = true;
            this.notifyIcon1.DoubleClick += new System.EventHandler(this.notifyIcon1_DoubleClick);
            // 
            // checkBox1
            // 
            this.checkBox1.AutoSize = true;
            this.checkBox1.Enabled = false;
            this.checkBox1.Location = new System.Drawing.Point(179, 12);
            this.checkBox1.Name = "checkBox1";
            this.checkBox1.Size = new System.Drawing.Size(183, 21);
            this.checkBox1.TabIndex = 8;
            this.checkBox1.Text = "发送到远端消息队列";
            this.checkBox1.UseVisualStyleBackColor = true;
            this.checkBox1.Visible = false;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("宋体", 14.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.label1.Location = new System.Drawing.Point(-1, 334);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(185, 24);
            this.label1.TabIndex = 9;
            this.label1.Text = "数据消费处理：";
            // 
            // CheckBox_IsShowLog
            // 
            this.CheckBox_IsShowLog.AutoSize = true;
            this.CheckBox_IsShowLog.Checked = true;
            this.CheckBox_IsShowLog.CheckState = System.Windows.Forms.CheckState.Checked;
            this.CheckBox_IsShowLog.Font = new System.Drawing.Font("宋体", 14.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.CheckBox_IsShowLog.Location = new System.Drawing.Point(806, 26);
            this.CheckBox_IsShowLog.Name = "CheckBox_IsShowLog";
            this.CheckBox_IsShowLog.Size = new System.Drawing.Size(182, 28);
            this.CheckBox_IsShowLog.TabIndex = 10;
            this.CheckBox_IsShowLog.Text = "是否显示日志";
            this.CheckBox_IsShowLog.UseVisualStyleBackColor = true;
            // 
            // checkBox3
            // 
            this.checkBox3.AutoSize = true;
            this.checkBox3.Location = new System.Drawing.Point(633, 12);
            this.checkBox3.Name = "checkBox3";
            this.checkBox3.Size = new System.Drawing.Size(136, 21);
            this.checkBox3.TabIndex = 11;
            this.checkBox3.Text = "保存packetID";
            this.checkBox3.UseVisualStyleBackColor = true;
            this.checkBox3.Visible = false;
            // 
            // checkBox4
            // 
            this.checkBox4.AutoSize = true;
            this.checkBox4.Location = new System.Drawing.Point(367, 12);
            this.checkBox4.Name = "checkBox4";
            this.checkBox4.Size = new System.Drawing.Size(159, 21);
            this.checkBox4.TabIndex = 12;
            this.checkBox4.Text = "保存bin（调试）";
            this.checkBox4.UseVisualStyleBackColor = true;
            this.checkBox4.Visible = false;
            // 
            // checkBox5
            // 
            this.checkBox5.AutoSize = true;
            this.checkBox5.Checked = true;
            this.checkBox5.CheckState = System.Windows.Forms.CheckState.Checked;
            this.checkBox5.Location = new System.Drawing.Point(517, 12);
            this.checkBox5.Name = "checkBox5";
            this.checkBox5.Size = new System.Drawing.Size(132, 21);
            this.checkBox5.TabIndex = 13;
            this.checkBox5.Text = "显示异常日志";
            this.checkBox5.UseVisualStyleBackColor = true;
            this.checkBox5.Visible = false;
            // 
            // TextBox_Log3
            // 
            this.TextBox_Log3.Font = new System.Drawing.Font("宋体", 14.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.TextBox_Log3.Location = new System.Drawing.Point(3, 365);
            this.TextBox_Log3.Multiline = true;
            this.TextBox_Log3.Name = "TextBox_Log3";
            this.TextBox_Log3.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.TextBox_Log3.Size = new System.Drawing.Size(951, 198);
            this.TextBox_Log3.TabIndex = 14;
            // 
            // TextBox_Log1
            // 
            this.TextBox_Log1.Font = new System.Drawing.Font("宋体", 14.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.TextBox_Log1.Location = new System.Drawing.Point(3, 106);
            this.TextBox_Log1.Multiline = true;
            this.TextBox_Log1.Name = "TextBox_Log1";
            this.TextBox_Log1.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.TextBox_Log1.Size = new System.Drawing.Size(951, 219);
            this.TextBox_Log1.TabIndex = 15;
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Font = new System.Drawing.Font("宋体", 14.25F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(134)));
            this.label2.Location = new System.Drawing.Point(-1, 80);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(185, 24);
            this.label2.TabIndex = 16;
            this.label2.Text = "数据处理日志：";
            // 
            // frm_mq2ch_V2
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(9F, 17F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.SystemColors.GradientActiveCaption;
            this.ClientSize = new System.Drawing.Size(951, 575);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.TextBox_Log1);
            this.Controls.Add(this.TextBox_Log3);
            this.Controls.Add(this.checkBox5);
            this.Controls.Add(this.checkBox4);
            this.Controls.Add(this.checkBox3);
            this.Controls.Add(this.CheckBox_IsShowLog);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.checkBox1);
            this.Controls.Add(this.listBox1);
            this.Controls.Add(this.btn_Save2DB);
            this.Font = new System.Drawing.Font("宋体", 10F);
            this.Name = "frm_mq2ch_V2";
            this.Text = "SLAC数据采集系统2.4-解析";
            this.Load += new System.EventHandler(this.frm_mq2ch_Load);
            this.Resize += new System.EventHandler(this.frm_mq2ch_Resize);
            this.menu_icon.ResumeLayout(false);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.ListBox listBox1;
        private System.Windows.Forms.Button btn_Save2DB;
        private System.Windows.Forms.ContextMenuStrip menu_icon;
        private System.Windows.Forms.ToolStripMenuItem btn_show;
        private System.Windows.Forms.ToolStripMenuItem btn_closeme;
        private System.Windows.Forms.NotifyIcon notifyIcon1;
        private System.Windows.Forms.CheckBox checkBox1;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.CheckBox CheckBox_IsShowLog;
        private System.Windows.Forms.CheckBox checkBox3;
        private System.Windows.Forms.CheckBox checkBox4;
        private System.Windows.Forms.CheckBox checkBox5;
        private System.Windows.Forms.TextBox TextBox_Log3;
        private System.Windows.Forms.TextBox TextBox_Log1;
        private System.Windows.Forms.Label label2;
    }
}

