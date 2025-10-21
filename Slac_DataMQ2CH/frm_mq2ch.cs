using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Net;
using System.Messaging;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using SlacDataCollect;
using ServiceStack.Redis;



namespace Slac_DataMQ2CH
{
    /// <summary>
    /// 
    /// 用于将MQ队列的消息解析并存储到CH数据库
    /// </summary>
    public partial class frm_mq2ch : Form
    {

        public frm_mq2ch()
        {
            InitializeComponent();
        }

        private const int CP_NOCLOSE_BUTTON = 0x200;
        protected override CreateParams CreateParams
        {
            get
            {
                CreateParams myCp = base.CreateParams;
                myCp.ClassStyle = myCp.ClassStyle | CP_NOCLOSE_BUTTON;
                return myCp;
            }
        }

        
        private static bool isPause_save2db = true;
        private static string MQserver = ConfigHelper.GetAppConfig("MQ");
        private static string MQexchange = ConfigHelper.GetAppConfig("factory");
        private static string lineID = ConfigHelper.GetAppConfig("LineID");
        private static string CHserver = ConfigHelper.GetAppConfig("CHserver");
        private static string isCluster = ConfigHelper.GetAppConfig("isCluster");
        private static string dataVer = ConfigHelper.GetAppConfig("dataVer");
        private static string CHpwd = ConfigHelper.GetAppConfig("CHpwd");  //"slac1028#" ;// "slac1028";  
        private static string CHport = "8123";// ConfigHelper.GetAppConfig("CHport");
        private static string is2MQbak = ConfigHelper.GetAppConfig("is2MQbak");
        private static IConnection connection = null;
        private static IModel channel = null;
        private static ConnectionFactory factory = new ConnectionFactory() { HostName = MQserver, Port = 5672, UserName = MQexchange, Password = "slac1028", AutomaticRecoveryEnabled = true };
        private static IModel channel2 = null;
        private static IBasicProperties properties2 = null;
       
        private static StringBuilder SqlString = new StringBuilder("");
        private static StringBuilder LogSqlString = new StringBuilder("");
        private static string sqlhead = "insert into " + MQexchange + ".line" + lineID + " (eventtime,device_id,msg_id,data) values ";  //1001临湖,1002巴西
        private static string Logsqlhead = "insert into " + MQexchange + ".log" + lineID + " (rcv_time,send_time,device_id,packet_id,data) values ";  //1001临湖,1002巴西
        private static string packetID = "";
        private static string RcvTime = "";
        private static int ListCount = 0;
        private static int LogListCount = 0;
        private static int ListCountLimit = Convert.ToInt32(ConfigHelper.GetAppConfig("ListCount"));
        private static WebClient web = new WebClient();
        private static System.DateTime startTime = TimeZone.CurrentTimeZone.ToLocalTime(new System.DateTime(1970, 1, 1)); // 当地时区
        private static string QueueName = MQexchange + "_line" + lineID;// "slac_line1003";
        private static EventingBasicConsumer consumer;

        //--- 2024-06-09  Add by CWZ 发送给Redis
        private static string RdsIP = ConfigHelper.GetAppConfig("RdsIP");
        public DataTable dt_api_list = new DataTable();
        public RedisClient client = new RedisClient(RdsIP, 6379);  //"127.0.0.1"

        //-2024-06-18 发送到msmq-slacmsg
        private static string MQPath_boardMsg = @".\private$\slacmsg";
        private static MessageQueue myQueue_boardMsg;
        private static string is2MQmsg = ConfigHelper.GetAppConfig("is2MQmsg");
        private static string is2RDSmsg = ConfigHelper.GetAppConfig("is2RDSmsg");
        public DataTable dt_dashboard_list = new DataTable();
        private void frm_mq2ch_Load(object sender, EventArgs e)
        {
            isPause_save2db = false;
            btn_Save2DB.Text = "停止数据解析";
            listBox1.Items.Add("开始数据解析 @ " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            SqlString = new StringBuilder(sqlhead);
            LogSqlString = new StringBuilder(Logsqlhead);
            connection = factory.CreateConnection();
            channel = connection.CreateModel();
            //每个消费者最多消费一条消息,没返回消息确认之前不再接收消息
            channel.BasicQos(0, 1, false);
            channel.QueueDeclare(QueueName, true, false, false, null);

           

            channel2 = connection.CreateModel();
            properties2 = channel2.CreateBasicProperties();
            properties2.DeliveryMode = 2;//持久化
                                         //properties2.Expiration = (3 * 24 * 3600 * 1000).ToString();  //设置消息的TTL生命周期:ms   3 * 24 * 3600 * 1000

          

            this.Invoke(new MethodInvoker(Run_ReciveInfo));

            this.Text = "SLAC数据采集系统2.1-解析:" + MQexchange + "_Line"+ lineID;
            this.notifyIcon1.Text = "Slac_DataMQ2CH: " + MQexchange + "_Line" + lineID;

            if (isCluster == "1")
            {
                CHpwd = "slac1028#";
                CHport = "10280";
            }
            if(is2MQbak=="1")
            {
                checkBox1.Checked = true;
            }

           

            try
            {
                dt_dashboard_list = ConfigSQL.Get_dashboard_list();
                dt_api_list = ConfigSQL.Get_data2api_list();
            }
            catch (Exception ex)
            {
                System.IO.File.AppendAllText("output\\" + DateTime.Now.ToString("yyyyMMdd_") + ".log", "Error:" + ex.ToString() + " @ " + DateTime.Now.ToString() + "\r\n");
                listBox1.Items.Add("Error:" + ex.ToString() + "@" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));
            }

            try
            {
                client.Password = "slac1028";//密码 
                client.Db = 0; //选择第1个数据库，0-15
               
            }
            catch (Exception ex)
            {
                System.IO.File.AppendAllText("output\\" + DateTime.Now.ToString("yyyyMMdd_") + ".log", "Error:" + ex.ToString() + " @ " + DateTime.Now.ToString() + "\r\n");
                listBox1.Items.Add("Error:" + ex.ToString() + "@" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));
            }


            if (MessageQueue.Exists(MQPath_boardMsg))
            {
                myQueue_boardMsg = new MessageQueue(MQPath_boardMsg);
            }
            else
            {
                myQueue_boardMsg = MessageQueue.Create(MQPath_boardMsg);
            }
            
        }

        private void frm_mq2ch_Resize(object sender, EventArgs e)
        {
            if (this.WindowState == FormWindowState.Minimized)
            {
                // notifyIcon1.Visible = true;  
                this.Hide();
            }
        }

        private void btn_Save2DB_Click(object sender, EventArgs e)
        {
            if (isPause_save2db)
            {
                isPause_save2db = false;
                btn_Save2DB.Text = "停止数据解析";
                if (listBox1.Items.Count > 30)
                {
                    listBox1.Items.RemoveAt(0);
                }
                listBox1.Items.Add("开始数据解析 @ " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            }
            else
            {
                isPause_save2db = true;
                btn_Save2DB.Text = "开始数据解析";
                if (listBox1.Items.Count > 30)
                {
                    listBox1.Items.RemoveAt(0);
                }
                listBox1.Items.Add("停止数据解析 @ " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                channel.BasicCancel(consumer.ConsumerTag);
                Thread.Sleep(1000);
                if (ListCount > 0)
                {
                    this.Invoke((MethodInvoker)delegate
                    {
                        saveToCK(RcvTime);
                    });
                }
                if (LogListCount > 0)
                {
                    this.Invoke((MethodInvoker)delegate
                    {
                        saveLogToCK(RcvTime);
                    });
                }
            }
        }

        private void notifyIcon1_DoubleClick(object sender, EventArgs e)
        {
            this.Show(); // 窗体显现
            this.WindowState = FormWindowState.Normal; //窗体回复正常大小
        }

        private void btn_show_Click(object sender, EventArgs e)
        {
            this.Show();
            this.WindowState = FormWindowState.Normal;
        }

        private void btn_closeme_Click(object sender, EventArgs e)
        {
            isPause_save2db = true;
            btn_Save2DB.Text = "开始数据解析";
            if (listBox1.Items.Count > 30)
            {
                listBox1.Items.RemoveAt(0);
            }
            listBox1.Items.Add("暂停数据解析 @ " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));
            Thread.Sleep(500);
            if (ListCount >= 0)
            {
                this.Invoke((MethodInvoker)delegate
                {
                    saveToCK(RcvTime);
                });
            }
            if (LogListCount > 0)
            {
                this.Invoke((MethodInvoker)delegate
                {
                    saveLogToCK(RcvTime);
                });
            }
            channel.Close();
            connection.Close();
            notifyIcon1.Visible = false;
            System.Environment.Exit(0);
        }



        private string PostResponse(string user, string password, string url, string postData)
        {
            AuthenticationHeaderValue authentication = new AuthenticationHeaderValue(
               "Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{user}:{password}")
               ));
            string result = string.Empty;
            HttpContent httpContent = new StringContent(postData);
            httpContent.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
            httpContent.Headers.ContentType.CharSet = "utf-8";
            using (HttpClient httpClient = new HttpClient())
            {
                try
                {
                    httpClient.DefaultRequestHeaders.Authorization = authentication;
                    HttpResponseMessage response = httpClient.PostAsync(url, httpContent).Result;
                    if (response.IsSuccessStatusCode)
                    {
                        Task<string> t = response.Content.ReadAsStringAsync();
                        result = t.Result;
                    }
                }
                catch (Exception ex)
                {
                    System.IO.File.AppendAllText("output\\" + DateTime.Now.ToString("yyyyMMdd_") + ".log", ex.ToString() + "\r\n");
                }
            }
            return result;
        }

        private void saveToCK(string RcvTime)
        {
            try
            {

                //web.Headers.Add("Content-Type", "application/json");
                StringBuilder ssql = SqlString.Remove(SqlString.Length - 1, 1);
                byte[] postData = Encoding.ASCII.GetBytes(ssql.ToString());
                string postUrl = "";
                if (CHport == "8123")
                {
                    postUrl = $"http://{CHserver}:8123/";
                }
                else
                {
                    int days2port = 10281+ Convert.ToDateTime(RcvTime).Day % 3;
                    postUrl = $"http://{CHserver}:{days2port.ToString()}";
                }
                var strResult = PostResponse("default", CHpwd, postUrl, ssql.ToString());
                // 上传数据
               // byte[] responseData = web.UploadData("http://" + CHserver + ":8123/", "POST", postData);
                //获取返回的二进制数据.
                // string strResult = Encoding.UTF8.GetString(responseData);
                if (string.IsNullOrEmpty(strResult))
                {
                    SqlString = new StringBuilder(sqlhead);
                    ListCount = 0;
                }
                Thread.Sleep(500);
            }
            catch (Exception ex)
            {
                if (listBox1.Items.Count > 30)
                {
                    listBox1.Items.RemoveAt(0);
                }
                listBox1.Items.Add("保存数据库失败！请检查网线是否连接正常...");
                System.IO.File.AppendAllText("output\\" + DateTime.Now.ToString("yyyyMMdd_") + ".log", ex.ToString() + "\r\n");
                return;
            }

        }


        private void saveLogToCK(string RcvTime)
        {
            try
            {

                //web.Headers.Add("Content-Type", "application/json");
                StringBuilder Logssql = LogSqlString.Remove(LogSqlString.Length - 1, 1);
                byte[] postData = Encoding.ASCII.GetBytes(Logssql.ToString());
                string postUrl = "";
                if (CHport == "8123")
                {
                    postUrl = $"http://{CHserver}:8123/";
                }
                else
                {
                    int days2port = 10281 + Convert.ToDateTime(RcvTime).Day % 3;
                    postUrl = $"http://{CHserver}:{days2port.ToString()}";
                }
                var strResult = PostResponse("default", CHpwd, postUrl, Logssql.ToString());
                // 上传数据
                // byte[] responseData = web.UploadData("http://" + CHserver + ":8123/", "POST", postData);
                //获取返回的二进制数据.
                // string strResult = Encoding.UTF8.GetString(responseData);
                if (string.IsNullOrEmpty(strResult))
                {
                    LogSqlString = new StringBuilder(Logsqlhead);
                    LogListCount = 0;
                }
                Thread.Sleep(500);
            }
            catch (Exception ex)
            {
                if (listBox1.Items.Count > 30)
                {
                    listBox1.Items.RemoveAt(0);
                }
                listBox1.Items.Add("Log保存数据库失败！请检查网线是否连接正常...");
                System.IO.File.AppendAllText("output\\" + DateTime.Now.ToString("yyyyMMdd_") + ".log", ex.ToString() + "\r\n");
                return;
            }

        }

        private void Run_ReciveInfo()
        {
            string nowdevice = "";
            string nowmsg = "";

            try
            {
                /* 这里定义了一个消费者，用于消费服务器接受的消息
                 * C#开发需要注意下这里，在一些非面向对象和面向对象比较差的语言中，是非常重视这种设计模式的。
                 * 比如RabbitMQ使用了生产者与消费者模式，然后很多相关的使用文章都在拿这个生产者和消费者来表述。
                 * 但是，在C#里，生产者与消费者对我们而言，根本算不上一种设计模式，他就是一种最基础的代码编写规则。
                 * 所以，大家不要复杂的名词吓到，其实，并没那么复杂。
                 * 这里，其实就是定义一个EventingBasicConsumer类型的对象，然后该对象有个Received事件，该事件会在服务接收到数据时触发。
                 */
                //消费者 
                consumer = new EventingBasicConsumer(channel);
                //消费消息 autoAck参数为消费后是否删除
                channel.BasicConsume(QueueName, false, consumer);   //true:自动删除  false: 不自动删除
                //自动接收消息事件：被动读取事件，只要客户端推送消息 ，这边被动接收 
                consumer.Received += (model, ea) =>
                {
                    StringBuilder errorLog = new StringBuilder();//日志记录
                    try
                    {
                        
                        if (!isPause_save2db)
                        {
                            var MQmsg = ea.Body;
                            byte[] bufferMQ = (byte[])MQmsg;

                            byte[] byte_rcvtime = new byte[8];
                            byte[] buffer_all = new byte[bufferMQ.Length - 8];

                            try
                            {
                                Buffer.BlockCopy(bufferMQ, 0, byte_rcvtime, 0, 8);
                                Buffer.BlockCopy(bufferMQ, 8, buffer_all, 0, bufferMQ.Length - 8);
                            }
                            catch (Exception ex)
                            {
                                errorLog.AppendLine($"{DateTime.Now}@Error1:{ex.ToString()}");
                                throw ex;
                            }
                            byte[] byte_head = new byte[16];

                            try
                            {
                                Buffer.BlockCopy(buffer_all, 0, byte_head, 0, 16);
                            }
                            catch (Exception ex)
                            {
                                errorLog.AppendLine($"{DateTime.Now}@Error2:{ex.ToString()}");
                                throw ex;
                            }
                            //依次是0通讯版本、1通讯码、2机器、3发送时、4分、5秒、6毫秒 、7包ID、8传输包序号、9传输包大小, 10传输包数量
                            int[] packet_head_list = dataVer == "1" ? SlacPlcDataV1.packet_head(byte_head) : SlacPlcDataV0.packet_head(byte_head);
                            byte[] byte_data = new byte[packet_head_list[9]]; //获取传输包大小 2021-2-3不再减去头大小-16
                            double rcv_timestamp = BitConverter.ToDouble(byte_rcvtime, 0);

                            try
                            {
                                Buffer.BlockCopy(buffer_all, 16, byte_data, 0, packet_head_list[9]); //2021-2-3不再减去头大小 -16
                            }
                            catch (Exception ex)
                            {
                                errorLog.AppendLine($"{DateTime.Now}@Error3:数据包标记大小与实际不匹配！标记大小：{packet_head_list[9]}-实际大小：{bufferMQ.Length},异常信息：{ex.ToString()}");

                                if (checkBox4.Checked)
                                {
                                    string exepath = System.Reflection.Assembly.GetExecutingAssembly().Location;
                                    string FilePath = System.IO.Path.GetDirectoryName(exepath);
                                    string sPath = FilePath + @"\" + DateTime.Now.ToString("yyyyMMdd");
                                    if (!Directory.Exists(sPath))
                                    {
                                        Directory.CreateDirectory(sPath);
                                    }
                                    string fileName = sPath + @"\" + lineID + "_" + DateTime.Now.ToString("yyyyMMdd_HHmmss_") + rcv_timestamp.ToString() + ".sbin";
                                    bool saveRet = FileHelper.ByteToFile(bufferMQ, fileName);
                                }
                                throw ex;
                            }

                            try
                            {
                                DateTime time_Plc_Receive = startTime.AddMilliseconds(rcv_timestamp);
                                DateTime time_Plc_send = time_Plc_Receive.Date.AddHours(packet_head_list[3]).AddMinutes(packet_head_list[4]).AddSeconds(packet_head_list[5]).AddMilliseconds(packet_head_list[6]);


                                int[] int_datalist = dataVer == "1" ? SlacPlcDataV1.SetByte2Int(byte_data) : SlacPlcDataV0.SetByte2Int(byte_data);
                                packetID = packet_head_list[8].ToString();
                                RcvTime = time_Plc_Receive.ToString("yyyy-MM-dd HH:mm:ss.fff");

                                int logNumCount = ListCount;

                                for (int i = 0; i < int_datalist.Count(); i++)
                                {
                                    int packet_id = dataVer == "1" ? SlacPlcDataV1.GetPacket_id(int_datalist[i]) : SlacPlcDataV0.GetPacket_id(int_datalist[i]); // >> 29 & 7;

                                    if (packet_id == 0) //000普通数据 
                                    {
                                        int[] datahead = dataVer == "1" ? SlacPlcDataV1.data_head(int_datalist[i]) : SlacPlcDataV0.data_head(int_datalist[i]);  //依次是：包ID、信息ID、时、分、秒
                                        DateTime time_data = time_Plc_Receive.Date.AddHours(datahead[2]).AddMinutes(datahead[3]).AddSeconds(datahead[4]);

                                        //double plctime_delta = (time_Plc_send.TimeOfDay - time_data.TimeOfDay).TotalMilliseconds;
                                        //if (plctime_delta < -43200000)
                                        //{
                                        //    plctime_delta = plctime_delta + 86400000;
                                        //}
                                        // 解决跨天问题
                                        double plctime_delta = (DateTime.Now - time_data).TotalMilliseconds;
                                        if (plctime_delta < -43200000)
                                        {
                                            plctime_delta = -86400000;
                                            time_data = time_data.AddMilliseconds(plctime_delta);
                                        }

                                        int msg_id = datahead[1];
                                        nowdevice = packet_head_list[2].ToString();
                                        nowmsg = msg_id.ToString();

                                        int data_value = int_datalist[i + 1];
                                        int enDatavalue = dataVer == "1" ? SlacPlcDataV1.GetEnIntV1(data_value, packet_head_list[2], msg_id) : SlacPlcDataV0.GetEnIntV1(data_value, packet_head_list[2], msg_id);

                                        //2024-06-24 cwz 改为保存PLC原始时间，与接收时间不再关联
                                        // SqlString.Append("('" + time_Plc_Receive.AddMilliseconds(-plctime_delta).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.fff") + "','" + packet_head_list[2].ToString() + "','" + msg_id.ToString() + "','" + enDatavalue.ToString() + "'),");
                                        SqlString.Append("('" + time_data.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.fff") + "','" + packet_head_list[2].ToString() + "','" + msg_id.ToString() + "','" + enDatavalue.ToString() + "'),");

                                        CheckAndSend2MQ(nowdevice, msg_id.ToString(), data_value);
                                        ListCount++;
                                        i++;

                                    }
                                    else if (packet_id == 1) //001高速数据
                                    {
                                        int[] datahead = dataVer == "1" ? SlacPlcDataV1.data_head(int_datalist[i]) : SlacPlcDataV0.data_head(int_datalist[i]);  //依次是：包ID、信息ID、时、分、秒
                                        DateTime time_data = time_Plc_Receive.Date.AddHours(datahead[2]).AddMinutes(datahead[3]).AddSeconds(datahead[4]);
                                        //double plctime_delta = (time_Plc_send.TimeOfDay - time_data.TimeOfDay).TotalMilliseconds;
                                        //if (plctime_delta < -43200000)
                                        //{
                                        //    plctime_delta = plctime_delta + 86400000;
                                        //}
                                        // 解决跨天问题
                                        double plctime_delta = (DateTime.Now - time_data).TotalMilliseconds;
                                        if (plctime_delta < -43200000)
                                        {
                                            plctime_delta = -86400000;
                                            time_data = time_data.AddMilliseconds(plctime_delta);
                                        }

                                        int[] datahead_highspeed2 = dataVer == "1" ? SlacPlcDataV1.data_head_highspeed2(int_datalist[i + 1]) : SlacPlcDataV0.data_head_highspeed2(int_datalist[i + 1]);
                                        //依次是：数据量、采样周期毫秒、起始时间毫秒
                                        int data_lenth_hspeed = datahead_highspeed2[0]; //数据量---32位数值个数
                                        int milli_second = datahead_highspeed2[1]; //采样周期毫秒
                                        int start_milli_second = datahead_highspeed2[2]; //起始时间毫秒

                                        nowdevice = packet_head_list[2].ToString();
                                        nowmsg = datahead[1].ToString();

                                        int[] datahead_highspeed3 = dataVer == "1" ? SlacPlcDataV1.data_head_highspeed3(int_datalist[i + 2]) : SlacPlcDataV0.data_head_highspeed3(int_datalist[i + 2]);
                                        //数据大小、功能3bit
                                        int data_type = datahead_highspeed3[0];  //000-bit  001-8bit  010-16bit  011-32bit
                                        int highspeed_type = datahead_highspeed3[1];//功能 

                                        //2020-12-31不按浮点数存储，存成int，用的时候再转为浮点数
                                        for (int m = 0; m < data_lenth_hspeed; m++)
                                        {
                                            int add_millisecond = start_milli_second;
                                            int msgID = datahead[1];
                                            int data_value = Convert.ToInt32(int_datalist[i + 3 + m]);

                                            if (data_type == 0)       //00--1bit
                                            {
                                                data_value = Convert.ToInt32(int_datalist[i + 3 + m]);
                                            }
                                            else if (data_type == 1)  //01--8bit
                                            {
                                                data_value = Convert.ToInt32(int_datalist[i + 3 + m]);
                                            }
                                            else if (data_type == 2)  //10--16bit
                                            {
                                                data_value = Convert.ToInt32(int_datalist[i + 3 + m]);
                                            }
                                            else if (data_type == 3)  //11--32bit
                                            {
                                                data_value = Convert.ToInt32(int_datalist[i + 3 + m]);
                                            }

                                            if (highspeed_type == 0) //功能=000： 同一个ID采集多条记录
                                            {
                                                msgID = datahead[1];
                                                add_millisecond = start_milli_second + milli_second * m; //此处如果怕因为时间相同覆盖数据的话，可以+m把时间区分开
                                            }
                                            else if (highspeed_type == 1) //功能=001：ID递增，每条ID一行记录
                                            {
                                                msgID = datahead[1] + m;
                                                add_millisecond = start_milli_second; //此处如果怕因为时间相同覆盖数据的话，可以+m把时间区分开
                                            }

                                            int enDatavalue = dataVer == "1" ? SlacPlcDataV1.GetEnIntV1(data_value, packet_head_list[2], msgID) : SlacPlcDataV0.GetEnIntV1(data_value, packet_head_list[2], msgID);
                                            //SqlString.Append("('" + time_Plc_Receive.AddMilliseconds(add_millisecond - plctime_delta).AddTicks(m * 2).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.fff") + "','" + packet_head_list[2].ToString() + "','" + msgID.ToString() + "','" + enDatavalue.ToString() + "'),");
                                            //2024-06-24 cwz 改为保存PLC原始时间，与接收时间不再关联
                                            SqlString.Append("('" + time_data.AddMilliseconds(add_millisecond).AddTicks(m * 2).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.fff") + "','" + packet_head_list[2].ToString() + "','" + msgID.ToString() + "','" + enDatavalue.ToString() + "'),");

                                            CheckAndSend2MQ(nowdevice, msgID.ToString(), data_value);
                                            ListCount++;
                                        }
                                        i = i + 2 + data_lenth_hspeed;

                                    }
                                    else if (packet_id == 2)  //010事件触发
                                    {
                                        int[] datahead = dataVer == "1" ? SlacPlcDataV1.data_head(int_datalist[i]) : SlacPlcDataV0.data_head(int_datalist[i]);  //依次是：包ID、信息ID、时、分、秒
                                        DateTime time_data = time_Plc_Receive.Date.AddHours(datahead[2]).AddMinutes(datahead[3]).AddSeconds(datahead[4]);
                                        //double plctime_delta = (time_Plc_send.TimeOfDay - time_data.TimeOfDay).TotalMilliseconds;
                                        //if (plctime_delta < -43200000)
                                        //{
                                        //    plctime_delta = plctime_delta + 86400000;
                                        //}
                                        // 解决跨天问题
                                        double plctime_delta = (DateTime.Now - time_data).TotalMilliseconds;
                                        if (plctime_delta < -43200000)
                                        {
                                            plctime_delta = -86400000;
                                            time_data = time_data.AddMilliseconds(plctime_delta);
                                        }

                                        int msg_id = datahead[1];
                                        nowdevice = packet_head_list[2].ToString();
                                        nowmsg = msg_id.ToString();
                                        int data_value = int_datalist[i + 1];
                                        int enDatavalue = dataVer == "1" ? SlacPlcDataV1.GetEnIntV1(data_value, packet_head_list[2], msg_id) : SlacPlcDataV0.GetEnIntV1(data_value, packet_head_list[2], msg_id);
                                        //SqlString.Append("('" + time_Plc_Receive.AddMilliseconds(-plctime_delta).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.fff") + "','" + packet_head_list[2].ToString() + "','" + msg_id.ToString() + "','" + enDatavalue.ToString() + "'),");
                                        //2024-06-24 cwz 改为保存PLC原始时间，与接收时间不再关联
                                        SqlString.Append("('" + time_data.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.fff") + "','" + packet_head_list[2].ToString() + "','" + msg_id.ToString() + "','" + enDatavalue.ToString() + "'),");

                                        CheckAndSend2MQ(nowdevice, msg_id.ToString(), data_value);
                                        ListCount++;
                                        i++;


                                    }
                                }

                                logNumCount = ListCount - logNumCount;
                                //System.IO.File.AppendAllText("output\\packetID_" + DateTime.Now.ToString("yyyyMMdd_") + packet_head_list[2].ToString() + ".log", packetID + " @ " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + "\r\n");
                                LogSqlString.Append("('" + time_Plc_Receive.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.fff") + "','" + time_Plc_send.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.fff") + "','" + packet_head_list[2].ToString() + "','" + packetID.ToString() + "','" + logNumCount.ToString() + "'),");
                                LogListCount++;



                                if (ListCount >= ListCountLimit)
                                {
                                    this.Invoke((MethodInvoker)delegate
                                    {
                                        saveToCK(RcvTime);
                                    });

                                    RichText_InputText($"保存数据库： {"  Total: " + ListCountLimit + " rows @   " + time_Plc_Receive}");
                                }


                                if (LogListCount >= ListCountLimit / 10)
                                {
                                    this.Invoke((MethodInvoker)delegate
                                    {
                                        saveLogToCK(RcvTime);
                                    });

                                    RichText_InputText($"Log保存数据库： {"  Total: " + ListCountLimit / 10 + " rows @   " + time_Plc_Receive}");
                                }

                                //--保存到 bak队列里面
                                if (is2MQbak == "1" && logNumCount > 0)
                                {
                                    SendtoMQ_2(MQexchange, "line" + lineID + "_bak", ea.Body);
                                }
                                //手动向rabbitmq发送消息确认
                                //当294行为false时需要手动ack才能删除消息  //前文的这里： channel.BasicConsume(QueueName, false, consumer); 
                                channel.BasicAck(ea.DeliveryTag, false);
                                // RichText_InputText($"Received： {packet_head_list[8].ToString()+"  @   "+time_Plc_Receive}");
                            }
                            catch (Exception ex)
                            {
                                errorLog.AppendLine($"{DateTime.Now}@Error3:RabbitMq数据解析转换异常:{ex.ToString()}");
                                throw ex;
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        RichText_InputText(errorLog.ToString());
                        System.IO.File.AppendAllText("output\\" + DateTime.Now.ToString("yyyyMMdd_") + ".log", $"====RabbitMQ received event Error Begin======{Environment.NewLine}{errorLog.ToString()}{Environment.NewLine}====RabbitMQ received event Error End======{Environment.NewLine}");
                        errorLog.Clear();
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);



                    }
                };

                //读取方式2:主动读取一次消息 ，不会定时取
                //BasicGetResult res = channel.BasicGet(queue, false/*noAck*/);
                //if (res != null)
                //{
                //    RichText_InputText("22222--" + System.Text.UTF8Encoding.UTF8.GetString(res.Body.ToArray()));
                //    ch.BasicAck(res.DeliveryTag, false);

                //}
                //else
                //{
                //    RichText_InputText("22222--[external-request-queue] no data");
                //}
                //处理时间 
            }
            catch (Exception ex)
            {
                System.IO.File.AppendAllText("output\\" + DateTime.Now.ToString("yyyyMMdd_") + ".log", $"{DateTime.Now}@Error:{ex.ToString()}{Environment.NewLine}");
                RichText_InputText($"{DateTime.Now}@Error:{ex.ToString()}");
            }
        }

        protected void RichText_InputText(string msg)
        {
            this.Invoke((MethodInvoker)delegate
                {
                    if (listBox1.Items.Count > 30)
                    {
                        listBox1.Items.RemoveAt(0);
                    }
                    listBox1.Items.Add(msg);
                }
                );
        }


        private void CheckAndSend2MQ(string from_device_id, string msg_id, Int32 data)
        {
            try
            {
                if (is2MQmsg == "1")
                {
                    string SqlStr = " from_device_id='" + from_device_id + "' and msg_id='" + msg_id + "'";
                    DataRow[] dr_list = dt_dashboard_list.Select(SqlStr);
                    for (int i = 0; i < dr_list.Count(); i++)
                    {
                        DataRow dr = dr_list[i];
                        int bit_id = Convert.ToInt32(dr["bit_id"]);
                        int bit_data = Convert.ToInt32(dr["bit_data"]);
                        int state_id = Convert.ToInt32(dr["state_id"]);
                        string device_id = dr["device_id"].ToString();
                        int state_type = (data >> bit_id) & 1;
                        if (state_type == bit_data)
                        {
                            myQueue_boardMsg = new MessageQueue(MQPath_boardMsg);
                            myQueue_boardMsg.Formatter = new BinaryMessageFormatter();
                            myQueue_boardMsg.DefaultPropertiesToSend.Recoverable = true;
                            var str = "{\"device_id\":\"" + device_id + "\",\"msg_id\":\"" + msg_id + "." + bit_id.ToString() + "\",\"data\":\"" + state_type.ToString() + "\",\"state_id\":\"" + state_id.ToString() + "\"}";
                            var chararr = Encoding.Default.GetBytes(str);
                            myQueue_boardMsg.Send(chararr, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                        }
                    }
                }

                if (is2RDSmsg == "1")
                {
                    string SqlStr_api = " from_device_id=" + from_device_id + " and msg_id=" + msg_id + " and line_id='" + lineID + "'";
                    DataRow[] dr_apilist = dt_api_list.Select(SqlStr_api);
                    for (int j = 0; j < dr_apilist.Count(); j++)
                    {
                        DataRow dr = dr_apilist[j];
                        string device_id = dr["device_id"].ToString();
                        int bit_id = Convert.ToInt32(dr["bit_id"]);
                        int isbitdata = Convert.ToInt32(dr["isbitdata"]);
                        if (isbitdata == 0)
                        {
                            client.Set("d" + lineID + "." + device_id + "." + msg_id, data);

                        }
                        else
                        {
                            int bitdata = (data >> bit_id) & 1;
                            client.Set("d" + lineID + "." + device_id + "." + msg_id + "." + bit_id, bitdata);
                        }

                    }
                }
            }
            catch (Exception ex)
            {
                System.IO.File.AppendAllText("output\\" + DateTime.Now.ToString("yyyyMMdd_") + ".log", from_device_id + "--" + msg_id + "--" + ex.ToString() + " @ " + DateTime.Now.ToString() + "\r\n");
            }
        }


        private string SendtoMQ_2(string MQ_exchange, string router, byte[] SendMsg)
        {

            try
            {
                if (connection.IsOpen)
                {
                    channel2.BasicPublish(MQ_exchange, router, properties2, SendMsg);
                    if (checkBox2.Checked)
                    {
                        label1.Text = "传输成功！ @ " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
                    }
                    return "0";

                }
                else
                {
                    label1.Text = "传输失败！ @ " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
                    return "error:100";
                }

            }
            catch (Exception ex)
            {
                label1.Text = "异常" + ex.ToString() + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
                return "error:101";
            }

        }

       
    }
}
