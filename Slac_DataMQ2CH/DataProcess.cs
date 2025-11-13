/***********************************************************************
*
* Copyright (C) 2010-2025, SLAC Co.,Ltd.,All Rights Reserved.
*
************************************************************************
* CLR Version： 4.0.30319.42000
* FileName:DataProcess
* Author：02899<张志杰>
* E-mail:zhangzhijie@slac.com.cn
* Create Date: 2025-10-16 11:04:59
* Description: RabbitMQ数据消费处理
*
*-----------------------------------------------------------------------
* Modifier：
* Modify Date:
* Description:
*
************************************************************************/

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ServiceStack.Messaging;
using Slac_DataMQ2File;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data.Entity.Core.Common.CommandTrees.ExpressionBuilder;
using System.Diagnostics.Eventing.Reader;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Slac_DataMQ2CH
{
    public class DataProcess
    {
        #region 字段、参数和事件
        public event Action<string, int> EventReceiveCount; // 接收处理数据包计数器事件

        DataComm dataComm = new DataComm();

        private static readonly string DeviceIP = ConfigHelper.GetAppConfig("DeviceIP");     //设备IP
        private static readonly string DevicePort = ConfigHelper.GetAppConfig("DevicePort"); //设备端口

        private static readonly string MQserver = ConfigHelper.GetAppConfig("MQ");
        private static readonly string MQexchange = ConfigHelper.GetAppConfig("factory");
        private static readonly string lineID = ConfigHelper.GetAppConfig("LineID");

        private static readonly string QueueName = MQexchange + "_line" + lineID;// "slac_line1003";

        private static IConnection connection = null;  // RabbitMQ连接
        private static IModel channel = null;          // RabbitMQ通道
        private static EventingBasicConsumer consumer; // RabbitMQ消费者

        private static ConnectionFactory factory = new ConnectionFactory() { HostName = MQserver, Port = 5672, UserName = MQexchange, Password = "slac1028", AutomaticRecoveryEnabled = true };

        private static System.DateTime startTime = TimeZone.CurrentTimeZone.ToLocalTime(new System.DateTime(1970, 1, 1)); // 当地时区

        private int _lastdataNum;  // 上一个数据序号

        private byte[] timeByte = new byte[4]; // 时间字节

        #endregion

        #region 参数

        private int _lastLoopSign11; // 脉冲通道1循环标志
        private int _lastLoopSign12; // 脉冲通道2循环标志

        private int _lastLoopSign21; // 采样通道1循环标志     
        private int _lastLoopSign22; // 采样通道2循环标志     
        private int _lastLoopSign23; // 采样通道3循环标志     
        private int _lastLoopSign24; // 采样通道4循环标志     


        // 当前接收缓存区，用于存放未处理的数据包
        private List<DataPacket> _buffer11 = new List<DataPacket>(); // 脉冲通道1缓存区
        private List<DataPacket> _buffer12 = new List<DataPacket>(); // 脉冲通道2缓存区

        private List<DataPacket> _buffer21 = new List<DataPacket>(); // 采样通道1缓存区
        private List<DataPacket> _buffer22 = new List<DataPacket>(); // 采样通道2缓存区
        private List<DataPacket> _buffer23 = new List<DataPacket>(); // 采样通道3缓存区
        private List<DataPacket> _buffer24 = new List<DataPacket>(); // 采样通道4缓存区

        /// <summary>
        /// 当前是否强制立即处理标志位
        /// </summary>
        public bool ForceProcess11 { get; set; } = false; // 脉冲通道1强制处理标志位
        public bool ForceProcess12 { get; set; } = false; // 脉冲通道2强制处理标志位

        public bool ForceProcess21 { get; set; } = false; // 采样通道1强制处理标志位
        public bool ForceProcess22 { get; set; } = false; // 采样通道2强制处理标志位
        public bool ForceProcess23 { get; set; } = false; // 采样通道3强制处理标志位
        public bool ForceProcess24 { get; set; } = false; // 采样通道4强制处理标志位

        #endregion

        private bool ReceiveState = false; // 接收状态

        public DataProcess()
        {
            frm_mq2ch_V2.EventReset += () =>
            {
                if (!ReceiveState)
                {
                    _buffer11.Clear();
                    _buffer12.Clear();

                    _buffer21.Clear();
                    _buffer22.Clear();
                    _buffer23.Clear();
                    _buffer24.Clear();
                }

            };

            #region 初始化RabbitMQ连接

            connection = factory.CreateConnection();
            channel = connection.CreateModel();
            //每个消费者最多消费一条消息,没返回消息确认之前不再接收消息
            channel.BasicQos(0, 1, false);
            channel.QueueDeclare(QueueName, true, false, false, null);

            #endregion
        }


        public void Run_ReciveInfo()
        {

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
                    ReceiveState = true;
                    try
                    {
                        var MQmsg = ea.Body;
                        byte[] bufferMQ = (byte[])MQmsg;

                        byte[] byte_rcvtime = new byte[8];                 // 前八个字节为数据接收时间（接收到添加进去）
                        byte[] buffer_all = new byte[bufferMQ.Length - 8]; // 数据体

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

                        // 接收时间处理（保存为四个字节）
                        double rcv_timestamp = BitConverter.ToDouble(byte_rcvtime, 0);        //接收时间戳
                        DateTime time_Plc_Receive = startTime.AddMilliseconds(rcv_timestamp); // 根据接收时间戳及时区，得出接收PLC数据的具体时间

                        DateTime today = time_Plc_Receive.Date;
                        uint msOfDay = (uint)(time_Plc_Receive - today).TotalMilliseconds;
                        byte[] timeReceiveBytes = BitConverter.GetBytes(msOfDay); // 将当天日期的时分秒转换为毫秒数（四个字节）

                        // 数据体处理
                        try
                        {
                            int buffer_length = buffer_all.Length;

                            // 基本长度校验（至少要包含 2字节帧头 + 1字节命令 + 2字节帧尾 = 5字节）
                            if (buffer_all.Length < 5)
                            {
                                channel.BasicAck(ea.DeliveryTag, false);
                                //Console.WriteLine($"来自 {ipstr} 的数据长度不足，无法解析。");
                                return;
                            }

                            // 校验帧头是否为 7B 7B
                            bool headMatch = buffer_all[0] == 0x7B && buffer_all[1] == 0x7B;
                            // 校验帧尾是否为 7D 7D
                            bool tailMatch = buffer_all[buffer_length - 2] == 0x7D && buffer_all[buffer_length - 1] == 0x7D;

                            if (!headMatch || !tailMatch)
                            {
                                channel.BasicAck(ea.DeliveryTag, false);
                                // Console.WriteLine($"来自 {ipstr} 的数据帧不合法（头或尾不匹配）");
                                return;
                            }

                            // 提取数据类型字节（第3个字节，下标为2）
                            byte command = buffer_all[2];

                            // 打印数据内容（调试用）
                            string hex = BitConverter.ToString(buffer_all, 0, buffer_length).Replace("-", " ");
                            //Console.WriteLine($" 接收来自 {ipstr} 的数据: {hex}，数据类型字节={command:X2}");

                            // 根据命令字执行不同逻辑
                            switch (command)
                            {
                                case 0x00:
                                    HandleCommand00(buffer_all);
                                    break;

                                case 0x01:
                                    HandleCommand01(buffer_all);
                                    break;

                                case 0x02:
                                    HandleCommand02(buffer_all.ToArray(), timeReceiveBytes.ToArray());
                                    break;

                                case 0x03:
                                    HandleCommand03(buffer_all, timeReceiveBytes);
                                    break;

                                default:
                                    Console.WriteLine($" 未知命令字: {command:X2}");
                                    break;
                            }

                            channel.BasicAck(ea.DeliveryTag, false); // 确认消息已处理，删除队列中的消息

                            ReceiveState = false;
                        }
                        catch (Exception ex)
                        {
                            LogConfig.Intence.WriteLog("ErrLog", "DataLog", $"异常：{ex.ToString()}\r\n {ex.StackTrace}");
                            throw;
                        }

                    }
                    catch (Exception ex)
                    {
                        ReceiveState = false;
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false); // 确认消息已处理，删除队列中的消息     
                        System.IO.File.AppendAllText("output\\" + DateTime.Now.ToString("yyyyMMdd_") + ".log", $"====RabbitMQ received event Error Begin======{Environment.NewLine}{errorLog.ToString()}{Environment.NewLine}====RabbitMQ received event Error End======{Environment.NewLine}");
                        errorLog.Clear();



                    }
                };


            }
            catch (Exception ex)
            {
                System.IO.File.AppendAllText("output\\" + DateTime.Now.ToString("yyyyMMdd_") + ".log", $"{DateTime.Now}@Error:{ex.ToString()}{Environment.NewLine}");
                //RichText_InputText($"{DateTime.Now}@Error:{ex.ToString()}");
            }
        }


        #region 数据处理

        /// <summary>
        /// 数据类型 00：设备心跳包
        /// </summary>
        private void HandleCommand00(byte[] data)
        {
            string hexString = BitConverter.ToString(data).Replace("-", " ");

            LogConfig.Intence.WriteLog("RunLog", "DataLog", $"心跳包：{hexString}\r\n");
        }

        /// <summary>
        /// 数据类型 01：设备配置
        /// </summary>
        private void HandleCommand01(byte[] data)
        {
            string hexString = BitConverter.ToString(data).Replace("-", " ");

            LogConfig.Intence.WriteLog("RunLog", "DataLog", $"01数据配置包：{hexString}\r\n");

            // 警报信号，需要启用新的循环
            if (data[5] == 0x0F)
            {
                // 警报信号
                byte[] target = { 0x7B, 0x7B, 0x01, 0x02, 0x00, 0x0F, 0x00, 0x7D, 0x7D };

                bool isEqual = data.SequenceEqual(target);
                if (isEqual)
                {
                    //ForceProcess = true; // 设置强制处理标志位
                }

            }

        }

        /// <summary>
        /// 数据类型 02：主动反馈脉冲数据
        /// </summary>
        private void HandleCommand02(byte[] data, byte[] timeReceiveBytes)
        {

            try
            {
                // 第四、五个字节为数据区长度
                int dataActualLength = GetBigEndianHex(data, 3, 2) - 9;

                // 第六个字节为通道序号 
                int channelNum = GetBigEndianHex(data, 5, 1);

                // 第七个字节为数据位数
                int dataBit = GetBigEndianHex(data, 6, 1);


                // 第八个字节为循环标记
                int loopSign = GetBigEndianHex(data, 7, 1);

                //if (_buffer1.Count == 139)
                //{

                //}


                // 第九到第十二个字节为数据起始序号
                int dataNum = GetBigEndianHex(data, 8, 4);

                // 第十三、十四字节为偏移数据
                int offset = GetBigEndianHex(data, 12, 2);


                string fileName = $"通道{channelNum}_位数{dataBit}_偏移{offset}_循环{loopSign}";

                // 第十五个字节开始为数据区，每两个字节代表一个数据
                #region 原代码注释
                //int[] dataArr = new int[dataActualLength / 4];

                //for (int i = 0; i < dataActualLength; i += 4)
                //{
                //    int value = GetBigEndianHex(data, 14 + i, 4);
                //    dataArr[i / 4] = value;
                //}
                #endregion

                int[] dataArr = new int[dataActualLength / 2];

                for (int i = 0; i < dataActualLength; i += 2)
                {
                    int value = GetBigEndianHex(data, 14 + i, 2);
                    dataArr[i / 2] = value;
                }

                int len = dataArr.Length;
                byte[] bytes = new byte[len * 4]; // 每个int 4字节

                Buffer.BlockCopy(dataArr, 0, bytes, 0, bytes.Length);

                DataPacket packet = new DataPacket();
                packet.PacketIndex = dataNum;
                packet.DataCount = dataActualLength / 2;  // 数据个数（接收到的是每四个字节表示一个数据）
                packet.Data = bytes.ToArray();            // 数据（每四个字节，表示一个int数据）
                packet.Time = timeReceiveBytes.ToArray(); // 时间戳            
                packet.LoopSign = loopSign;               // 循环标志
                packet.ChannelNum = channelNum;           // 通道序号

                if (channelNum == 0) // 通道1
                {
                    bool discard = false;

                    if (loopSign != _lastLoopSign11)
                    {
                        // 循环标志发生变化，处理所有缓冲区数据，清空缓冲区，并重置计数为0
                        _lastLoopSign11 = loopSign;

                        ForceProcess11 = true; // 设置强制处理标志位

                        // 数据溢出，超出界限数据，丢掉
                        if (dataNum > 2000000)
                        {
                            discard = true;
                        }
                    }

                    //  如果设置了强制处理标志位，立即处理所有缓存包
                    if (ForceProcess11)
                    {
                        LogConfig.Intence.WriteLog("RunLog", "ProcessLog", $"主动反馈脉冲数据通道0：循环{_lastLoopSign11}：检测到 ForceProcess = true，立即处理所有缓存数据包。\r\n");
                        ProcessAllPackets(_buffer11, "主动反馈脉冲数据通道0", fileName, _lastLoopSign11);
                        _buffer11.Clear();
                        ForceProcess11 = false; // 处理完成后自动复位                
                    }

                    if (!discard)
                    {
                        _buffer11.Add(packet); // 将数据包添加到缓冲区
                    }

                    if (_buffer11.Count >= 200)
                    {
                        // 是否有重复数据包
                        bool hasDuplicate = _buffer11
                            .GroupBy(p => p.PacketIndex)
                            .Any(g => g.Count() > 1);

                        // 找出重复对象
                        var duplicates = _buffer11
                            .GroupBy(p => p.PacketIndex)
                            .Where(g => g.Count() > 1)
                            .SelectMany(g => g)
                            .ToList();

                        if (hasDuplicate)
                        {
                            LogConfig.Intence.WriteLog("RunLog", "DataLog", $"主动反馈脉冲数据通道0：存在重复数据包 循环标记：{loopSign}\r\n");

                            var seen = new HashSet<int>();
                            var filtered = new List<DataPacket>();

                            foreach (var item in _buffer11)
                            {
                                if (seen.Add(item.PacketIndex))
                                {
                                    filtered.Add(item);
                                }
                            }

                            _buffer11 = filtered;
                        }
                    }

                    //  正常情况：累积200个包时触发处理
                    if (_buffer11.Count >= 200)
                    {
                        //_buffer1.Select

                        // 是否有重复数据包
                        bool hasDuplicate = _buffer11
                            .GroupBy(p => p.PacketIndex)
                            .Any(g => g.Count() > 1);

                        // 找出重复对象
                        var duplicates = _buffer11
                            .GroupBy(p => p.PacketIndex)
                            .Where(g => g.Count() > 1)
                            .SelectMany(g => g)
                            .ToList();

                        _buffer11 = ProcessPackets(_buffer11, "主动反馈脉冲数据通道0", fileName, loopSign);

                        EventReceiveCount?.Invoke($"主动反馈脉冲数据通道0 循环{loopSign}", 100); // 触发事件，刷新界面日志
                    }
                }
                else if (channelNum == 1) // 通道2
                {
                    bool discard = false;

                    if (loopSign != _lastLoopSign12)
                    {
                        // 循环标志发生变化，处理所有缓冲区数据，清空缓冲区，并重置计数为0
                        _lastLoopSign12 = loopSign;

                        ForceProcess12 = true; // 设置强制处理标志位

                        // 数据溢出，超出界限数据，丢掉
                        if (dataNum > 2000000)
                        {
                            discard = true;
                        }
                    }

                    //  如果设置了强制处理标志位，立即处理所有缓存包
                    if (ForceProcess12)
                    {
                        LogConfig.Intence.WriteLog("RunLog", "ProcessLog", $"主动反馈脉冲数据通道1：循环{_lastLoopSign11}：检测到 ForceProcess = true，立即处理所有缓存数据包。\r\n");
                        ProcessAllPackets(_buffer12, "主动反馈脉冲数据通道1", fileName, _lastLoopSign12);
                        _buffer12.Clear();
                        ForceProcess12 = false; // 处理完成后自动复位                
                    }

                    if (!discard)
                    {
                        _buffer12.Add(packet); // 将数据包添加到缓冲区
                    }

                    if (_buffer12.Count >= 200)
                    {
                        // 是否有重复数据包
                        bool hasDuplicate = _buffer12
                            .GroupBy(p => p.PacketIndex)
                            .Any(g => g.Count() > 1);

                        // 找出重复对象
                        var duplicates = _buffer12
                            .GroupBy(p => p.PacketIndex)
                            .Where(g => g.Count() > 1)
                            .SelectMany(g => g)
                            .ToList();

                        if (hasDuplicate)
                        {
                            LogConfig.Intence.WriteLog("RunLog", "DataLog", $"主动反馈脉冲数据通道1：存在重复数据包 循环标记：{loopSign}\r\n");

                            var seen = new HashSet<int>();
                            var filtered = new List<DataPacket>();

                            foreach (var item in _buffer12)
                            {
                                if (seen.Add(item.PacketIndex))
                                {
                                    filtered.Add(item);
                                }
                            }

                            _buffer12 = filtered;
                        }
                    }

                    //  正常情况：累积200个包时触发处理
                    if (_buffer12.Count >= 200)
                    {
                        //_buffer1.Select

                        // 是否有重复数据包
                        bool hasDuplicate = _buffer12
                            .GroupBy(p => p.PacketIndex)
                            .Any(g => g.Count() > 1);

                        // 找出重复对象
                        var duplicates = _buffer12
                            .GroupBy(p => p.PacketIndex)
                            .Where(g => g.Count() > 1)
                            .SelectMany(g => g)
                            .ToList();

                        _buffer12 = ProcessPackets(_buffer12, "主动反馈脉冲数据通道1", fileName, loopSign);

                        EventReceiveCount?.Invoke($"主动反馈脉冲数据通道1 循环{loopSign}", 100); // 触发事件，刷新界面日志
                    }
                }
            }
            catch (Exception ex)
            {
                LogConfig.Intence.WriteLog("ErrLog", "DataLog", $"主动反馈脉冲数据：处理数据包时发生异常：{ex.Message}\r\n {ex.StackTrace}");
                //throw;
            }
                       
        }

        /// <summary>
        /// 数据类型 03：主动反馈采样数据
        /// </summary>
        private void HandleCommand03(byte[] data, byte[] timeReceiveBytes)
        {
            try
            {
                // 第四、五个字节为数据区长度
                int dataActualLength = GetBigEndianHex(data, 3, 2) - 10;

                // 第六个字节为通道序号 
                int channelNum = GetBigEndianHex(data, 5, 1);

                // 第七、八字节为采样率
                int dataBit = GetBigEndianHex(data, 6, 2);

                // 第九个字节为循环标记
                int loopSign = GetBigEndianHex(data, 8, 1);

                // 第十到第十三个字节为数据起始序号
                int dataNum = GetBigEndianHex(data, 9, 4);

                // 第十四、十五字节为偏移数据
                int offset = GetBigEndianHex(data, 13, 2);

                string fileName = $"通道{channelNum}_采样率{dataBit}_偏移{offset}_循环{loopSign}";

                // 第十六个字节开始为数据区，每两个字节代表一个数据
                int[] dataArr = new int[dataActualLength / 2];

                for (int i = 0; i < dataActualLength; i += 2)
                {
                    int value = GetBigEndianHex(data, 15 + i, 2);
                    dataArr[i / 2] = value;
                }

                int len = dataArr.Length;
                byte[] bytes = new byte[len * 4]; // 每个int 4字节

                Buffer.BlockCopy(dataArr, 0, bytes, 0, bytes.Length);

                DataPacket packet = new DataPacket();
                packet.PacketIndex = dataNum;
                packet.DataCount = dataActualLength / 2;  // 数据个数（接收到的是每两个字节表示一个数据）
                packet.Data = bytes.ToArray();            // 数据（每四个字节，表示一个int数据）
                packet.Time = timeReceiveBytes.ToArray(); // 时间戳
                packet.LoopSign = loopSign;               // 循环标志
                packet.ChannelNum = channelNum;           // 通道序号

                if (channelNum == 0)
                {
                    bool discard = false;

                    if (loopSign != _lastLoopSign21)
                    {
                        // 循环标志发生变化，处理所有缓冲区数据，清空缓冲区，并重置计数为0
                        _lastLoopSign21 = loopSign;

                        ForceProcess21 = true; // 设置强制处理标志位

                        // 数据溢出，超出界限数据，丢掉
                        if (dataNum > 2000000)
                        {
                            discard = true;
                        }
                    }

                    //  如果设置了强制处理标志位，立即处理所有缓存包
                    if (ForceProcess21)
                    {
                        LogConfig.Intence.WriteLog("RunLog", "ProcessLog", $"主动反馈采样数据通道0：循环{_lastLoopSign11}：检测到 ForceProcess = true，立即处理所有缓存数据包。\r\n");
                        ProcessAllPackets(_buffer21, "主动反馈采样数据通道0", fileName, _lastLoopSign21);
                        _buffer21.Clear();
                        ForceProcess21 = false; // 处理完成后自动复位                
                    }

                    if (!discard)
                    {
                        _buffer21.Add(packet); // 将数据包添加到缓冲区
                    }

                    if (_buffer21.Count >= 200)
                    {
                        // 是否有重复数据包
                        bool hasDuplicate = _buffer21
                            .GroupBy(p => p.PacketIndex)
                            .Any(g => g.Count() > 1);

                        // 找出重复对象
                        var duplicates = _buffer21
                            .GroupBy(p => p.PacketIndex)
                            .Where(g => g.Count() > 1)
                            .SelectMany(g => g)
                            .ToList();

                        if (hasDuplicate)
                        {
                            LogConfig.Intence.WriteLog("RunLog", "DataLog", $"主动反馈采样数据通道0：存在重复数据包 循环标记：{loopSign}\r\n");
                            var seen = new HashSet<int>();
                            var filtered = new List<DataPacket>();

                            foreach (var item in _buffer21)
                            {
                                if (seen.Add(item.PacketIndex))
                                {
                                    filtered.Add(item);
                                }
                            }

                            _buffer21 = filtered;
                        }
                    }

                    //  正常情况：累积200个包时触发处理
                    if (_buffer21.Count >= 200)
                    {
                        // 是否有重复数据包
                        bool hasDuplicate = _buffer21
                            .GroupBy(p => p.PacketIndex)
                            .Any(g => g.Count() > 1);

                        // 找出重复对象
                        var duplicates = _buffer21
                            .GroupBy(p => p.PacketIndex)
                            .Where(g => g.Count() > 1)
                            .SelectMany(g => g)
                            .ToList();

                        _buffer21 = ProcessPackets(_buffer21, "主动反馈采样数据通道0", fileName, loopSign);
                        EventReceiveCount?.Invoke($"主动反馈采样数据通道0 循环{loopSign}", 100); // 触发事件，刷新界面日志
                    }
                }
                else if (channelNum == 1)
                {
                    bool discard = false;

                    if (loopSign != _lastLoopSign22)
                    {
                        // 循环标志发生变化，处理所有缓冲区数据，清空缓冲区，并重置计数为0
                        _lastLoopSign22 = loopSign;

                        ForceProcess22 = true; // 设置强制处理标志位

                        // 数据溢出，超出界限数据，丢掉
                        if (dataNum > 2000000)
                        {
                            discard = true;
                        }
                    }

                    //  如果设置了强制处理标志位，立即处理所有缓存包
                    if (ForceProcess22)
                    {
                        LogConfig.Intence.WriteLog("RunLog", "ProcessLog", $"主动反馈采样数据通道1：循环{_lastLoopSign11}：检测到 ForceProcess = true，立即处理所有缓存数据包。\r\n");
                        ProcessAllPackets(_buffer22, "主动反馈采样数据通道1", fileName, _lastLoopSign22);
                        _buffer22.Clear();
                        ForceProcess22 = false; // 处理完成后自动复位                
                    }

                    if (!discard)
                    {
                        _buffer22.Add(packet); // 将数据包添加到缓冲区
                    }

                    if (_buffer22.Count >= 200)
                    {
                        // 是否有重复数据包
                        bool hasDuplicate = _buffer22
                            .GroupBy(p => p.PacketIndex)
                            .Any(g => g.Count() > 1);

                        // 找出重复对象
                        var duplicates = _buffer22
                            .GroupBy(p => p.PacketIndex)
                            .Where(g => g.Count() > 1)
                            .SelectMany(g => g)
                            .ToList();

                        if (hasDuplicate)
                        {
                            LogConfig.Intence.WriteLog("RunLog", "DataLog", $"主动反馈采样数据通道1：存在重复数据包 循环标记：{loopSign}\r\n");
                            var seen = new HashSet<int>();
                            var filtered = new List<DataPacket>();

                            foreach (var item in _buffer22)
                            {
                                if (seen.Add(item.PacketIndex))
                                {
                                    filtered.Add(item);
                                }
                            }

                            _buffer22 = filtered;
                        }
                    }

                    //  正常情况：累积200个包时触发处理
                    if (_buffer22.Count >= 200)
                    {
                        // 是否有重复数据包
                        bool hasDuplicate = _buffer22
                            .GroupBy(p => p.PacketIndex)
                            .Any(g => g.Count() > 1);

                        // 找出重复对象
                        var duplicates = _buffer22
                            .GroupBy(p => p.PacketIndex)
                            .Where(g => g.Count() > 1)
                            .SelectMany(g => g)
                            .ToList();

                        _buffer22 = ProcessPackets(_buffer22, "主动反馈采样数据通道1", fileName, loopSign);
                        EventReceiveCount?.Invoke($"主动反馈采样数据通道1 循环{loopSign}", 100); // 触发事件，刷新界面日志
                    }
                }
                else if (channelNum == 2)
                {
                    bool discard = false;

                    if (loopSign != _lastLoopSign23)
                    {
                        // 循环标志发生变化，处理所有缓冲区数据，清空缓冲区，并重置计数为0
                        _lastLoopSign23 = loopSign;

                        ForceProcess23 = true; // 设置强制处理标志位

                        // 数据溢出，超出界限数据，丢掉
                        if (dataNum > 2000000)
                        {
                            discard = true;
                        }
                    }

                    //  如果设置了强制处理标志位，立即处理所有缓存包
                    if (ForceProcess23)
                    {
                        LogConfig.Intence.WriteLog("RunLog", "ProcessLog", $"主动反馈采样数据通道2：循环{_lastLoopSign11}：检测到 ForceProcess = true，立即处理所有缓存数据包。\r\n");
                        ProcessAllPackets(_buffer23, "主动反馈采样数据通道2", fileName, _lastLoopSign23);
                        _buffer23.Clear();
                        ForceProcess23 = false; // 处理完成后自动复位                
                    }

                    if (!discard)
                    {
                        _buffer23.Add(packet); // 将数据包添加到缓冲区
                    }

                    if (_buffer23.Count >= 200)
                    {
                        // 是否有重复数据包
                        bool hasDuplicate = _buffer23
                            .GroupBy(p => p.PacketIndex)
                            .Any(g => g.Count() > 1);

                        // 找出重复对象
                        var duplicates = _buffer23
                            .GroupBy(p => p.PacketIndex)
                            .Where(g => g.Count() > 1)
                            .SelectMany(g => g)
                            .ToList();

                        if (hasDuplicate)
                        {
                            LogConfig.Intence.WriteLog("RunLog", "DataLog", $"主动反馈采样数据通道2：存在重复数据包 循环标记：{loopSign}\r\n");

                            var seen = new HashSet<int>();
                            var filtered = new List<DataPacket>();

                            foreach (var item in _buffer23)
                            {
                                if (seen.Add(item.PacketIndex))
                                {
                                    filtered.Add(item);
                                }
                            }

                            _buffer23 = filtered;
                        }
                    }

                    //  正常情况：累积200个包时触发处理
                    if (_buffer23.Count >= 200)
                    {
                        // 是否有重复数据包
                        bool hasDuplicate = _buffer23
                            .GroupBy(p => p.PacketIndex)
                            .Any(g => g.Count() > 1);

                        // 找出重复对象
                        var duplicates = _buffer23
                            .GroupBy(p => p.PacketIndex)
                            .Where(g => g.Count() > 1)
                            .SelectMany(g => g)
                            .ToList();

                        _buffer23 = ProcessPackets(_buffer23, "主动反馈采样数据通道2", fileName, loopSign);
                        EventReceiveCount?.Invoke($"主动反馈采样数据通道2 循环{loopSign}", 100); // 触发事件，刷新界面日志
                    }
                }
                else if (channelNum == 3)
                {
                    bool discard = false;

                    if (loopSign != _lastLoopSign24)
                    {
                        // 循环标志发生变化，处理所有缓冲区数据，清空缓冲区，并重置计数为0
                        _lastLoopSign24 = loopSign;

                        ForceProcess24 = true; // 设置强制处理标志位

                        // 数据溢出，超出界限数据，丢掉
                        if (dataNum > 2000000)
                        {
                            discard = true;
                        }
                    }

                    //  如果设置了强制处理标志位，立即处理所有缓存包
                    if (ForceProcess24)
                    {
                        LogConfig.Intence.WriteLog("RunLog", "ProcessLog", $"主动反馈采样数据通道3：循环{_lastLoopSign11}：检测到 ForceProcess = true，立即处理所有缓存数据包。\r\n");
                        ProcessAllPackets(_buffer24, "主动反馈采样数据通道3", fileName, _lastLoopSign24);
                        _buffer24.Clear();
                        ForceProcess24 = false; // 处理完成后自动复位                
                    }

                    if (!discard)
                    {
                        _buffer24.Add(packet); // 将数据包添加到缓冲区
                    }

                    if (_buffer24.Count >= 200)
                    {
                        // 是否有重复数据包
                        bool hasDuplicate = _buffer24
                            .GroupBy(p => p.PacketIndex)
                            .Any(g => g.Count() > 1);

                        // 找出重复对象
                        var duplicates = _buffer24
                            .GroupBy(p => p.PacketIndex)
                            .Where(g => g.Count() > 1)
                            .SelectMany(g => g)
                            .ToList();

                        if (hasDuplicate)
                        {
                            LogConfig.Intence.WriteLog("RunLog", "DataLog", $"主动反馈采样数据通道3：存在重复数据包 循环标记：{loopSign}\r\n");

                            var seen = new HashSet<int>();
                            var filtered = new List<DataPacket>();

                            foreach (var item in _buffer24)
                            {
                                if (seen.Add(item.PacketIndex))
                                {
                                    filtered.Add(item);
                                }
                            }

                            _buffer24 = filtered;
                        }
                    }

                    //  正常情况：累积200个包时触发处理
                    if (_buffer24.Count >= 200)
                    {
                        // 是否有重复数据包
                        bool hasDuplicate = _buffer24
                            .GroupBy(p => p.PacketIndex)
                            .Any(g => g.Count() > 1);

                        // 找出重复对象
                        var duplicates = _buffer24
                            .GroupBy(p => p.PacketIndex)
                            .Where(g => g.Count() > 1)
                            .SelectMany(g => g)
                            .ToList();

                        _buffer24 = ProcessPackets(_buffer24, "主动反馈采样数据通道3", fileName, loopSign);
                        EventReceiveCount?.Invoke($"主动反馈采样数据通道3 循环{loopSign}", 100); // 触发事件，刷新界面日志
                    }
                }
            }
            catch (Exception ex)
            {
                LogConfig.Intence.WriteLog("ErrLog", "DataLog", $"主动反馈采样数据：处理数据包时发生异常：{ex.Message}\r\n {ex.StackTrace}");
                //throw;
            }            

        }

        /// <summary>
        /// 从字节数组中取出连续的字节段，将小端转为大端，并转换为int值。
        /// </summary>
        /// <param name="source">源字节数组</param>
        /// <param name="startIndex">起始位置（从0开始）</param>
        /// <param name="length">要取的字节数量</param>
        /// <returns>大端排序后的十六进制字符串（如 "0A1B2C3D"）</returns>
        public static int GetBigEndianHex(byte[] source, int startIndex, int length)
        {
            // 参数检查，防止越界或空引用
            if (source == null)
            {
                return 0;
            }
            if (startIndex < 0 || length <= 0 || startIndex + length > source.Length)
            {
                return 0;
            }

            // 1️⃣ 取出指定长度的字节段
            byte[] segment = new byte[length];
            Array.Copy(source, startIndex, segment, 0, length);

            // 2️⃣ 小端转大端（反转字节顺序）
            Array.Reverse(segment);

            // 3️⃣ 转换为十六进制字符串（大写且无空格）
            StringBuilder sb = new StringBuilder(length * 2);
            foreach (byte b in segment)
            {
                sb.Append(b.ToString("X2")); // 每个字节转两位十六进制
            }

            int value = Convert.ToInt32(sb.ToString(), 16);

            return value;
        }

        /// <summary>
        /// 获取数据序号byte数组：将int数组转换为字节数组
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public byte[] GetDataNumBytes(int _dataPacketCount)
        {
            int dataPacketNum = _dataPacketCount / 100; // 数据包数量(一个包默认有700条数据)

            // 数据序号
            int count = 70000;                    // 需要写入的整数个数
            byte[] byteNum = new byte[count * 4]; // 70000 * 4 = 280000

            int[] values = new int[count];

            for (int i = (dataPacketNum - 1) * count; i < count * dataPacketNum; i++)
            {
                values[i] = (i + 1);
            }

            // 将 int 数组整体复制到 byte 数组中
            Buffer.BlockCopy(values, 0, byteNum, 0, byteNum.Length);
            return byteNum;
        }

        #region 逻辑处理

        /// <summary>
        /// 正常模式：处理200个数据包
        /// </summary>
        private List<DataPacket> ProcessPackets(List<DataPacket> buffer111, string dataType, string fileName, int loopSign)
        {
            // 按序号排序
            var buffer = buffer111.OrderBy(p => p.PacketIndex).ToList();

            //buffer.Sort((a, b) => a.PacketIndex.CompareTo(b.PacketIndex));
            //byte[] firstTimeValue = _buffer.First().Time;

            // 取前100个包，第一个包的Time，作为数据时间
            byte[] firstTimeValue = buffer.OrderBy(p => p.PacketIndex).First().Time;

            #region 文件名时间
            // 假设已知当天日期
            DateTime today = DateTime.Today;

            // 将 byte[] 转回 uint
            uint msOfDay = BitConverter.ToUInt32(firstTimeValue, 0);

            // 将毫秒数加回当天日期，得到完整时间
            DateTime time_Plc_Receive = today.AddMilliseconds(msOfDay);

            string timeReceive = time_Plc_Receive.ToString("yyyyMMdd_HHmmss_fff");
            #endregion

            // 取前100个包
            var first100 = buffer.Take(100).ToList();

            // 调用核心处理逻辑
            var resultBytes = HandlePacketList(first100, firstTimeValue);

            // 获取当前程序的运行目录（例如 D:\MyApp\bin\Debug）
            string basePath = AppDomain.CurrentDomain.BaseDirectory;

            // 组合出 Data 文件夹的完整路径
            string dataDir = Path.Combine(basePath, "Data", dataType);

            // 若 Data 文件夹不存在，则自动创建
            if (!Directory.Exists(dataDir))
            {
                Directory.CreateDirectory(dataDir);
            }

            // 拼接最终文件路径
            string filePath = Path.Combine(dataDir, $"时间{timeReceive}_{fileName}.bin");

            // 最终结果写入文件
            using (FileStream fs = new FileStream
                (filePath, FileMode.Append, FileAccess.Write))
            {
                fs.Write(resultBytes, 0, resultBytes.Length);
            }

            LogConfig.Intence.WriteLog("RunLog", "ProcessLog", $"{dataType} 循环标记：{loopSign} 数据包数量：100\r\n");

            // 存储结果
            //ProcessedByteBlocks.Add(resultBytes);

            // 剩下100个包保留继续
            var returnBytes = buffer.Skip(100).ToList();

            return returnBytes;
        }

        /// <summary>
        /// 强制模式：直接处理所有包（不管数量是否足够）
        /// </summary>
        private void ProcessAllPackets(List<DataPacket> buffer, string dataType, string fileName, int lastLoopSign)
        {
            if (buffer.Count == 0)
                return;

            // 全部排序
            buffer = buffer.OrderBy(p => p.PacketIndex).ToList();

            // 取排序后第一个包的Time，作为数据时间
            byte[] firstTimeValue = buffer.OrderBy(p => p.PacketIndex).First().Time;

            #region 文件名时间
            // 假设已知当天日期
            DateTime today = DateTime.Today;

            // 将 byte[] 转回 uint
            uint msOfDay = BitConverter.ToUInt32(firstTimeValue, 0);

            // 将毫秒数加回当天日期，得到完整时间
            DateTime time_Plc_Receive = today.AddMilliseconds(msOfDay);

            string timeReceive = time_Plc_Receive.ToString("yyyyMMdd_HHmmss_fff");
            #endregion

            // 调用相同的核心处理逻辑
            var resultBytes = HandlePacketList(buffer, firstTimeValue);

            // 获取当前程序的运行目录（例如 D:\MyApp\bin\Debug）
            string basePath = AppDomain.CurrentDomain.BaseDirectory;

            // 组合出 Data 文件夹的完整路径
            string dataDir = Path.Combine(basePath, "Data", dataType);

            // 若 Data 文件夹不存在，则自动创建
            if (!Directory.Exists(dataDir))
            {
                Directory.CreateDirectory(dataDir);
            }

            // 拼接最终文件路径
            string filePath = Path.Combine(dataDir, $"时间{timeReceive}_{fileName}.bin");

            // 最终结果写入文件
            using (FileStream fs = new FileStream
                (filePath, FileMode.Append, FileAccess.Write))
            {
                fs.Write(resultBytes, 0, resultBytes.Length);
            }

            LogConfig.Intence.WriteLog("RunLog", "ProcessLog", $"{dataType} 循环标记：{lastLoopSign} 数据包数量：{buffer.Count}\r\n");
            EventReceiveCount?.Invoke($"{dataType} 循环标记{lastLoopSign}", buffer.Count); // 触发事件，刷新界面日志

            //ProcessedByteBlocks.Add(resultBytes);

            // 处理完清空缓存
            buffer.Clear();
        }

        /// <summary>
        /// 核心逻辑：处理一组数据包（检查连续性、插入C包、拼接数据）
        /// </summary>
        private byte[] HandlePacketList(List<DataPacket> packetList, byte[] firstTimeValue)
        {

            List<DataPacket> processedList = new List<DataPacket>();

            for (int i = 0; i < packetList.Count; i++)
            {
                var current = packetList[i];
                processedList.Add(current);

                if (i == packetList.Count - 1)
                    break;

                var next = packetList[i + 1];

                int endA = current.PacketIndex + current.DataCount;  // A包结束序号
                int startB = next.PacketIndex;                       // B包起始序号

                // 检查是否连续
                if (endA != startB)
                {
                    int cIndex = endA;
                    int cCount = (startB - 1) - cIndex;

                    if (cCount > 0)
                    {
                        processedList.Add(new DataPacket
                        {
                            PacketIndex = cIndex,
                            DataCount = cCount,
                            Data = new byte[4 * cCount] // 全部填充0
                        });
                    }
                }
            }

            // 拼接所有数据
            List<byte> mergedBytes = new List<byte>();
            foreach (var pkt in processedList)
                mergedBytes.AddRange(pkt.Data);


            // === 3. 生成等长的序号数组 ===
            List<byte> indexBytes = new List<byte>();

            // 起始值：第一个数据包的 PacketIndex
            int startIndex = processedList.First().PacketIndex;
            int totalDataCount = mergedBytes.Count / 4;

            // 计算每个 index 对应的4字节表示
            for (int i = 0; i < totalDataCount; i++)
            {
                int currentIndex = startIndex + i;
                byte[] intBytes = BitConverter.GetBytes(currentIndex);
                indexBytes.AddRange(intBytes);
            }

            // 最后校验长度是否一致            
            if (indexBytes.Count != mergedBytes.Count)
            {
                // throw new InvalidOperationException("indexBytes 与 mergedBytes 长度不一致！");
            }
            else
            {
                //int dataLength = mergedBytes.Count / 4;
                //byte[] dataLengthByte = BitConverter.GetBytes(dataLength);
            }

            int dataLength = mergedBytes.Count / 4;
            byte[] dataLengthByte = BitConverter.GetBytes(dataLength);


            // 合并最终的结果
            // 计算合并后的总长度
            int totalLength = firstTimeValue.Length + dataLengthByte.Length + indexBytes.ToArray().Length + mergedBytes.ToArray().Length;

            // 创建一个新的 byte 数组用于存放结果
            byte[] merged = new byte[totalLength];

            // 当前写入位置
            int offset = 0;

            // 依次拷贝四个数组内容
            Buffer.BlockCopy(firstTimeValue, 0, merged, offset, firstTimeValue.Length);
            offset += firstTimeValue.Length;

            Buffer.BlockCopy(dataLengthByte, 0, merged, offset, dataLengthByte.Length);
            offset += dataLengthByte.Length;

            Buffer.BlockCopy(indexBytes.ToArray(), 0, merged, offset, indexBytes.ToArray().Length);
            offset += indexBytes.ToArray().Length;

            Buffer.BlockCopy(mergedBytes.ToArray(), 0, merged, offset, mergedBytes.ToArray().Length);
            offset += mergedBytes.ToArray().Length;

            //return (mergedBytes.ToArray(), indexBytes.ToArray());

            return merged;
        }

        #endregion

        #region 注释
        ///// <summary>
        ///// 对当前 200 个数据包进行处理
        ///// </summary>
        //private void ProcessPackets()
        //{
        //    // Step 1：按序号排序
        //    _buffer = _buffer.OrderBy(p => p.PacketIndex).ToList();

        //    // Step 2：取前100个包进行处理
        //    var first100 = _buffer.Take(100).ToList();

        //    // Step 3：检查相邻数据包之间是否连续，不连续则插入空包（C包）
        //    List<DataPacket> processedList = new List<DataPacket>();

        //    for (int i = 0; i < first100.Count; i++)
        //    {
        //        // 当前包
        //        var current = first100[i];
        //        processedList.Add(current);

        //        // 最后一个包不用比较
        //        if (i == first100.Count - 1)
        //            break;

        //        var next = first100[i + 1];

        //        // A包的结束位置 = A包序号 + A包数据数量
        //        int endA = current.PacketIndex + current.DataCount;

        //        // B包的开始位置 = B包序号
        //        int startB = next.PacketIndex;

        //        // 理论上应该 endA + 1 == startB，如果不成立，就需要补C包
        //        if (endA + 1 != startB)
        //        {
        //            // 创建 C 包
        //            int cIndex = endA;  // C包序号 = A包序号 + 数据数量
        //            int cCount = (startB - 1) - cIndex; // C包数据数量 = B包序号 - 1 - C包序号

        //            if (cCount > 0)
        //            {
        //                processedList.Add(new DataPacket
        //                {
        //                    PacketIndex = cIndex,
        //                    DataCount = cCount,
        //                    Data = new byte[cCount] // C包数据值全部为0
        //                });
        //            }
        //        }
        //    }

        //    // Step 4：将所有包的数据拼接成一个完整的 byte 数组
        //    List<byte> mergedBytes = new List<byte>();
        //    foreach (var pkt in processedList)
        //    {
        //        mergedBytes.AddRange(pkt.Data);
        //    }

        //    // Step 5：存储处理结果
        //    // ProcessedByteBlocks.Add(mergedBytes.ToArray());

        //    // Step 6：剩下100个包继续保留，用于下次循环
        //    _buffer = _buffer.Skip(100).ToList();
        //} 
        #endregion

        #endregion

    }


    /// <summary>
    /// 表示一个数据包
    /// </summary>
    public class DataPacket
    {
        /// <summary>
        /// 数据包序号（唯一编号，用于排序）
        /// </summary>
        public int PacketIndex { get; set; }

        /// <summary>
        /// 当前数据包中包含的数据数量
        /// </summary>
        public int DataCount { get; set; }

        /// <summary>
        /// 数据内容（字节数组）
        /// </summary>
        public byte[] Data { get; set; }

        /// <summary>
        /// 数据时间
        /// </summary>
        public byte[] Time { get; set; }

        /// <summary>
        /// 循环标记
        /// </summary>
        public int LoopSign { get; set; }

        /// <summary>
        /// 通道号
        /// </summary>
        public int ChannelNum { get; set; }
    }
}
