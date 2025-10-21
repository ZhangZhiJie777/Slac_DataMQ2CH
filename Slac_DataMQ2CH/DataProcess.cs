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
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Entity.Core.Common.CommandTrees.ExpressionBuilder;
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

        private int _lastLoopSign; // 循环标志     

        private int _dataPacketNum; // 数据包数量计数（每100个包上传一次）

        private int _dataPacketCount; // 数据包计数（累计多少个数据）

        private byte[] timeByte = new byte[4]; // 时间字节

        #endregion


        // 当前接收缓存区，用于存放未处理的数据包
        private List<DataPacket> _buffer1 = new List<DataPacket>();

        private List<DataPacket> _buffer2 = new List<DataPacket>();

        /// <summary>
        /// 当前是否强制立即处理标志位
        /// </summary>
        public bool ForceProcess { get; set; } = false;

        public DataProcess()
        {
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
                                //Console.WriteLine($"来自 {ipstr} 的数据长度不足，无法解析。");
                                return;
                            }

                            // 校验帧头是否为 7B 7B
                            bool headMatch = buffer_all[0] == 0x7B && buffer_all[1] == 0x7B;
                            // 校验帧尾是否为 7D 7D
                            bool tailMatch = buffer_all[buffer_length - 2] == 0x7D && buffer_all[buffer_length - 1] == 0x7D;

                            if (!headMatch || !tailMatch)
                            {
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
                                    HandleCommand02(buffer_all, timeReceiveBytes);
                                    break;

                                case 0x03:
                                    HandleCommand03(buffer_all, timeReceiveBytes);
                                    break;

                                default:
                                    Console.WriteLine($" 未知命令字: {command:X2}");
                                    break;
                            }

                            channel.BasicAck(ea.DeliveryTag, false); // 确认消息已处理，删除队列中的消息
                        }
                        catch (Exception ex)
                        {

                            throw;
                        }

                    }
                    catch (Exception ex)
                    {

                        System.IO.File.AppendAllText("output\\" + DateTime.Now.ToString("yyyyMMdd_") + ".log", $"====RabbitMQ received event Error Begin======{Environment.NewLine}{errorLog.ToString()}{Environment.NewLine}====RabbitMQ received event Error End======{Environment.NewLine}");
                        errorLog.Clear();
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false); // 确认消息已处理，删除队列中的消息     



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
            //Console.WriteLine($"执行命令 00：设备 {remote} 回复心跳包");
            // TODO: 在这里添加具体业务逻辑
        }

        /// <summary>
        /// 数据类型 01：设备配置
        /// </summary>
        private void HandleCommand01(byte[] data)
        {
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

            // 第四、五个字节为数据区长度
            int dataActualLength = GetBigEndianHex(data, 3, 2);

            // 第六个字节为通道序号 
            int channelNum = GetBigEndianHex(data, 5, 1);

            // 第七个字节为数据位数
            int dataBit = GetBigEndianHex(data, 6, 1);

            // 第八个字节为循环标记
            int loopSign = GetBigEndianHex(data, 7, 1);

            // 第九到第十二个字节为数据起始序号
            int dataNum = GetBigEndianHex(data, 8, 4);

            // 第十三、十四字节为偏移数据
            int offset = GetBigEndianHex(data, 12, 2);


            string fileName = $"通道{channelNum}_位数{dataBit}_偏移{offset}_循环{loopSign}";

            // 第十五个字节开始为数据区，每两个字节代表一个数据
            int[] dataArr = new int[dataActualLength / 2];

            for (int i = 0; i < dataActualLength; i += 2)
            {
                int value = GetBigEndianHex(data, 14 + i, 2);
                dataArr[i / 2] = value;
            }

            int len = dataArr.Length;
            byte[] bytes = new byte[len * 4]; // 每个int 4字节

            Buffer.BlockCopy(dataArr, 0, bytes, 0, bytes.Length);


            if (loopSign != _lastLoopSign)
            {
                // 循环标志发生变化，处理所有缓冲区数据，清空缓冲区，并重置计数为0
                _dataPacketNum = 0;
                _dataPacketCount = 0;
                _lastLoopSign = loopSign;

                ForceProcess = true; // 设置强制处理标志位
            }

            DataPacket packet = new DataPacket();
            packet.PacketIndex = dataNum;
            packet.DataCount = dataActualLength / 2; // 数据个数（接收到的是每两个字节表示一个数据）
            packet.Data = bytes;                     // 数据（每四个字节，表示一个int数据）
            packet.Time = timeReceiveBytes;          // 时间戳            

            //  如果设置了强制处理标志位，立即处理所有缓存包
            if (ForceProcess)
            {
                Console.WriteLine("检测到 ForceProcess = true，立即处理所有缓存数据包。");
                ProcessAllPackets(_buffer1, "主动反馈脉冲数据", fileName);
                _buffer1.Clear();
                ForceProcess = false; // 处理完成后自动复位
                //return;
            }

            _buffer1.Add(packet); // 将数据包添加到缓冲区


            //  正常情况：累积200个包时触发处理
            if (_buffer1.Count >= 200)
            {
                _buffer1 = ProcessPackets(_buffer1, "主动反馈脉冲数据", fileName);
            }

            #region 注释
            //_dataPacketNum++;   // 数据包上传计数加1 （满足200个数据包，上传前100个数据包）
            //_dataPacketCount++; // 数据包总计数加1

            //// 数据包计数小于100，继续累加数据包
            //if (_dataPacketNum < 100)
            //{
            //    // 数据包计数小于100，继续累加数据包到第一个缓冲区
            //    int index = (dataNum % 70000) * 4;

            //    Buffer.BlockCopy(bytes, 0, firstData, index, bytes.Length);

            //}
            //else if (_dataPacketNum == 100)
            //{
            //    int index = (dataNum % 70000) * 4;

            //    Buffer.BlockCopy(bytes, 0, firstData, index, bytes.Length);

            //    firstDataNum = GetDataNumBytes(_dataPacketCount); // 获取数据序号byte数组

            //}
            //else if (_dataPacketNum > 100 && _dataPacketNum < 200)
            //{
            //    // 数据包计数大于100，小于200，继续累加数据包到第二个缓冲区
            //    int index = (dataNum % 70000) * 4;

            //    Buffer.BlockCopy(bytes, 0, secondData, index, bytes.Length);

            //}
            //else if (_dataPacketNum == 200)
            //{
            //    // 数据包计数等于200，处理数据(处理第一层缓冲数据firstData)
            //    _dataPacketNum = 100;

            //} 
            #endregion

        }

        /// <summary>
        /// 数据类型 03：主动反馈采样数据
        /// </summary>
        private void HandleCommand03(byte[] data, byte[] timeReceiveBytes)
        {
            // 第四、五个字节为数据区长度
            int dataActualLength = GetBigEndianHex(data, 3, 2);

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
                dataArr[i] = value;
            }

            int len = dataArr.Length;
            byte[] bytes = new byte[len * 4]; // 每个int 4字节

            Buffer.BlockCopy(dataArr, 0, bytes, 0, bytes.Length);


            if (loopSign != _lastLoopSign)
            {
                // 循环标志发生变化，处理所有缓冲区数据，清空缓冲区，并重置计数为0
                _dataPacketNum = 0;
                _dataPacketCount = 0;
                _lastLoopSign = loopSign;

                ForceProcess = true; // 设置强制处理标志位
            }

            DataPacket packet = new DataPacket();
            packet.PacketIndex = dataNum;
            packet.DataCount = dataActualLength / 2; // 数据个数（接收到的是每两个字节表示一个数据）
            packet.Data = bytes;                     // 数据（每四个字节，表示一个int数据）
            packet.Time = timeReceiveBytes;          // 时间戳

            //  如果设置了强制处理标志位，立即处理所有缓存包
            if (ForceProcess)
            {
                Console.WriteLine("检测到 ForceProcess = true，立即处理所有缓存数据包。");
                ProcessAllPackets(_buffer2, "主动反馈采样数据", fileName);
                _buffer2.Clear();
                ForceProcess = false; // 处理完成后自动复位
                //return;
            }

            _buffer2.Add(packet); // 将数据包添加到缓冲区


            //  正常情况：累积200个包时触发处理
            if (_buffer2.Count >= 200)
            {
                _buffer2 = ProcessPackets(_buffer2, "主动反馈采样数据", fileName);
                EventReceiveCount?.Invoke($"主动反馈采样数据 循环{loopSign}", 100); // 触发事件，刷新界面日志
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
        private List<DataPacket> ProcessPackets(List<DataPacket> buffer, string dataType, string fileName)
        {
            // 按序号排序
            buffer = buffer.OrderBy(p => p.PacketIndex).ToList();

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

            string timeReceive = time_Plc_Receive.ToString("yyyyMMdd_HHmmss");
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
            string filePath = Path.Combine(dataDir, $"{timeReceive}_{fileName}.bin");

            // 最终结果写入文件
            using (FileStream fs = new FileStream
                (filePath, FileMode.Append, FileAccess.Write))
            {
                fs.Write(resultBytes, 0, resultBytes.Length);
            }

            // 存储结果
            //ProcessedByteBlocks.Add(resultBytes);

            // 剩下100个包保留继续
            var returnBytes = buffer.Skip(100).ToList();

            return returnBytes;
        }

        /// <summary>
        /// 强制模式：直接处理所有包（不管数量是否足够）
        /// </summary>
        private void ProcessAllPackets(List<DataPacket> buffer, string dataType, string fileName)
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

            string timeReceive = time_Plc_Receive.ToString("yyyyMMdd_HHmmss");
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
            string filePath = Path.Combine(dataDir, $"{timeReceive}_{fileName}.bin");

            // 最终结果写入文件
            using (FileStream fs = new FileStream
                (filePath, FileMode.Append, FileAccess.Write))
            {
                fs.Write(resultBytes, 0, resultBytes.Length);
            }

            EventReceiveCount?.Invoke($"{dataType} 循环标记{_lastLoopSign}", buffer.Count); // 触发事件，刷新界面日志

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
                if (endA + 1 != startB)
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
    }
}
