/***********************************************************************
*
* Copyright (C) 2010-2025, SLAC Co.,Ltd.,All Rights Reserved.
*
************************************************************************
* CLR Version： 4.0.30319.42000
* FileName:DataComm
* Author：02899<张志杰>
* E-mail:zhangzhijie@slac.com.cn
* Create Date: 2025-10-16 10:59:47
* Description: UDP通讯类
*
*-----------------------------------------------------------------------
* Modifier：
* Modify Date:
* Description:
*
************************************************************************/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Slac_DataMQ2CH
{
    /// <summary>
    /// UDP 数据通信类
    /// </summary>
    public class DataComm
    {
        #region 字段与事件

        /// <summary>
        /// 数据接收事件，当成功接收到 UDP 数据后触发
        /// 参数：Tuple(远端地址, 数据字节数组, 数据长度)
        /// </summary>
        public event Action<Tuple<IPEndPoint, byte[], int>> EventAcceptData;

        /// <summary>
        /// UDP 服务端 Socket 对象
        /// 用于接收与发送 UDP 数据包
        /// </summary>
        private Socket _udpSocket;

        /// <summary>
        /// 接收异步事件对象，用于非阻塞接收数据
        /// </summary>
        private SocketAsyncEventArgs _recvEventArgs;

        /// <summary>
        /// 接收缓冲区（预分配 64MB）
        /// 注意：过大会占用内存，但可以减少频繁 GC
        /// </summary>
        private readonly byte[] _recvBuffer = new byte[64 * 1024 * 1024]; // 64MB

        /// <summary>
        /// 用于控制服务器启动状态
        /// </summary>
        private bool _isRunning = false;

        /// <summary>
        /// 同步锁，防止多线程同时访问Socket资源
        /// </summary>
        private readonly object _lock = new object();

        private string exepath = System.Reflection.Assembly.GetExecutingAssembly().Location;

        #endregion

        #region 启动与停止

        /// <summary>
        /// 启动 UDP 服务端
        /// </summary>
        /// <param name="ip">绑定的本地 IP 地址（如 "0.0.0.0" 表示监听所有网卡）</param>
        /// <param name="port">监听端口号</param>
        /// <returns>启动成功返回 true，否则 false</returns>
        public bool StartDATAServer(string ip, int port)
        {
            try
            {
                // 初始化 Socket，使用 UDP 协议
                _udpSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);

                // 设置接收缓冲区大小（影响内核 UDP 缓冲性能）
                // 这里设置为 64MB，可根据网速与内存情况调整
                _udpSocket.ReceiveBufferSize = 64 * 1024 * 1024; // 64MB（接收缓冲区）
                _udpSocket.SendBufferSize = 8 * 1024 * 1024;     // 8MB （发送缓冲区）

                #region 打印缓冲区实际大小（设置值可能被操作系统拦截或下调）
                int realRecv = (int)_udpSocket.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveBuffer);
                int realSend = (int)_udpSocket.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendBuffer);

                Console.WriteLine($"[UDP] 实际接收缓冲区: {realRecv / 1024 / 1024} MB");
                Console.WriteLine($"[UDP] 实际发送缓冲区: {realSend / 1024 / 1024} MB");
                #endregion


                // 绑定 IP 与端口
                IPEndPoint localEP = new IPEndPoint(IPAddress.Parse(ip), port);
                _udpSocket.Bind(localEP);

                // 初始化异步接收参数
                _recvEventArgs = new SocketAsyncEventArgs();
                _recvEventArgs.RemoteEndPoint = new IPEndPoint(IPAddress.Any, 0);
                _recvEventArgs.SetBuffer(_recvBuffer, 0, _recvBuffer.Length);
                _recvEventArgs.Completed += OnReceiveCompleted;

                // 启动第一次异步接收
                bool willRaiseEvent = _udpSocket.ReceiveFromAsync(_recvEventArgs);
                if (!willRaiseEvent)
                {
                    // 如果异步操作立即完成，则手动调用回调处理
                    ProcessReceive(_recvEventArgs);
                }

                _isRunning = true;
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[UDP] 启动失败：{ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// 停止 UDP 服务端
        /// 关闭 Socket 并释放资源
        /// </summary>
        public void StopDATAServer()
        {
            lock (_lock)
            {
                _isRunning = false;

                if (_recvEventArgs != null)
                {
                    _recvEventArgs.Completed -= OnReceiveCompleted;
                    _recvEventArgs.Dispose();
                    _recvEventArgs = null;
                }

                if (_udpSocket != null)
                {
                    try
                    {
                        _udpSocket.Shutdown(SocketShutdown.Both);
                    }
                    catch { }

                    _udpSocket.Close();
                    _udpSocket.Dispose();
                    _udpSocket = null;
                }
            }
        }

        #endregion

        #region 异步接收逻辑

        /// <summary>
        /// 当 UDP Socket 异步接收完成时触发
        /// </summary>
        private void OnReceiveCompleted(object sender, SocketAsyncEventArgs e)
        {
            ProcessReceive(e);
        }

        /// <summary>
        /// 处理接收到的数据
        /// </summary>
        /// <param name="e">异步事件参数，包含接收缓冲和远端地址</param>
        private void ProcessReceive(SocketAsyncEventArgs e)
        {
            try
            {
                // 如果没有错误且确实收到数据
                if (e.BytesTransferred > 0 && e.SocketError == SocketError.Success)
                {
                    // 拷贝实际数据（避免引用同一缓冲区）
                    byte[] receivedData = new byte[e.BytesTransferred];
                    Buffer.BlockCopy(e.Buffer, 0, receivedData, 0, e.BytesTransferred);

                    // 触发上层事件回调
                    EventAcceptData?.Invoke(
                        Tuple.Create((IPEndPoint)e.RemoteEndPoint, receivedData, e.BytesTransferred)
                    );
                }
                else
                {
                    // 如果出错，记录详细信息
                    string err = $"[UDP] 接收错误：SocketError = {e.SocketError}, BytesTransferred = {e.BytesTransferred}";
                    //WriteUdpLog("ReceiveError", err);
                    System.IO.File.AppendAllText(System.IO.Path.GetDirectoryName(exepath) + "\\Rcvlog.txt", err + "\r\n");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[UDP] 数据处理异常：{ex.Message}");
            }
            finally
            {
                if (_isRunning && _udpSocket != null)
                {
                    try
                    {
                        // 重置缓冲并继续下一次异步接收
                        e.SetBuffer(0, _recvBuffer.Length);
                        e.RemoteEndPoint = new IPEndPoint(IPAddress.Any, 0);
                        bool willRaiseEvent = _udpSocket.ReceiveFromAsync(e);
                        if (!willRaiseEvent)
                        {
                            ProcessReceive(e);
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                        // Socket 已关闭，忽略
                    }
                }
            }
        }

        #endregion

        #region 数据发送

        /// <summary>
        /// 发送 UDP 数据
        /// </summary>
        /// <param name="remoteEP">目标终端（IP + 端口）</param>
        /// <param name="data">要发送的字节数组</param>
        public async void SendDATA(IPEndPoint remoteEP, byte[] data)
        {
            //if (_udpSocket == null || !_isRunning) return;
            //if (!_isRunning) return;

            try
            {
                if (_udpSocket == null)
                {
                    _udpSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
                }
                for (int i = 0; i < 2000; i++)
                {
                    _udpSocket.SendTo(data, remoteEP);
                    await Task.Delay(1);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[UDP] 发送异常：{ex.Message}");
            }
        }

        #endregion
    }
}
