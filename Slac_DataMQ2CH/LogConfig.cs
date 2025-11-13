using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Slac_DataMQ2File
{
    /// <summary>
    /// 类功能描述：Log日志
    /// </summary>
    public class LogConfig
    {
        #region 旧版日志记录——同步加锁
        //static readonly object o = new object(); // 运行日志锁

        //static readonly object objErr = new object(); // 错误日志锁
        ///// <summary>
        ///// 写运行日志
        ///// </summary>
        ///// <param name="msglocation">文件位置</param>
        ///// <param name="sMsg">日志</param>
        //public static void WriteRunLog(string msglocation, string sMsg)
        //{
        //    try
        //    {
        //        lock (o)
        //        {
        //            string sFileName, sPath;

        //            sPath = AppDomain.CurrentDomain.SetupInformation.ApplicationBase + $"LogFile\\RunLog\\{msglocation.Trim()}";
        //            sFileName = "\\" + DateTime.Now.ToString("yyyy-MM-dd") + ".log";
        //            if (!Directory.Exists(sPath))
        //            {
        //                Directory.CreateDirectory(sPath);
        //            }
        //            using (StreamWriter w = File.AppendText(sPath + sFileName))
        //            {
        //                w.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff") + "  " + sMsg.ToString());
        //                w.Close();
        //            }
        //        }
        //    }
        //    catch
        //    { }
        //}

        ///// <summary>
        ///// 写异常日志
        ///// </summary>
        ///// <param name="sMsg"></param>
        //public static void WriteErrLog(string msglocation, string sMsg)
        //{
        //    try
        //    {
        //        lock (objErr)
        //        {
        //            string sFileName, sPath;

        //            sPath = AppDomain.CurrentDomain.SetupInformation.ApplicationBase + $"LogFile\\ErrLog\\{msglocation.Trim()}";
        //            sFileName = "\\" + DateTime.Now.ToString("yyyy-MM-dd") + ".log";
        //            if (!Directory.Exists(sPath))
        //            {
        //                Directory.CreateDirectory(sPath);
        //            }
        //            using (StreamWriter w = File.AppendText(sPath + sFileName))
        //            {
        //                w.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff") + "  " + sMsg.ToString());
        //                w.Close();
        //            }
        //        }
        //    }
        //    catch
        //    { }
        //}
        #endregion

        #region 新版日志记录——异步多任务模式
        private readonly ConcurrentQueue<Tuple<string, string, string>> logQueue = new ConcurrentQueue<Tuple<string, string, string>>(); // 日志队列
        private readonly CancellationTokenSource cts = new CancellationTokenSource(); // 取消任务
        private readonly Task loggingTask;                                            // 日志任务

        // 单例模式：懒惰初始化（即在第一次请求时创建实例）
        private static readonly Lazy<LogConfig> intence = new Lazy<LogConfig>(() => new LogConfig());

        public static LogConfig Intence => intence.Value; // 获取单例

        /// <summary>
        /// 构造函数，启动后台任务来处理日志队列
        /// </summary>
        private LogConfig()
        {
            // 启动后台任务来处理日志队列
            loggingTask = Task.Run(() => ProcessLogQueue(cts.Token));
        }

        /// <summary>
        /// 添加日志到日志队列
        /// </summary>
        /// <param name="msgtype">日志类型</param>
        /// <param name="msglocation">日志所属模块</param>
        /// <param name="sMsg">日志内容</param>
        public void WriteLog(string msgtype, string msglocation, string sMsg)
        {
            logQueue.Enqueue(new Tuple<string, string, string>(msgtype.Trim(), msglocation.Trim(), sMsg));
        }

        /// <summary>
        /// 处理日志队列
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        private async Task ProcessLogQueue(CancellationToken token)
        {
            while (!token.IsCancellationRequested || logQueue.Count > 0)
            {
                try
                {
                    if (logQueue.TryDequeue(out Tuple<string, string, string> logTuple))
                    {
                        string msgtype = logTuple.Item1;     // 日志类型
                        string msglocation = logTuple.Item2; // 日志位置
                        string msg = logTuple.Item3;         // 日志内容

                        await Task.Run(async () =>
                        {
                            string sFileName, sPath;

                            sPath = AppDomain.CurrentDomain.SetupInformation.ApplicationBase + $"LogFile\\{msgtype.Trim()}\\{msglocation.Trim()}";
                            sFileName = "\\" + DateTime.Now.ToString("yyyy-MM-dd") + ".log";
                            if (!Directory.Exists(sPath))
                            {
                                Directory.CreateDirectory(sPath);
                            }
                            using (StreamWriter w = File.AppendText(sPath + sFileName))
                            {
                                await w.WriteLineAsync(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss fff") + "  " + msg.ToString());
                                w.Close();
                            }
                        });
                    }
                    else
                    {
                        await Task.Delay(1);
                    }
                }
                catch
                { }

            }
        }

        /// <summary>
        /// 结束停止日志任务
        /// </summary>
        public void StopLog()
        {
            try
            {
                cts.Cancel();
                if (loggingTask != null)
                {
                    loggingTask.Wait(2000); // 等待两秒，防止有日志没打印完
                }
                cts.Dispose();
            }
            catch (Exception)
            { }

        }
        #endregion
    }
}
