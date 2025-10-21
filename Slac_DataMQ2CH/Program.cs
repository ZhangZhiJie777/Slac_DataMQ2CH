using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Slac_DataMQ2CH
{
    static class Program
    {
        /// <summary>
        /// 应用程序的主入口点。
        /// </summary>
        [STAThread]
        static void Main()
        {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Run(new frm_mq2ch_V2());
            // frm_mq2ch: 使用PLC时间保存；  frm_mq2ch_V0：使用PC接收时间保存； frm_mq2ch_V2：PLC时间+事件触发拆位
        }
    }
}
