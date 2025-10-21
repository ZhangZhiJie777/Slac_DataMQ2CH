using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SQLite;

namespace Slac_DataMQ2CH
{
     public class ConfigSQL
    {
        public static string connStr = @"Data Source=.\System.Data.Sql.db;Initial Catalog=sqlite;Integrated Security=True;Max Pool Size=10";

        #region dashboard 看板列表

        public static DataTable Get_dashboard_list()
        {
            SQLiteConnection conn = new SQLiteConnection(connStr);
            conn.Open();
            string sql = "SELECT * FROM dashboard ";
            SQLiteDataAdapter ap = new SQLiteDataAdapter(sql, conn);
            DataSet ds = new DataSet();
            ap.Fill(ds);
            DataTable dt = ds.Tables[0];
            conn.Close();
            return ds.Tables[0];
        }

        #endregion

        #region data2API 将需要的数据发送到REDIS,供API调用

        public static DataTable Get_data2api_list()
        {
            SQLiteConnection conn = new SQLiteConnection(connStr);
            conn.Open();
            string sql = "SELECT * FROM data2api where enable=1 ";
            SQLiteDataAdapter ap = new SQLiteDataAdapter(sql, conn);
            DataSet ds = new DataSet();
            ap.Fill(ds);
            DataTable dt = ds.Tables[0];
            conn.Close();
            return ds.Tables[0];
        }

        #endregion


    }
}
