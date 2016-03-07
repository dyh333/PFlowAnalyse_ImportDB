using System;
using System.Collections.Generic;
using System.Data.OracleClient;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Sipbus_PFlowAnalyse_ImportSQLServer
{
    class Program
    {
        private const string SqlConnStr = "Data Source=172.24.192.74,8033;Initial Catalog=sipbus_pflow;User ID=sa;Password=15year@sipac";
        private const string OclConnStr = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.84.58) (PORT=8521)))(CONNECT_DATA=(SERVICE_NAME=orcl.sipsd.local)));Persist Security Info=True;User Id=dm; Password=geoneds123";

        private static string[] tables = new string[] { "history_z_line_stand_od", "history_z_line_stand_od_detail", "history_z_stand_od", "history_z_transfer", "history_z_punctuality" };
        private static string date = "20160123" ; //DateTime.Now.AddDays(-2).Date.ToString("yyyy-MM-dd");

        static void Main(string[] args)
        {
            //打开Oracle连接   
            OracleConnection oclConn = new OracleConnection(OclConnStr);
            oclConn.Open();
            //打开Sqlserver连接   
            SqlConnection sqlConn = new SqlConnection(SqlConnStr);
            sqlConn.Open();

            try
            {
                foreach (string tableName in tables)
                {
                    //Oracle读取数据
                    OracleCommand com = oclConn.CreateCommand();
                    com.CommandText = string.Format("Select * From {0} where WHICH_DAY_INT = '{1}'", tableName, date);
                    OracleDataReader odr = com.ExecuteReader();

                    //删除旧数据
                    using (SqlCommand command = new SqlCommand(string.Format("DELETE FROM {0} WHERE WHICH_DAY_INT = '{1}'", tableName, date), sqlConn))
                    {
                        command.ExecuteNonQuery();
                    }

                    //开始转移数据
                    SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn);
                    bulkCopy.BulkCopyTimeout = 3000;
                    bulkCopy.DestinationTableName = string.Format("dbo.{0}", tableName);

                    bulkCopy.WriteToServer(odr);

                    //关闭reader
                    odr.Close();

                    Console.WriteLine(tableName + "执行成功");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("执行失败");

                string path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), @"ErrorLog.txt");
                FileStream fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Write);
                StreamWriter m_streamWriter = new StreamWriter(fs);
                m_streamWriter.BaseStream.Seek(0, SeekOrigin.End);
                m_streamWriter.WriteLine("Error:         时间" + DateTime.Now.ToString() + ex.Message.ToString() + "-执行失败" + "\n");
                m_streamWriter.Flush();
                m_streamWriter.Close();

                fs.Close();
            }
            finally
            {
                //关闭打开的连接   
                oclConn.Close();
                sqlConn.Close();

                //Console.Read();
            }
        }
    }
}
