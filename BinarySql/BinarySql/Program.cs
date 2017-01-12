using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections;
using System.Data;

namespace BinarySql
{
    class Program
    {
        static void Main(string[] args)
        {

            {
                using (var producer = new Producer(2000))
                {
                    var con1 = new Consumer(GetConnectionString(), producer, "ManyColumns");

                    producer.Start();

                    con1.Start(50, float.MaxValue);

                    Thread.Sleep(30000);
                    Console.WriteLine(producer.Produced);
                }
            }
            {
                using (var producer = new Producer(2000))
                {
                    var con1 = new Consumer(GetConnectionString(), producer, "OneBigBinaryColumn");

                    producer.Start();
                    var sb = new StringBuilder();
                    for (int i = 0; i < 50; i++)
                    {
                        sb.Append(string.Format("{0}|", float.MaxValue));
                    }
                    var b = Encoding.UTF8.GetBytes(sb.ToString());

                    con1.Start(1, b);

                    Thread.Sleep(30000);
                    Console.WriteLine(producer.Produced);
                }
            }
        }

        private static string GetConnectionString()
        {
            var builder = new SqlConnectionStringBuilder();
            builder.DataSource = "localhost\\peheintegration";
            builder.IntegratedSecurity = true;
            builder.InitialCatalog = "BinaryTestGround";
            return builder.ToString();
        }
    }
}

