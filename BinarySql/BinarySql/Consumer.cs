using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BinarySql
{
    class Consumer
    {
        private readonly string connectionString;
        private Producer producer;
        private string targetTable;

        public Consumer(string connectionString, Producer producer, string targetTableName)
        {
            this.producer = producer;
            this.connectionString = connectionString;
            this.targetTable = targetTableName;
        }

        public void Consume(IDataReader b)
        {
            using (var con = new SqlConnection(connectionString))
            {
                con.Open();
                var tran = con.BeginTransaction();
                using (var cmd = new SqlCommand("DBCC TRACEON(610)", con, tran))
                {
                    cmd.ExecuteNonQuery();
                }

                SqlBulkCopy c = new SqlBulkCopy(con, SqlBulkCopyOptions.TableLock, tran);
                c.DestinationTableName = targetTable;
                //c.BatchSize = 2000;
                for (int i = 0; i < b.FieldCount; i++)
                {
                    var fieldName = string.Format("C{0}", i);
                    c.ColumnMappings.Add(
                        new SqlBulkCopyColumnMapping
                        {
                            DestinationColumn = fieldName,
                            SourceColumn = fieldName,
                            DestinationOrdinal = i,
                            SourceOrdinal = i
                        });
                }
                //c.EnableStreaming = true;
                c.WriteToServer(b);
                tran.Commit();
            }
            Console.WriteLine("Commit {0}", Thread.CurrentThread.ManagedThreadId);
        }

        internal void Start(int numColumns, object value)
        {
            var a = new Action(() =>
            {
                foreach (var batchSize in producer.Pop())
                {
                    Consume(new Batch(numColumns, batchSize, value));
                    Console.WriteLine("Ate a bunch {0}", Thread.CurrentThread.ManagedThreadId);
                }
            });

            a.BeginInvoke(null, null);
        }
    }
}
