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
                    var con1 = new ColumnConsumer(GetConnectionString(), producer);

                    producer.Start();

                    con1.Start();

                    Thread.Sleep(30000);
                    Console.WriteLine(producer.Produced);
                }
            }
            {
                using (var producer = new Producer(2000))
                {
                    var con1 = new BinaryConsumer(GetConnectionString(), producer);

                    producer.Start();

                    con1.Start();

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

    class ColumnConsumer : IConsumer
    {
        private readonly string connectionString;
        private Producer producer;

        public ColumnConsumer(string connectionString, Producer producer)
        {
            this.producer = producer;
            this.connectionString = connectionString;
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
                c.DestinationTableName = "ManyColumns";
                c.BatchSize = 2000;
                for (int i = 0; i < 50; i++)
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
                c.EnableStreaming = true;
                c.WriteToServer(b);
                tran.Commit();
            }
            Console.WriteLine("Commit {0}", Thread.CurrentThread.ManagedThreadId);
        }

        internal void Start()
        {
            var a = new Action(() =>
            {
                foreach (var size in producer.GetData())
                {
                    Consume(new Batch().Fill(size));
                    Console.WriteLine("Ate a bunch {0}", Thread.CurrentThread.ManagedThreadId);
                }
            });

            a.BeginInvoke(null, null);
        }
    }

    class BinaryConsumer : IConsumer
    {
        private readonly string connectionString;
        private Producer producer;

        public BinaryConsumer(string connectionString, Producer producer)
        {
            this.producer = producer;
            this.connectionString = connectionString;
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
                c.DestinationTableName = "OneBigBinaryColumn";

                var fieldName = "bin";
                c.ColumnMappings.Add(
                        new SqlBulkCopyColumnMapping
                        {
                            DestinationColumn = fieldName,
                            SourceColumn = fieldName,
                            DestinationOrdinal = 1,
                            SourceOrdinal = 0
                        });
                //var col = new DataReaderCollection<BinaryMeasurement>(b.Measurements);
                c.EnableStreaming = true;
                c.WriteToServer(b);
                tran.Commit();
            }
            Console.WriteLine("Commit {0}", Thread.CurrentThread.ManagedThreadId);
        }

        internal void Start()
        {
            var a = new Action(() =>
            {
                foreach (var size in producer.GetData())
                {
                    Consume(new BinaryBatch().Fill(size));
                    Console.WriteLine("Ate a bunch {0}", Thread.CurrentThread.ManagedThreadId);
                }
            });

            a.BeginInvoke(null, null);
        }
    }


    class BinaryBatch : IDataReader
    {
        int current = 0;
        private int size;

        private static byte[] b;

        static BinaryBatch()
        {
            var sb = new StringBuilder();
            for (int i = 0; i < 50; i++)
            {
                sb.Append(string.Format("{0}|", float.MaxValue));
            }
            b = Encoding.UTF8.GetBytes(sb.ToString());
        }

        public BinaryBatch()
        {
            current = 0;
        }

        public BinaryBatch Fill(int size)
        {
            this.size = size;
            return this;
        }

        public void Close()
        {
        }

        public int Depth
        {
            get { throw new NotImplementedException(); }
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public bool IsClosed
        {
            get { throw new NotImplementedException(); }
        }

        public bool NextResult()
        {
            current++;
            return current < size;
        }

        public bool Read()
        {
            current++;
            return current < size;
        }

        public int RecordsAffected
        {
            get { return 1; }
        }

        public void Dispose()
        {
        }

        public int FieldCount
        {
            get { return 1; }
        }

        public bool GetBoolean(int i)
        {
            throw new NotImplementedException();
        }

        public byte GetByte(int i)
        {
            throw new NotImplementedException();
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            throw new NotImplementedException();
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i)
        {
            throw new NotImplementedException();
        }

        public decimal GetDecimal(int i)
        {
            throw new NotImplementedException();
        }

        public double GetDouble(int i)
        {
            throw new NotImplementedException();
        }

        public Type GetFieldType(int i)
        {
            throw new NotImplementedException();
        }

        public float GetFloat(int i)
        {
            throw new NotImplementedException();
        }

        public Guid GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        public short GetInt16(int i)
        {
            throw new NotImplementedException();
        }

        public int GetInt32(int i)
        {
            throw new NotImplementedException();
        }

        public long GetInt64(int i)
        {
            throw new NotImplementedException();
        }

        public string GetName(int i)
        {
            throw new NotImplementedException();
        }

        public int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        public string GetString(int i)
        {
            throw new NotImplementedException();
        }

        public object GetValue(int i)
        {
            return b;
        }

        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            return false;
        }

        public object this[string name]
        {
            get { throw new NotImplementedException(); }
        }

        public object this[int i]
        {
            get { throw new NotImplementedException(); }
        }
    }


    class Batch : IDataReader
    {
        int size;
        int current = 0;

        public Batch()
        {
            current = 0;
        }

        public Batch Fill(int size)
        {
            this.size = size;
            return this;
        }

        public void Close()
        {
        }

        public int Depth
        {
            get { throw new NotImplementedException(); }
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public bool IsClosed
        {
            get { throw new NotImplementedException(); }
        }

        public bool NextResult()
        {
            current++;
            return current < size;
        }

        public bool Read()
        {
            current++;
            return current < size;
        }

        public int RecordsAffected
        {
            get { return 1; }
        }

        public void Dispose()
        {
        }

        public int FieldCount
        {
            get { return 51; }
        }

        public bool GetBoolean(int i)
        {
            throw new NotImplementedException();
        }

        public byte GetByte(int i)
        {
            throw new NotImplementedException();
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            throw new NotImplementedException();
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i)
        {
            throw new NotImplementedException();
        }

        public decimal GetDecimal(int i)
        {
            throw new NotImplementedException();
        }

        public double GetDouble(int i)
        {
            throw new NotImplementedException();
        }

        public Type GetFieldType(int i)
        {
            throw new NotImplementedException();
        }

        public float GetFloat(int i)
        {
            throw new NotImplementedException();
        }

        public Guid GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        public short GetInt16(int i)
        {
            throw new NotImplementedException();
        }

        public int GetInt32(int i)
        {
            throw new NotImplementedException();
        }

        public long GetInt64(int i)
        {
            throw new NotImplementedException();
        }

        public string GetName(int i)
        {
            throw new NotImplementedException();
        }

        public int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        public string GetString(int i)
        {
            throw new NotImplementedException();
        }

        public object GetValue(int i)
        {
            return float.MaxValue;
        }

        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            return false;
        }

        public object this[string name]
        {
            get { throw new NotImplementedException(); }
        }

        public object this[int i]
        {
            get { throw new NotImplementedException(); }
        }
    }

    class Producer : IDisposable
    {
        Queue<int> batches = new Queue<int>();
        object _lock = new object();
        private int batchSize;
        private bool go;

        public int Produced { get; private set; }

        public Producer(int batchSize)
        {
            Produced = 0;
            this.batchSize = batchSize;
        }

        internal IEnumerable<int> GetData()
        {
            while (true)
            {
                lock (_lock)
                {
                    if (batches.Count > 0)
                    {
                        yield return batches.Dequeue();
                        Produced += batchSize;
                    }
                }
            }
        }

        internal void Start()
        {
            go = true;
            var a = new Action(() =>
            {
                while (go)
                {
                    if (batches.Count < 10)
                    {
                        batches.Enqueue(batchSize);
                    }
                    Thread.Sleep(10);
                }

            });
            a.BeginInvoke(null, null);
        }

        public void Dispose()
        {
            go = false;
            batches.Clear();
        }
    }
}

