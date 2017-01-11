using DataReaderTest;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections;

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
                    var con2 = new ColumnConsumer(GetConnectionString(), producer);
                    var con3 = new ColumnConsumer(GetConnectionString(), producer);

                    producer.Start();

                    con1.Start();
                    con2.Start();
                    con3.Start();


                    Thread.Sleep(30000);
                    Console.WriteLine(producer.Produced);
                }
            }
            {
                using (var producer = new Producer(2000))
                {
                    var con1 = new BinaryConsumer(GetConnectionString(), producer);
                    var con2 = new BinaryConsumer(GetConnectionString(), producer);
                    var con3 = new BinaryConsumer(GetConnectionString(), producer);

                    producer.Start();

                    con1.Start();
                    con2.Start();
                    con3.Start();


                    Thread.Sleep(30000);
                    Console.WriteLine(producer.Produced);
                }
            }
        }

        private static string GetConnectionString()
        {
            var builder = new SqlConnectionStringBuilder();
            builder.DataSource = "localhost";
            builder.IntegratedSecurity = true;
            builder.InitialCatalog = "BinaryTesting";
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

        public void Consume(Batch b)
        {
            using (var con = new SqlConnection(connectionString))
            {
                con.Open();
                SqlBulkCopy c = new SqlBulkCopy(con);
                c.DestinationTableName = "peter";
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
                var col = new DataReaderCollection<Measurement>(b.Measurements);
                c.EnableStreaming = true;
                c.WriteToServer(col);
            }
            Console.WriteLine("Commit {0}", Thread.CurrentThread.ManagedThreadId);
        }

        internal void Start()
        {
            var a = new Action(() =>
            {
                foreach (var batch in producer.GetData())
                {
                    Consume(batch);
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

        public void Consume(Batch b)
        {
            using (var con = new SqlConnection(connectionString))
            {
                con.Open();
                SqlBulkCopy c = new SqlBulkCopy(con);
                c.DestinationTableName = "peterBinary";

                var fieldName = "bin";
                c.ColumnMappings.Add(
                        new SqlBulkCopyColumnMapping
                        {
                            DestinationColumn = fieldName,
                            SourceColumn = fieldName,
                            DestinationOrdinal = 1,
                            SourceOrdinal = 50
                        });
                var col = new DataReaderCollection<Measurement>(b.Measurements);
                c.EnableStreaming = true;
                c.WriteToServer(col);
            }
            Console.WriteLine("Commit {0}", Thread.CurrentThread.ManagedThreadId);
        }

        internal void Start()
        {
            var a = new Action(() =>
            {
                foreach (var batch in producer.GetData())
                {
                    Consume(batch);
                    Console.WriteLine("Ate a bunch {0}", Thread.CurrentThread.ManagedThreadId);
                }
            });

            a.BeginInvoke(null, null);
        }
    }

    class Measurement
    {


        public float c0 { get; set; } = float.MaxValue;
        public float c1 { get; set; } = float.MaxValue;
        public float c2 { get; set; } = float.MaxValue;
        public float c3 { get; set; } = float.MaxValue;
        public float c4 { get; set; } = float.MaxValue;
        public float c5 { get; set; } = float.MaxValue;
        public float c6 { get; set; } = float.MaxValue;
        public float c7 { get; set; } = float.MaxValue;
        public float c8 { get; set; } = float.MaxValue;
        public float c9 { get; set; } = float.MaxValue;
        public float c10 { get; set; } = float.MaxValue;
        public float c11 { get; set; } = float.MaxValue;
        public float c12 { get; set; } = float.MaxValue;
        public float c13 { get; set; } = float.MaxValue;
        public float c14 { get; set; } = float.MaxValue;
        public float c15 { get; set; } = float.MaxValue;
        public float c16 { get; set; } = float.MaxValue;
        public float c17 { get; set; } = float.MaxValue;
        public float c18 { get; set; } = float.MaxValue;
        public float c19 { get; set; } = float.MaxValue;
        public float c20 { get; set; } = float.MaxValue;
        public float c21 { get; set; } = float.MaxValue;
        public float c22 { get; set; } = float.MaxValue;
        public float c23 { get; set; } = float.MaxValue;
        public float c24 { get; set; } = float.MaxValue;
        public float c25 { get; set; } = float.MaxValue;
        public float c26 { get; set; } = float.MaxValue;
        public float c27 { get; set; } = float.MaxValue;
        public float c28 { get; set; } = float.MaxValue;
        public float c29 { get; set; } = float.MaxValue;
        public float c30 { get; set; } = float.MaxValue;
        public float c31 { get; set; } = float.MaxValue;
        public float c32 { get; set; } = float.MaxValue;
        public float c33 { get; set; } = float.MaxValue;
        public float c34 { get; set; } = float.MaxValue;
        public float c35 { get; set; } = float.MaxValue;
        public float c36 { get; set; } = float.MaxValue;
        public float c37 { get; set; } = float.MaxValue;
        public float c38 { get; set; } = float.MaxValue;
        public float c39 { get; set; } = float.MaxValue;
        public float c40 { get; set; } = float.MaxValue;
        public float c41 { get; set; } = float.MaxValue;
        public float c42 { get; set; } = float.MaxValue;
        public float c43 { get; set; } = float.MaxValue;
        public float c44 { get; set; } = float.MaxValue;
        public float c45 { get; set; } = float.MaxValue;
        public float c46 { get; set; } = float.MaxValue;
        public float c47 { get; set; } = float.MaxValue;
        public float c48 { get; set; } = float.MaxValue;
        public float c49 { get; set; } = float.MaxValue;
        public byte[] bin { get; set; } = b;

        private static byte[] b;

        static Measurement()
        {
            var sb = new StringBuilder();
            for (int i = 0; i < 50; i++)
            {
                sb.Append(string.Format("{0}|", float.MaxValue));
            }
            b = Encoding.UTF8.GetBytes(sb.ToString());
        }
    }

    class Batch
    {
        public List<Measurement> Measurements { get; set; }

        public Batch()
        {
            Measurements = new List<Measurement>();
        }

        public Batch Fill(int size)
        {
            for (int i = 0; i < size; i++)
            {
                Measurements.Add(new Measurement());
            }
            return this;
        }
    }

    class Producer : IDisposable
    {
        Queue<Batch> batches = new Queue<Batch>();
        object _lock = new object();
        private int batchSize;
        private bool go;

        public int Produced { get; private set; } = 0;

        public Producer(int batchSize)
        {
            this.batchSize = batchSize;
        }

        internal IEnumerable<Batch> GetData()
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
                        batches.Enqueue(new Batch().Fill(batchSize));
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
