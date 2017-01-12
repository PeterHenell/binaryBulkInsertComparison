using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BinarySql
{
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

        internal IEnumerable<int> Pop()
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
