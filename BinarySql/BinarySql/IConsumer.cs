namespace BinarySql
{
    interface IConsumer
    {
        void Consume(Batch b);
    }
}