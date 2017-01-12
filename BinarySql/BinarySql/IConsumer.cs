using System.Data;
namespace BinarySql
{
    interface IConsumer
    {
        void Consume(IDataReader b);
    }
}