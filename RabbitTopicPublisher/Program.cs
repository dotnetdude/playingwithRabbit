using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using RabbitMQ.Client;

namespace Producer
{
    public enum Sector
    {
        Personal,
        Business
    }

    interface ILogger
    {
        void Write(Sector sector, string entry, TraceEventType traceEventType);
    }

    class RabbitLogger : ILogger, IDisposable
    {
        readonly long _clientId;
        readonly IModel _channel;
        bool _disposed;

        public RabbitLogger(IConnection connection, long clientId)
        {
            _clientId = clientId;
            _channel = connection.CreateModel();
            _channel.ExchangeDeclare("topic-exchange-example", ExchangeType.Topic, false, true, null);
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                if (_channel != null && _channel.IsOpen)
                {
                    _channel.Close();
                }
            }
            GC.SuppressFinalize(this);
        }

        public void Write(Sector sector, string entry, TraceEventType traceEventType)
        {
            byte[] message = Encoding.UTF8.GetBytes(Guid.NewGuid() + entry);
            string routingKey = string.Format("{0}.{1}.{2}", _clientId, sector.ToString(), traceEventType.ToString());
            _channel.BasicPublish("topic-exchange-example", routingKey, null, message);
            Console.WriteLine(String.Format("I am sending {0},{1}", routingKey, entry));
        }

        ~RabbitLogger()
        {
            Dispose();
        }
    }

    class Program
    {
        const long ClientId = 10843;

        static void Main(string[] args)
        {
            var connectionFactory = new ConnectionFactory();
            IConnection connection = connectionFactory.CreateConnection();

            TimeSpan time = TimeSpan.FromSeconds(600);
            var stopwatch = new Stopwatch();
            Console.WriteLine("I am the Topic Publisher");
            Console.WriteLine("Running for {0} seconds", time.ToString("ss"));
            stopwatch.Start();

            while (stopwatch.Elapsed < time)
            {
                using (var logger = new RabbitLogger(connection, ClientId))
                {
                    Console.Write("Time to complete: {0} seconds\r", (time - stopwatch.Elapsed).ToString("ss"));
                    logger.Write(Sector.Personal, "This is an information message", TraceEventType.Information);
                    logger.Write(Sector.Personal, "This is an information message", TraceEventType.Transfer);
                    logger.Write(Sector.Business, "This is an warning message", TraceEventType.Warning);
                    logger.Write(Sector.Business, "This is an error message", TraceEventType.Error);
                    //Thread.Sleep(1000);
                }
            }

            connection.Close();
            Console.Write("                             \r");
            Console.WriteLine("Press any key to exit");
            Console.ReadKey();
        }
    }
}