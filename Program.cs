using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using RabbitMQ.Client;

namespace Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("I am Direct Publisher");
            sendMessage();
            sendMessage();
            sendMessage();
            Console.WriteLine("Done!");
            Console.ReadLine();
        }

        private static void sendMessage()
        {
            Thread.Sleep(1000);
            var connectionFactory = new ConnectionFactory();
            IConnection connection = connectionFactory.CreateConnection();
            IModel channel = connection.CreateModel();

            channel.ExchangeDeclare("direct-exchange-example", ExchangeType.Direct);

            string value = DoSomethingInteresting();
            string logMessage = string.Format("{0}: {1}", TraceEventType.Information, value);

            byte[] message = Encoding.UTF8.GetBytes(logMessage);
            channel.BasicPublish("direct-exchange-example", "", null, message);
            Console.WriteLine(string.Format("I am Sending {0}", logMessage));
            channel.Close();
            connection.Close();
            Console.WriteLine("Press Any key to send Next Message....");
            Console.ReadLine();
        }

        static string DoSomethingInteresting()
        {
            return Guid.NewGuid().ToString();
        }
    }
}