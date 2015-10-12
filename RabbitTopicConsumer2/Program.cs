using System;
using System.IO;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var connectionFactory = new ConnectionFactory();
            IConnection connection = connectionFactory.CreateConnection();
            IModel channel = connection.CreateModel();

            channel.ExchangeDeclare("topic-exchange-example", ExchangeType.Topic, false, true, null);
            channel.QueueDeclare("log2", false, false, true, null);
            channel.QueueBind("log2", "topic-exchange-example", "*.Business.*");

            var consumer = new QueueingBasicConsumer(channel);
            channel.BasicConsume("log2", true, consumer);

            Console.WriteLine("I am the Business Consumer");

            while (true)
            {
                try
                {
                    var eventArgs = (BasicDeliverEventArgs)consumer.Queue.Dequeue();
                    string message = Encoding.UTF8.GetString(eventArgs.Body);
                    Console.WriteLine(string.Format("{0} - {1}", eventArgs.RoutingKey, message));
                }
                catch (EndOfStreamException)
                {
                    // The consumer was cancelled, the model closed, or the connection went away.
                    break;
                }
            }

            channel.Close();
            connection.Close();
        }
    }
}