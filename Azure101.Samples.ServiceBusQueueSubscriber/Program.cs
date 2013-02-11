using System;
using System.Threading;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure;

namespace Azure101.Samples.ServiceBusQueueSubscriber
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Console.WriteLine("Azure 101 Samples: Service Bus Queue Subscriber");
            Console.WriteLine();

            string queueName = null;

            while (String.IsNullOrEmpty(queueName))
            {
                Console.Write("Please enter the name of the queue that you wish to subscribe to: ");
                queueName = Console.ReadLine().ToLower();
            }

            Console.WriteLine();

            string connectionString = CloudConfigurationManager.GetSetting("ServiceBusConnectionString");
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            if (namespaceManager.QueueExists(queueName) == false)
            {
                namespaceManager.CreateQueue(queueName);

                Console.WriteLine("Service bus queue [{0}] created.");
                Console.WriteLine();
            }

            QueueClient queueClient = QueueClient.CreateFromConnectionString(connectionString, queueName);

            while (true)
            {
                BrokeredMessage message = queueClient.Receive();

                Console.WriteLine();

                if (message == null)
                    Console.WriteLine("Service bus queue [{0}] is empty.", queueName);
                else
                {
                    try
                    {
                        Console.WriteLine("Message received.");
                        Console.WriteLine("Body: " + message.GetBody<String>());
                        Console.WriteLine("Message ID: " + message.Properties["MessageId"]);

                        message.Complete();
                    }
                    catch (Exception ex)
                    {
                        message.Abandon();
                    }
                }

                Thread.Sleep(100);
            }
        }
    }
}