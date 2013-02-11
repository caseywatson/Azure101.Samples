using System;
using System.Threading;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure;

namespace Azure101.Samples.ServiceBusQueuePublisher
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Guid publisherId = Guid.NewGuid();

            Console.WriteLine("Azure 101 Samples: Service Bus Queue Publisher");
            Console.WriteLine("Publisher ID: [{0}]", publisherId);
            Console.WriteLine();

            string queueName = null;

            while (String.IsNullOrEmpty(queueName))
            {
                Console.Write("Please enter the name of the queue that you wish to publish to: ");
                queueName = Console.ReadLine().ToLower();
            }

            Console.WriteLine();

            string connectionString = CloudConfigurationManager.GetSetting("ServiceBusConnectionString");
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            if (namespaceManager.QueueExists(queueName) == false)
            {
                namespaceManager.CreateQueue(queueName);

                Console.WriteLine("Service bus queue [{0}] created.", queueName);
                Console.WriteLine();
            }

            var randomizer = new Random();
            QueueClient queueClient = QueueClient.CreateFromConnectionString(connectionString, queueName);

            while (true)
            {
                int messageId = randomizer.Next(1, 10000);
                string messageContents = String.Format("Publisher [{0}] / Message [{1}]", publisherId, messageId);
                var message = new BrokeredMessage(messageContents);

                message.Properties["MessageId"] = messageId;

                queueClient.Send(message);

                Console.WriteLine("Message [{0}] sent.", messageId);

                Thread.Sleep(100);
            }
        }
    }
}