using System;
using System.Threading;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure;

namespace Azure101.Samples.ServiceBusTopicPublisher
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Guid publisherId = Guid.NewGuid();

            Console.WriteLine("Azure 101 Samples: Service Bus Topic Publisher");
            Console.WriteLine("Publisher ID: [{0}]", publisherId);
            Console.WriteLine();

            string topicName = null;

            while (String.IsNullOrEmpty(topicName))
            {
                Console.Write("Please enter the name of the topic that you wish to publish to: ");
                topicName = Console.ReadLine().ToLower();
            }

            Console.WriteLine();

            string connectionString = CloudConfigurationManager.GetSetting("ServiceBusConnectionString");
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            if (namespaceManager.TopicExists(topicName) == false)
            {
                namespaceManager.CreateTopic(topicName);

                Console.WriteLine("Service bus topic [{0}] created.", topicName);
                Console.WriteLine();
            }

            var randomizer = new Random();
            TopicClient topicClient = TopicClient.CreateFromConnectionString(connectionString, topicName);

            while (true)
            {
                int messageId = randomizer.Next(1, 10000);
                string messageContents = String.Format("Publisher [{0}] / Message [{1}]", publisherId, messageId);
                var message = new BrokeredMessage(messageContents);

                message.Properties["MessageId"] = messageId;

                topicClient.Send(message);

                Console.WriteLine("Message [{0}] sent.", messageId);

                Thread.Sleep(100);
            }
        }
    }
}