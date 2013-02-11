using System;
using System.Threading;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure;

namespace Azure101.Samples.ServiceBusTopicSubscriber
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Console.WriteLine("Azure 101 Samples: Service Bus Topic Subscriber");
            Console.WriteLine();

            string topicName = null, subscriptionName = null;
            int lowMessageId = 0, highMessageId = 0;

            while (String.IsNullOrEmpty(topicName))
            {
                Console.Write("Please enter the name of the topic that you wish to subscribe to: ");
                topicName = Console.ReadLine().ToLower();
            }

            while (String.IsNullOrEmpty(subscriptionName))
            {
                Console.Write("Please enter the name of the new subscription: ");
                subscriptionName = Console.ReadLine().ToLower();
            }

            Console.Write("Please enter the minimum message ID: ");
            lowMessageId = int.Parse(Console.ReadLine());

            Console.Write("Please enter the maximum message ID: ");
            highMessageId = int.Parse(Console.ReadLine());

            Console.WriteLine();

            string connectionString = CloudConfigurationManager.GetSetting("ServiceBusConnectionString");
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            if (namespaceManager.TopicExists(topicName) == false)
            {
                namespaceManager.CreateTopic(topicName);

                Console.WriteLine("Service bus topic [{0}] created.", topicName);
                Console.WriteLine();
            }

            if (namespaceManager.SubscriptionExists(topicName, subscriptionName) == false)
            {
                namespaceManager.CreateSubscription(topicName, subscriptionName,
                                                    new SqlFilter(String.Format(
                                                        "MessageId >= {0} AND MessageId <= {1}", lowMessageId,
                                                        highMessageId)));

                Console.WriteLine("Service bus subscription [{0}] created.", subscriptionName);
                Console.WriteLine();
            }

            SubscriptionClient subscriptionClient = SubscriptionClient.CreateFromConnectionString(connectionString,
                                                                                                  topicName,
                                                                                                  subscriptionName);

            while (true)
            {
                BrokeredMessage message = subscriptionClient.Receive();

                Console.WriteLine();

                if (message == null)
                    Console.WriteLine("Service bus subscription [{0}] is empty.", subscriptionClient);
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