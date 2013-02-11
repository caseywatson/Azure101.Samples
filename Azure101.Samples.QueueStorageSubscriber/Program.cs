using System;
using System.Threading;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Azure101.Samples.QueueStorageSubscriber
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Console.WriteLine("Azure 101 Samples: Queue Storage Publisher");
            Console.WriteLine();

            string queueName = null;

            while (String.IsNullOrEmpty(queueName))
            {
                Console.Write("Please enter the naem of the queue that you wish to subscribe to: ");
                queueName = Console.ReadLine().ToLower();
            }

            Console.WriteLine();

            CloudStorageAccount storageAccount =
                CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));

            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudQueue queueReference = queueClient.GetQueueReference(queueName);

            queueReference.CreateIfNotExists();

            while (true)
            {
                CloudQueueMessage nextMessage = queueReference.GetMessage();

                if (nextMessage == null)
                    Console.WriteLine("Queue [{0}] is empty.", queueName);
                else
                {
                    Console.WriteLine("Message [{0}] received.", nextMessage.AsString);
                    queueReference.DeleteMessage(nextMessage);
                }

                Thread.Sleep(100);
            }
        }
    }
}