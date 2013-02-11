using System;
using System.Threading;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Azure101.Samples.QueueStoragePublisher
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Guid publisherId = Guid.NewGuid();

            Console.WriteLine("Azure 101 Samples: Queue Storage Publisher");
            Console.WriteLine("Publisher ID: [{0}]", publisherId);
            Console.WriteLine();

            string queueName = null;

            while (String.IsNullOrEmpty(queueName))
            {
                Console.Write("Please enter the name of the queue that you wish to publish to: ");
                queueName = Console.ReadLine().ToLower();
            }

            Console.WriteLine();

            CloudStorageAccount storageAccount =
                CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));

            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudQueue queueReference = queueClient.GetQueueReference(queueName);

            queueReference.CreateIfNotExists();

            int messageCount = 0;

            while (true)
            {
                messageCount++;
                string messageContent = String.Format("Publisher [{0}] / Message [{1}]", publisherId, messageCount);
                queueReference.AddMessage(new CloudQueueMessage(messageContent));
                Console.WriteLine("Message [{0}] published.", messageCount);
                Thread.Sleep(100);
            }
        }
    }
}