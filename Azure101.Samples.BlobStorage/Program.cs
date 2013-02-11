using System;
using System.IO;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Azure101.Samples.BlobStorage
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Console.WriteLine("Azure 101 Samples: Blob Storage");
            Console.WriteLine();

            // CreateBlobContainer();
            // CreateRootBlobContainer();
            // UploadFileToBlobContainer();
            // DownloadFileFromBlobContainer();
            // MakeBlobContainerPubliclyAccessible();
            // ListBlobContainerContents();
            // DeleteBlob();
            // DeleteBlobContainer();

            Console.WriteLine();
            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }

        private static CloudStorageAccount GetCloudStorageAccount()
        {
            return CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));
        }

        private static void CreateBlobContainer()
        {
            CloudStorageAccount cloudStorageAccount = GetCloudStorageAccount();
            CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();

            string containerName = null;

            while (String.IsNullOrEmpty(containerName))
            {
                Console.Write("Please enter container name: ");
                containerName = Console.ReadLine().ToLower();
            }

            Console.WriteLine();

            CloudBlobContainer containerReference = blobClient.GetContainerReference(containerName);

            containerReference.CreateIfNotExists();

            Console.WriteLine("Blob container [{0}] created.", containerName);
        }

        private static void MakeBlobContainerPubliclyAccessible()
        {
            CloudStorageAccount cloudStorageAccount = GetCloudStorageAccount();
            CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();

            string containerName = null;

            while (String.IsNullOrEmpty(containerName))
            {
                Console.Write("Please enter container name: ");
                containerName = Console.ReadLine().ToLower();
            }

            Console.WriteLine();

            CloudBlobContainer containerReference = blobClient.GetContainerReference(containerName);

            containerReference.CreateIfNotExists();

            containerReference.SetPermissions(new BlobContainerPermissions
                {
                    PublicAccess = BlobContainerPublicAccessType.Container
                });

            Console.WriteLine("Blob container [{0}] is now publicly accessible.", containerName);
        }

        private static void CreateRootBlobContainer()
        {
            CloudStorageAccount cloudStorageAccount = GetCloudStorageAccount();
            CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();

            CloudBlobContainer containerReference = blobClient.GetContainerReference("$root");

            containerReference.CreateIfNotExists();

            Console.WriteLine("Root blob container [$root] created.");
        }

        private static void UploadFileToBlobContainer()
        {
            CloudStorageAccount cloudStorageAccount = GetCloudStorageAccount();
            CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();

            string containerName = null, filePath = null;

            while (String.IsNullOrEmpty(containerName))
            {
                Console.Write("Please enter container name: ");
                containerName = Console.ReadLine().ToLower();
            }

            while (String.IsNullOrEmpty(filePath))
            {
                Console.Write("Please enter the full path of the file you wish to upload: ");
                filePath = Console.ReadLine();
            }

            Console.WriteLine();

            CloudBlobContainer containerReference = blobClient.GetContainerReference(containerName);
            containerReference.CreateIfNotExists();

            CloudBlockBlob blobReference = containerReference.GetBlockBlobReference(Path.GetFileName(filePath).ToLower());
            blobReference.UploadFromStream(File.OpenRead(filePath));

            Console.WriteLine("Blob [{0}] uploaded.", blobReference.Uri);
        }

        private static void DownloadFileFromBlobContainer()
        {
            CloudStorageAccount cloudStorageAccount = GetCloudStorageAccount();
            CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();

            string containerName = null, blobName = null, filePath = null;

            while (String.IsNullOrEmpty(containerName))
            {
                Console.Write("Please enter container name: ");
                containerName = Console.ReadLine().ToLower();
            }

            while (String.IsNullOrEmpty(blobName))
            {
                Console.Write("Please enter blob name: ");
                blobName = Console.ReadLine().ToLower();
            }

            while (String.IsNullOrEmpty(filePath))
            {
                Console.Write("Please enter the full path of the file you wish to download: ");
                filePath = Console.ReadLine();
            }

            Console.WriteLine();

            CloudBlobContainer containerReference = blobClient.GetContainerReference(containerName);
            containerReference.CreateIfNotExists();

            CloudBlockBlob blobReference = containerReference.GetBlockBlobReference(blobName);

            using (FileStream fileStream = File.Create(filePath))
                blobReference.DownloadToStream(fileStream);

            Console.WriteLine("Blob [{0}] downloaded.", blobReference.Uri);
        }

        private static void DeleteBlobContainer()
        {
            CloudStorageAccount cloudStorageAccount = GetCloudStorageAccount();
            CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();

            string containerName = null;

            while (String.IsNullOrEmpty(containerName))
            {
                Console.Write("Please enter container name: ");
                containerName = Console.ReadLine().ToLower();
            }

            Console.WriteLine();

            CloudBlobContainer containerReference = blobClient.GetContainerReference(containerName);

            containerReference.DeleteIfExists();

            Console.WriteLine("Blob container [{0}] deleted.", containerName);
        }

        private static void DeleteBlob()
        {
            CloudStorageAccount cloudStorageAccount = GetCloudStorageAccount();
            CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();

            string containerName = null, blobName = null;

            while (String.IsNullOrEmpty(containerName))
            {
                Console.Write("Please enter container name: ");
                containerName = Console.ReadLine().ToLower();
            }

            while (String.IsNullOrEmpty(blobName))
            {
                Console.Write("Please enter blob name: ");
                blobName = Console.ReadLine().ToLower();
            }

            Console.WriteLine();

            CloudBlobContainer containerReference = blobClient.GetContainerReference(containerName);
            CloudBlockBlob blobReference = containerReference.GetBlockBlobReference(blobName);

            blobReference.DeleteIfExists();

            Console.WriteLine("Blob [{0}/{1}] deleted.", containerName, blobName);
        }

        private static void ListBlobContainerContents()
        {
            CloudStorageAccount cloudStorageAccount = GetCloudStorageAccount();
            CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();

            string containerName = null;

            while (String.IsNullOrEmpty(containerName))
            {
                Console.Write("Please enter container name: ");
                containerName = Console.ReadLine().ToLower();
            }

            CloudBlobContainer containerReference = blobClient.GetContainerReference(containerName);

            foreach (IListBlobItem blobItem in containerReference.ListBlobs())
            {
                Console.WriteLine();

                if (blobItem is CloudBlockBlob)
                    Console.WriteLine("Block Blob: [{0}]", blobItem.Uri);
                else if (blobItem is CloudPageBlob)
                    Console.WriteLine("Page Blob: [{0}]", blobItem.Uri);
                else if (blobItem is CloudBlobDirectory)
                    Console.WriteLine("Blob Directory: [{0}]", blobItem.Uri);
            }
        }
    }
}