using System;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Azure101.Samples.TableStorage
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Console.WriteLine("Azure 101 Samples: Table Storage");
            Console.WriteLine();

            //InsertNewPersonRecords();
            //InsertNewPersonRecordsAsBatch();
            //InsertOrReplaceNewPersonRecords();
            //RetrieveSinglePersonRecord();
            //RetrieveAllPersonRecordsInPartition();
            //RetrieveAllPersonRecordsInPartitionWithFirstName();
            //DeleteSinglePersonRecord();
            //DeletePeopleTable();

            Console.WriteLine();
            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }

        private static CloudStorageAccount GetCloudStorageAccount()
        {
            return CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));
        }

        private static void InsertNewPersonRecords()
        {
            CloudStorageAccount cloudStorageAccount = GetCloudStorageAccount();
            CloudTableClient tableClient = cloudStorageAccount.CreateCloudTableClient();
            CloudTable tableReference = tableClient.GetTableReference("People");

            tableReference.CreateIfNotExists();

            while (true)
            {
                Console.WriteLine();

                var person = new Person();

                while (String.IsNullOrEmpty(person.FirstName))
                {
                    Console.Write("Please enter your first name: ");
                    person.FirstName = Console.ReadLine();
                }

                while (String.IsNullOrEmpty(person.LastName))
                {
                    Console.Write("Please enter your last name: ");
                    person.LastName = Console.ReadLine();
                }

                while (String.IsNullOrEmpty(person.RowKey))
                {
                    Console.Write("Please enter your e-mail address: ");
                    person.RowKey = Console.ReadLine();
                }

                while (String.IsNullOrEmpty(person.PartitionKey))
                {
                    Console.Write("Please enter your year of birth: ");
                    person.PartitionKey = Console.ReadLine();
                }

                TableOperation insertOperation = TableOperation.Insert(person);

                tableReference.Execute(insertOperation);

                Console.WriteLine();
                Console.WriteLine("New record inserted.");
                Console.WriteLine();
                Console.Write("Would you like to enter another record? (Y or N): ");

                if (Console.ReadLine().ToUpper() != "Y")
                    break;
            }
        }

        private static void InsertOrReplaceNewPersonRecords()
        {
            CloudStorageAccount cloudStorageAccount = GetCloudStorageAccount();
            CloudTableClient tableClient = cloudStorageAccount.CreateCloudTableClient();
            CloudTable tableReference = tableClient.GetTableReference("People");

            tableReference.CreateIfNotExists();

            while (true)
            {
                Console.WriteLine();

                var person = new Person();

                while (String.IsNullOrEmpty(person.FirstName))
                {
                    Console.Write("Please enter your first name: ");
                    person.FirstName = Console.ReadLine();
                }

                while (String.IsNullOrEmpty(person.LastName))
                {
                    Console.Write("Please enter your last name: ");
                    person.LastName = Console.ReadLine();
                }

                while (String.IsNullOrEmpty(person.RowKey))
                {
                    Console.Write("Please enter your e-mail address: ");
                    person.RowKey = Console.ReadLine();
                }

                while (String.IsNullOrEmpty(person.PartitionKey))
                {
                    Console.Write("Please enter your year of birth: ");
                    person.PartitionKey = Console.ReadLine();
                }

                TableOperation insertOrReplaceOperation = TableOperation.InsertOrReplace(person);

                tableReference.Execute(insertOrReplaceOperation);

                Console.WriteLine();
                Console.WriteLine("New record inserted or replaced.");
                Console.WriteLine();
                Console.Write("Would you like to enter another record? (Y or N): ");

                if (Console.ReadLine().ToUpper() != "Y")
                    break;
            }
        }

        private static void InsertNewPersonRecordsAsBatch()
        {
            CloudStorageAccount cloudStorageAccount = GetCloudStorageAccount();
            CloudTableClient tableClient = cloudStorageAccount.CreateCloudTableClient();
            CloudTable tableReference = tableClient.GetTableReference("People");

            tableReference.CreateIfNotExists();

            var batchOperation = new TableBatchOperation();

            while (true)
            {
                Console.WriteLine();

                var person = new Person();

                while (String.IsNullOrEmpty(person.FirstName))
                {
                    Console.Write("Please enter your first name: ");
                    person.FirstName = Console.ReadLine();
                }

                while (String.IsNullOrEmpty(person.LastName))
                {
                    Console.Write("Please enter your last name: ");
                    person.LastName = Console.ReadLine();
                }

                while (String.IsNullOrEmpty(person.RowKey))
                {
                    Console.Write("Please enter your e-mail address: ");
                    person.RowKey = Console.ReadLine();
                }

                while (String.IsNullOrEmpty(person.PartitionKey))
                {
                    Console.Write("Please enter your year of birth: ");
                    person.PartitionKey = Console.ReadLine();
                }

                TableOperation insertOperation = TableOperation.Insert(person);

                batchOperation.Add(insertOperation);

                Console.WriteLine();
                Console.Write("Would you like to enter another record? (Y or N): ");

                if (Console.ReadLine().ToUpper() != "Y")
                {
                    tableReference.ExecuteBatch(batchOperation);
                    break;
                }
            }
        }

        private static void RetrieveSinglePersonRecord()
        {
            CloudStorageAccount cloudStorageAccount = GetCloudStorageAccount();
            CloudTableClient tableClient = cloudStorageAccount.CreateCloudTableClient();
            CloudTable tableReference = tableClient.GetTableReference("People");

            tableReference.CreateIfNotExists();

            string rowKey = null, partitionKey = null;

            while (String.IsNullOrEmpty(rowKey))
            {
                Console.Write("Please enter e-mail address: ");
                rowKey = Console.ReadLine();
            }

            while (String.IsNullOrEmpty(partitionKey))
            {
                Console.Write("Please enter birth year: ");
                partitionKey = Console.ReadLine();
            }

            Console.WriteLine();

            TableOperation retrieveOperation = TableOperation.Retrieve<Person>(partitionKey, rowKey);
            TableResult retrieveResult = tableReference.Execute(retrieveOperation);

            if (retrieveResult.Result == null)
                Console.WriteLine("The requested person was not found.");
            else
            {
                var person = (retrieveResult.Result as Person);

                Console.WriteLine("Record found.");
                Console.WriteLine();
                Console.WriteLine("First Name: " + person.FirstName);
                Console.WriteLine("Last Name: " + person.LastName);
                Console.WriteLine("E-Mail Address: " + person.RowKey);
                Console.WriteLine("Year of Birth: " + person.PartitionKey);
            }
        }

        private static void DeleteSinglePersonRecord()
        {
            CloudStorageAccount cloudStorageAccount = GetCloudStorageAccount();
            CloudTableClient tableClient = cloudStorageAccount.CreateCloudTableClient();
            CloudTable tableReference = tableClient.GetTableReference("People");

            tableReference.CreateIfNotExists();

            string rowKey = null, partitionKey = null;

            while (String.IsNullOrEmpty(rowKey))
            {
                Console.Write("Please enter e-mail address: ");
                rowKey = Console.ReadLine();
            }

            while (String.IsNullOrEmpty(partitionKey))
            {
                Console.Write("Please enter birth year: ");
                partitionKey = Console.ReadLine();
            }

            Console.WriteLine();

            TableOperation retrieveOperation = TableOperation.Retrieve<Person>(partitionKey, rowKey);
            TableResult retrieveResult = tableReference.Execute(retrieveOperation);

            if (retrieveResult.Result == null)
                Console.WriteLine("The requested person was not found.");
            else
            {
                var person = (retrieveResult.Result as Person);
                TableOperation deleteOperation = TableOperation.Delete(person);

                tableReference.Execute(deleteOperation);

                Console.WriteLine("Record deleted.");
            }
        }

        private static void RetrieveAllPersonRecordsInPartition()
        {
            CloudStorageAccount cloudStorageAccount = GetCloudStorageAccount();
            CloudTableClient tableClient = cloudStorageAccount.CreateCloudTableClient();
            CloudTable tableReference = tableClient.GetTableReference("People");

            tableReference.CreateIfNotExists();

            string partitionKey = null;

            while (String.IsNullOrEmpty(partitionKey))
            {
                Console.Write("Please enter birth year: ");
                partitionKey = Console.ReadLine();
            }

            TableQuery<Person> tableQuery = new TableQuery<Person>().Where(
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

            foreach (Person person in tableReference.ExecuteQuery(tableQuery))
            {
                Console.WriteLine();
                Console.WriteLine("First Name: " + person.FirstName);
                Console.WriteLine("Last Name: " + person.LastName);
                Console.WriteLine("E-Mail Address: " + person.RowKey);
                Console.WriteLine("Year of Birth: " + person.PartitionKey);
            }

            Console.WriteLine();
        }

        private static void RetrieveAllPersonRecordsInPartitionWithFirstName()
        {
            CloudStorageAccount cloudStorageAccount = GetCloudStorageAccount();
            CloudTableClient tableClient = cloudStorageAccount.CreateCloudTableClient();
            CloudTable tableReference = tableClient.GetTableReference("People");

            tableReference.CreateIfNotExists();

            string partitionKey = null, firstName = null;

            while (String.IsNullOrEmpty(partitionKey))
            {
                Console.Write("Please enter birth year: ");
                partitionKey = Console.ReadLine();
            }

            while (String.IsNullOrEmpty(firstName))
            {
                Console.Write("Please enter first name: ");
                firstName = Console.ReadLine();
            }

            TableQuery<Person> tableQuery = new TableQuery<Person>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("FirstName", QueryComparisons.Equal, firstName)));

            foreach (Person person in tableReference.ExecuteQuery(tableQuery))
            {
                Console.WriteLine();
                Console.WriteLine("First Name: " + person.FirstName);
                Console.WriteLine("Last Name: " + person.LastName);
                Console.WriteLine("E-Mail Address: " + person.RowKey);
                Console.WriteLine("Year of Birth: " + person.PartitionKey);
            }
        }

        private static void DeletePeopleTable()
        {
            CloudStorageAccount cloudStorageAccount = GetCloudStorageAccount();
            CloudTableClient tableClient = cloudStorageAccount.CreateCloudTableClient();
            CloudTable tableReference = tableClient.GetTableReference("People");

            tableReference.CreateIfNotExists();

            tableReference.Delete();

            Console.WriteLine("Table deleted.");
        }
    }
}