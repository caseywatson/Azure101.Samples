using Microsoft.WindowsAzure.Storage.Table;

namespace Azure101.Samples.TableStorage
{
    public class Person : TableEntity
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
    }
}