using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Gemstone.Data.UnitTests
{
    // TODO: Determine how to enable tests for any environment
    //[TestClass]
    public class AdoDataConnectionTest
    {
        [TestMethod]
        public void TestSQLServerConnection()
        {
            string connectionString = @"Data Source=localhost\SQLEXPRESS; Initial Catalog=openPDC; Integrated Security=SSPI; TrustServerCertificate=True; Connect Timeout=5";
            string dataProviderString = "AssemblyName=Microsoft.Data.SqlClient; ConnectionType=Microsoft.Data.SqlClient.SqlConnection";
            //$"AssemblyName={{{typeof(Microsoft.Data.SqlClient.SqlConnection).Assembly.FullName}}}; ConnectionType=Microsoft.Data.SqlClient.SqlConnection; AdapterType=Microsoft.Data.SqlClient.SqlDataAdapter";

            System.Console.WriteLine($"Connection String = {connectionString}");
            System.Console.WriteLine($"Data Provider String = {dataProviderString}");

            AdoDataConnection connection = new(connectionString, dataProviderString);

            Assert.IsTrue(Convert.ToInt32(connection.ExecuteScalar("SELECT COUNT(*) FROM Device")) >= 0);
        }

        [TestMethod]
        public void TestSQLiteConnection()
        {
            string connectionString = @"Data Source=C:\Program Files\openPDC\Database Scripts\SQLite\openPDC.db; Foreign Keys=True";
            string dataProviderString = "AssemblyName=Microsoft.Data.Sqlite; ConnectionType=Microsoft.Data.Sqlite.SqliteConnection";
            //$"AssemblyName={{{typeof(Microsoft.Data.SqlClient.SqlConnection).Assembly.FullName}}}; ConnectionType=Microsoft.Data.SqlClient.SqlConnection; AdapterType=Microsoft.Data.SqlClient.SqlDataAdapter";

            System.Console.WriteLine($"Connection String = {connectionString}");
            System.Console.WriteLine($"Data Provider String = {dataProviderString}");

            AdoDataConnection connection = new(connectionString, dataProviderString);

            Assert.IsTrue(Convert.ToInt32(connection.ExecuteScalar("SELECT COUNT(*) FROM Device")) >= 0);
        }
    }
}
