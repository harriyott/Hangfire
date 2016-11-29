using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using Dapper;
using Moq;
using Xunit;

namespace Hangfire.SqlServer.Tests
{
    public class SqlServer2005ConnectionFacts
    {
        // ReSharper disable once PrivateFieldCanBeConvertedToLocalVariable
        private readonly Mock<IPersistentJobQueue> _queue;
        private readonly PersistentJobQueueProviderCollection _providers;

        public SqlServer2005ConnectionFacts()
        {
            _queue = new Mock<IPersistentJobQueue>();

            var provider = new Mock<IPersistentJobQueueProvider>();
            provider.Setup(x => x.GetJobQueue())
                .Returns(_queue.Object);

            _providers = new PersistentJobQueueProviderCollection(provider.Object);
        }

        [Fact, CleanDatabase]
        public void SetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetJobParameter(null, "name", "value"));

                Assert.Equal("id", exception.ParamName);
            });
        }

        private void UseConnection(Action<SqlServerConnection> action)
        {
            using (var sql = ConnectionUtils.CreateConnection())
            {
                var storage = new Mock<SqlServerStorage>(sql, new SqlServerStorageOptions
                {
                    SqlServer2005Compatibility = true
                });
                storage.Setup(x => x.QueueProviders).Returns(_providers);

                using (var connection = new SqlServerConnection(storage.Object))
                {
                    action(connection);
                }
            }
        }

        private void UseConnections(Action<SqlConnection, SqlServerConnection> action)
        {
            using (var sqlConnection = ConnectionUtils.CreateConnection())
            {
                var storage = new SqlServerStorage(sqlConnection, new SqlServerStorageOptions
                {
                    SqlServer2005Compatibility = true
                });
                using (var connection = new SqlServerConnection(storage))
                {
                    action(sqlConnection, connection);
                }
            }
        }


        [Fact, CleanDatabase]
        public void SetParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetJobParameter("1", null, "value"));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameters_CreatesNewParameter_WhenParameterWithTheGivenNameDoesNotExists()
        {
            const string arrangeSql = @"
insert into HangFire.Job (InvocationData, Arguments, CreatedAt)
values ('', '', getutcdate())
select scope_identity() as Id";

            UseConnections((sql, connection) =>
            {
                var job = sql.Query(arrangeSql).Single();
                string jobId = job.Id.ToString();

                connection.SetJobParameter(jobId, "Name", "Value");

                var parameter = sql.Query(
                    "select * from HangFire.JobParameter where JobId = @id and Name = @name",
                    new { id = jobId, name = "Name" }).Single();

                Assert.Equal("Value", parameter.Value);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_UpdatesValue_WhenParameterWithTheGivenName_AlreadyExists()
        {
            const string arrangeSql = @"
insert into HangFire.Job (InvocationData, Arguments, CreatedAt)
values ('', '', getutcdate())
select scope_identity() as Id";

            UseConnections((sql, connection) =>
            {
                var job = sql.Query(arrangeSql).Single();
                string jobId = job.Id.ToString();

                connection.SetJobParameter(jobId, "Name", "Value");
                connection.SetJobParameter(jobId, "Name", "AnotherValue");

                var parameter = sql.Query(
                    "select * from HangFire.JobParameter where JobId = @id and Name = @name",
                    new { id = jobId, name = "Name" }).Single();

                Assert.Equal("AnotherValue", parameter.Value);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_CanAcceptNulls_AsValues()
        {
            const string arrangeSql = @"
insert into HangFire.Job (InvocationData, Arguments, CreatedAt)
values ('', '', getutcdate())
select scope_identity() as Id";

            UseConnections((sql, connection) =>
            {
                var job = sql.Query(arrangeSql).Single();
                string jobId = job.Id.ToString();

                connection.SetJobParameter(jobId, "Name", null);

                var parameter = sql.Query(
                    "select * from HangFire.JobParameter where JobId = @id and Name = @name",
                    new { id = jobId, name = "Name" }).Single();

                Assert.Equal((string)null, parameter.Value);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                // ReSharper disable once AssignNullToNotNullAttribute
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash(null, new Dictionary<string, string>()));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(connection =>
            {
                // ReSharper disable once AssignNullToNotNullAttribute
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.SetRangeInHash("some-hash", null));

                Assert.Equal("keyValuePairs", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_MergesAllRecords()
        {
            UseConnections((sql, connection) =>
            {
                connection.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                });

                var result = sql.Query(
                    "select * from HangFire.Hash where [Key] = @key",
                    new { key = "some-hash" })
                    .ToDictionary(x => (string)x.Field, x => (string)x.Value);

                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }


    }
}