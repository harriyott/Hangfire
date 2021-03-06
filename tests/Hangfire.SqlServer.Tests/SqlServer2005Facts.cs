﻿using System;
using System.IO;
using System.Reflection;
using HangFire.SqlServer.Settings;
using Xunit;

namespace Hangfire.SqlServer.Tests
{
    public class SqlServer2005Facts
    {
        [Fact]
        public void Ctor_CreatesDefaultSettings()
        {
            var options = new SqlServerStorageOptions
            {
                SqlServer2005Compatibility = false
            };
            var storage = new SqlServerStorage(ConnectionUtils.GetConnectionString(), options);
            Assert.Equal(typeof(SqlServerDefaultSettings), storage.SqlServerSettings.GetType());
        }

        [Fact]
        public void Ctor_CreatesSqlServer2005Settings()
        {
            var options = new SqlServerStorageOptions
            {
                SqlServer2005Compatibility = true
            };
            var storage = new SqlServerStorage(ConnectionUtils.GetConnectionString(), options);
            Assert.Equal(typeof(SqlServer2005Settings), storage.SqlServerSettings.GetType());
        }

        [Fact]
        public void TransformScript_RemovesDateTime2()
        {
            var script = GetStringResource(
                typeof(SqlServerObjectsInstaller).Assembly,
                "Hangfire.SqlServer.Install.sql");

            Assert.False(new SqlServer2005Settings().TransformScript(script).Contains("datetime2"));
        }

        [Fact]
        public void CountersAggregationQuery_BlankInDefault()
        {
            Assert.True(string.IsNullOrEmpty(new SqlServerDefaultSettings().CountersAggregationSql));
        }

        [Fact]
        public void CountersAggregationQuery_PresentIn2005()
        {
            Assert.True(new SqlServer2005Settings().CountersAggregationSql.Contains("UPDATE"));
            Assert.False(new SqlServer2005Settings().CountersAggregationSql.Contains("merge"));
        }

        [Fact]
        public void SetJobParameterSql_BlankInDefault()
        {
            Assert.True(string.IsNullOrEmpty(new SqlServerDefaultSettings().SetJobParameterSql));
        }

        [Fact]
        public void SetJobParameterSql_PresentIn2005()
        {
            Assert.True(new SqlServer2005Settings().SetJobParameterSql.Contains("UPDATE"));
            Assert.False(new SqlServer2005Settings().SetJobParameterSql.Contains("merge"));
        }

        [Fact]
        public void SetRangeInHash_BlankInDefault()
        {
            Assert.True(string.IsNullOrEmpty(new SqlServerDefaultSettings().SetRangeInHashSql));
        }

        [Fact]
        public void SetRangeInHash_PresentIn2005()
        {
            Assert.True(new SqlServer2005Settings().SetRangeInHashSql.Contains("UPDATE"));
            Assert.False(new SqlServer2005Settings().SetRangeInHashSql.Contains("merge"));
        }

        [Fact]
        public void AddToSetSql_BlankInDefault()
        {
            Assert.True(string.IsNullOrEmpty(new SqlServerDefaultSettings().AddToSetSql));
        }

        [Fact]
        public void AddToSetSql_PresentIn2005()
        {
            Assert.True(new SqlServer2005Settings().AddToSetSql.Contains("UPDATE"));
            Assert.False(new SqlServer2005Settings().AddToSetSql.Contains("merge"));
        }

        [Fact]
        public void SetRangeInHashWriteOnlySql_BlankInDefault()
        {
            Assert.True(string.IsNullOrEmpty(new SqlServerDefaultSettings().SetRangeInHashWriteOnlySql));
        }

        [Fact]
        public void SetRangeInHashWriteOnlySql_PresentIn2005()
        {
            Assert.True(new SqlServer2005Settings().SetRangeInHashWriteOnlySql.Contains("UPDATE"));
            Assert.False(new SqlServer2005Settings().SetRangeInHashWriteOnlySql.Contains("merge"));
        }

        [Fact]
        public void WithForceSeekSql_BlankIn2005()
        {
            Assert.True(string.IsNullOrEmpty(new SqlServer2005Settings().WithForceSeekSql));
        }

        [Fact]
        public void WithForceSeekSql_PresentInDefault()
        {
            Assert.False(string.IsNullOrEmpty(new SqlServerDefaultSettings().WithForceSeekSql));
            Assert.True(new SqlServerDefaultSettings().WithForceSeekSql.Contains("forceseek"));
        }

        [Fact]
        public void AnnounceServerSql_BlankInDefault()
        {
            Assert.True(string.IsNullOrEmpty(new SqlServerDefaultSettings().AnnounceServerSql));
        }

        [Fact]
        public void AnnounceServerSql_PresentIn2005()
        {
            Assert.True(new SqlServer2005Settings().AnnounceServerSql.Contains("UPDATE"));
            Assert.False(new SqlServer2005Settings().AnnounceServerSql.Contains("merge"));
        }

        private static string GetStringResource(Assembly assembly, string resourceName)
        {
            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                if (stream == null)
                {
                    throw new InvalidOperationException(
                        $"Requested resource `{resourceName}` was not found in the assembly `{assembly}`.");
                }

                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }

    }
}