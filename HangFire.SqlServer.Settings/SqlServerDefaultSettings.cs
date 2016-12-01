namespace HangFire.SqlServer.Settings
{
    public class SqlServerDefaultSettings : ISqlServerSettings
    {
        public string TransformScript(string script)
        {
            return script;
        }

        public string CountersAggregationSql => null;
        public string SetJobParameterSql => null;
        public string SetRangeInHashSql => null;
        public string AddToSetSql => null;
        public string SetRangeInHashWriteOnlySql => null;
        public string AnnounceServerSql => null;
        public string WithForceSeekSql => " with (forceseek) ";
    }
}