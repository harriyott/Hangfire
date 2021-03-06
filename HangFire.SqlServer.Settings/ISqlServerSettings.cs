﻿namespace HangFire.SqlServer.Settings
{
    public interface ISqlServerSettings
    {
        string TransformScript(string script);
        string CountersAggregationSql { get; }
        string SetJobParameterSql { get; }
        string SetRangeInHashSql { get; }
        string AddToSetSql { get; }
        string SetRangeInHashWriteOnlySql { get; }
        string AnnounceServerSql { get; }
        string WithForceSeekSql { get; }
    }
}