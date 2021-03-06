﻿using System.Text.RegularExpressions;

namespace HangFire.SqlServer.Settings
{
    public class SqlServer2005Settings : ISqlServerSettings
    {
        public string TransformScript(string script)
        {
            return Regex.Replace(script, @"\[datetime2\]\([0-9]\)", "[datetime]");
        }

        public string CountersAggregationSql => @"
DECLARE @RecordsToAggregate TABLE
(
	[Key] NVARCHAR(100) NOT NULL,
	[Value] SMALLINT NOT NULL,
	[ExpireAt] DATETIME NULL
)

SET TRANSACTION ISOLATION LEVEL READ COMMITTED
BEGIN TRAN

DELETE TOP (@count) [{0}].[Counter] with (readpast)
OUTPUT DELETED.[Key], DELETED.[Value], DELETED.[ExpireAt] INTO @RecordsToAggregate

SET NOCOUNT ON

UPDATE [{0}].[AggregatedCounter]
SET 
	[Value] = ac.[Value] + ra.[Value],
	[ExpireAt] = (SELECT MAX([ExpireAt]) FROM (VALUES (ac.ExpireAt), (ra.[ExpireAt])) AS MaxExpireAt([ExpireAt]))
FROM [{0}].[AggregatedCounter] AS ac
JOIN @RecordsToAggregate ra
ON ac.[Key] = ra.[Key];

INSERT INTO [{0}].[AggregatedCounter]
SELECT [Key], SUM([Value]) as [Value], MAX([ExpireAt]) AS [ExpireAt] 
FROM @RecordsToAggregate 
GROUP BY [Key]
HAVING [Key] NOT IN (SELECT [Key] FROM [{0}].[AggregatedCounter]);

COMMIT TRAN";

        public string SetJobParameterSql => @";BEGIN TRANSACTION;"
                                            + @"UPDATE [{0}].JobParameter "
                                            + @"SET [Value] = @value "
                                            + @"WHERE JobId = @jobId AND [Name] = @name; "
                                            + @"IF @@ROWCOUNT = 0 "
                                            + @"INSERT INTO [{0}].JobParameter (JobId, Name, Value) "
                                            + @"VALUES(@jobId, @name, @value); "
                                            + @"COMMIT TRANSACTION;";

        public string SetRangeInHashSql => @"
;BEGIN TRANSACTION;
UPDATE [{0}].Hash
SET [Value] = @value
WHERE [Key] = @key AND Field = @field;

IF @@ROWCOUNT = 0
INSERT INTO [{0}].Hash ([Key], Field, Value)
VALUES(@key, @field, @value);
COMMIT TRANSACTION;";

        public string AddToSetSql => @"
;BEGIN TRANSACTION;
UPDATE [{0}].[Set]
SET Score = @score
WHERE [Key] = @key AND Value = @value;

IF @@ROWCOUNT = 0
INSERT INTO [{0}].[Set] ([Key], Score, Value)
VALUES(@key, @score, @value);
COMMIT TRANSACTION;";

        public string SetRangeInHashWriteOnlySql => @"
;BEGIN TRANSACTION;
UPDATE [{0}].Hash
SET [Value] = @value
WHERE [Key] = @key AND Field = @field;

IF @@ROWCOUNT = 0
INSERT INTO [{0}].Hash ([Key], Field, Value)
VALUES(@key, @field, @value);
COMMIT TRANSACTION;";

        public string AnnounceServerSql => @"
;BEGIN TRANSACTION;
;UPDATE [{0}].Server
SET Data = @data, LastHeartbeat = @heartbeat
WHERE [Id] = @id;

IF @@ROWCOUNT = 0
INSERT INTO [{0}].Server (Id, Data, LastHeartbeat)
VALUES(@id, @data, @heartbeat);
COMMIT TRANSACTION;";

        public string WithForceSeekSql => "";
    }
}