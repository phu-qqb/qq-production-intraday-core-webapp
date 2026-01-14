USE [Intraday]
GO

/*
  BuildMissingHourlyPriceBars.sql
  --------------------------------
  Reconstructs missing 60-minute rows in mkt.PriceBar from existing 15-minute
  bars starting on 2025-11-20 (inclusive). Adjust @FromUtc / @ToUtc if you need
  a different backfill window.
*/
SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @FromUtc DATETIME2(3) = '2025-11-20T00:00:00.000';
DECLARE @ToUtc   DATETIME2(3) = NULL; -- optional exclusive upper bound

;WITH Fifteen AS (
    SELECT
        pb.SecurityId,
        pb.BarTimeUtc,
        pb.[Open],
        pb.High,
        pb.[Low],
        pb.[Close],
        pb.Volume,
        pb.Source,
        DATEADD(HOUR, DATEDIFF(HOUR, 0, pb.BarTimeUtc), 0) AS HourStartUtc,
        ROW_NUMBER() OVER (
            PARTITION BY pb.SecurityId, DATEADD(HOUR, DATEDIFF(HOUR, 0, pb.BarTimeUtc), 0)
            ORDER BY pb.BarTimeUtc ASC
        ) AS rn_open,
        ROW_NUMBER() OVER (
            PARTITION BY pb.SecurityId, DATEADD(HOUR, DATEDIFF(HOUR, 0, pb.BarTimeUtc), 0)
            ORDER BY pb.BarTimeUtc DESC
        ) AS rn_close
    FROM mkt.PriceBar pb
    WHERE pb.TimeframeMinute = 15
      AND pb.BarTimeUtc >= @FromUtc
      AND (@ToUtc IS NULL OR pb.BarTimeUtc < @ToUtc)
),
Hourly AS (
    SELECT
        SecurityId,
        HourStartUtc          AS BarTimeUtc,
        CAST(60 AS SMALLINT)  AS TimeframeMinute,
        MAX(CASE WHEN rn_open  = 1 THEN [Open]  END) AS [Open],
        MAX(High)                             AS High,
        MIN([Low])                            AS [Low],
        MAX(CASE WHEN rn_close = 1 THEN [Close] END) AS [Close],
        SUM(COALESCE(Volume, 0))              AS Volume,
        MIN(Source)                           AS Source,
        COUNT(*)                              AS FifteenMinuteBars
    FROM Fifteen
    GROUP BY SecurityId, HourStartUtc
),
Missing AS (
    SELECT h.*
    FROM Hourly h
    WHERE NOT EXISTS (
        SELECT 1
        FROM mkt.PriceBar t
        WHERE t.SecurityId      = h.SecurityId
          AND t.BarTimeUtc      = h.BarTimeUtc
          AND t.TimeframeMinute = h.TimeframeMinute
    )
)
INSERT INTO mkt.PriceBar
    (SecurityId, BarTimeUtc, TimeframeMinute,
     [Open], High, [Low], [Close], Volume, Source)
SELECT
    SecurityId,
    BarTimeUtc,
    TimeframeMinute,
    [Open],
    High,
    [Low],
    [Close],
    Volume,
    Source
FROM Missing
ORDER BY SecurityId, BarTimeUtc;

DECLARE @Inserted INT = @@ROWCOUNT;
PRINT CONCAT('Inserted ', @Inserted, ' hourly bars from ', FORMAT(@FromUtc, 'yyyy-MM-dd'),
             CASE WHEN @ToUtc IS NULL THEN '' ELSE CONCAT(' up to ', FORMAT(@ToUtc, 'yyyy-MM-dd')) END, '.');
GO
