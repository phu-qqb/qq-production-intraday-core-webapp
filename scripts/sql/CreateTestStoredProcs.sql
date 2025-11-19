USE [Intraday_Test]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/*---------------------------------------------
  mkt_test.LoadRawFromStage
  - Load raw bars from mkt_test.Stage_HistClose into mkt_test.PriceBar
  - Idempotent upsert via mkt_test.BulkUpsertPriceBar (TVP)
----------------------------------------------*/
CREATE OR ALTER PROCEDURE [mkt_test].[LoadRawFromStage]
  @TimeframeMinute  SMALLINT,                   -- e.g. 60
  @Source           NVARCHAR(32) = N'HistImport',
  @FromUtc          DATETIME2(3) = NULL,        -- optional filter on BarTimeUtc (>=)
  @ToUtc            DATETIME2(3) = NULL         -- optional filter on BarTimeUtc (<)
AS
BEGIN
  SET NOCOUNT ON;

  -- Buffer rows to upsert into the duplicated price bar table
  DECLARE @Rows TABLE (
      SecurityId      INT            NOT NULL,
      BarTimeUtc      DATETIME2(3)   NOT NULL,
      TimeframeMinute SMALLINT       NOT NULL,
      [Open]          DECIMAL(18, 8) NOT NULL,
      High            DECIMAL(18, 8) NOT NULL,
      [Low]           DECIMAL(18, 8) NOT NULL,
      [Close]         DECIMAL(18, 8) NOT NULL,
      Volume          BIGINT         NULL,
      Source          NVARCHAR(32)   NOT NULL
  );

  ;WITH ImportDeDup AS (
      SELECT
          h.SecurityId,
          h.BarTimeUtc,
          h.[Close],
          ROW_NUMBER() OVER (
              PARTITION BY h.SecurityId, h.BarTimeUtc
              ORDER BY h.BarTimeUtc
          ) AS rn
      FROM mkt_test.Stage_HistClose h
      WHERE (@FromUtc IS NULL OR h.BarTimeUtc >= @FromUtc)
        AND (@ToUtc   IS NULL OR h.BarTimeUtc <  @ToUtc)
  ),
  Clean AS (
      SELECT SecurityId, BarTimeUtc, [Close]
      FROM ImportDeDup
      WHERE rn = 1                                      -- keep one per key
        AND SecurityId IS NOT NULL
        AND BarTimeUtc IS NOT NULL
        AND [Close] IS NOT NULL
  )
  INSERT INTO @Rows (SecurityId, BarTimeUtc, TimeframeMinute,
                     [Open], High, [Low], [Close], Volume, Source)
  SELECT
      c.SecurityId,
      c.BarTimeUtc,
      @TimeframeMinute,
      c.[Close], c.[Close], c.[Close], c.[Close],
      NULL,
      @Source
  FROM Clean c;

  -- Pre-compute summary (how many will insert vs update)
  DECLARE @Total BIGINT = (SELECT COUNT(*) FROM @Rows);
  DECLARE @Existing BIGINT =
      (SELECT COUNT(*)
       FROM @Rows r
       JOIN mkt_test.PriceBar t
         ON t.SecurityId      = r.SecurityId
        AND t.BarTimeUtc      = r.BarTimeUtc
        AND t.TimeframeMinute = r.TimeframeMinute);
  DECLARE @ToInsert BIGINT = @Total - @Existing;
  DECLARE @ToUpdate BIGINT = @Existing;

  -- Update existing bars in place
  UPDATE t
  SET
      t.[Open]  = r.[Open],
      t.High    = r.High,
      t.[Low]   = r.[Low],
      t.[Close] = r.[Close],
      t.Volume  = r.Volume,
      t.Source  = r.Source
  FROM mkt_test.PriceBar t
  INNER JOIN @Rows r
    ON t.SecurityId      = r.SecurityId
   AND t.BarTimeUtc      = r.BarTimeUtc
   AND t.TimeframeMinute = r.TimeframeMinute;

  -- Insert the missing bars
  INSERT INTO mkt_test.PriceBar
      (SecurityId, BarTimeUtc, TimeframeMinute,
       [Open], High, [Low], [Close], Volume, Source)
  SELECT
      r.SecurityId,
      r.BarTimeUtc,
      r.TimeframeMinute,
      r.[Open],
      r.High,
      r.[Low],
      r.[Close],
      r.Volume,
      r.Source
  FROM @Rows r
  WHERE NOT EXISTS (
      SELECT 1
      FROM mkt_test.PriceBar t
      WHERE t.SecurityId      = r.SecurityId
        AND t.BarTimeUtc      = r.BarTimeUtc
        AND t.TimeframeMinute = r.TimeframeMinute
  );

  -- Return a compact summary to SSMS Results
  SELECT
    @Total   AS TotalRowsProcessed,
    @ToInsert AS WouldInsert,
    @ToUpdate AS WouldUpdate;
END;
GO

USE [Intraday_Test]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [mkt_test].[LoadFlatFromMinimal]
  @TimeframeMinute  SMALLINT,                   -- ex: 60
  @Source           NVARCHAR(32)  = N'Python',
  @FlattenVersion   NVARCHAR(32)  = N'v1.0.0'
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  IF NOT EXISTS (SELECT 1 FROM [dbo_test].[mkt_FlatBar_Staging])
  BEGIN
    RAISERROR('mkt_FlatBar_Staging is empty.', 16, 1);
    RETURN;
  END

  IF OBJECT_ID('tempdb..#S')      IS NOT NULL DROP TABLE #S;
  IF OBJECT_ID('tempdb..#D')      IS NOT NULL DROP TABLE #D;
  IF OBJECT_ID('tempdb..#Lag')    IS NOT NULL DROP TABLE #Lag;
  IF OBJECT_ID('tempdb..#Final')  IS NOT NULL DROP TABLE #Final;

  /* 1) Normalisation minimale (pas d’alignement) */
  SELECT
      s.SecurityId,
      CAST(s.BarTimeUtc AS datetime2(3))              AS BarTimeUtc,
      CAST(s.[Close]   AS decimal(18,8))              AS CloseVal,
      CAST(LTRIM(RTRIM(COALESCE(NULLIF(s.[Session], N''), N'REG'))) AS nvarchar(16)) AS SessionCode
  INTO #S
  FROM [dbo_test].[mkt_FlatBar_Staging] s;

  /* 2) Dédoublonnage strict */
  SELECT
      SecurityId,
      SessionCode,
      BarTimeUtc,
      MAX(CloseVal) AS CloseVal
  INTO #D
  FROM #S
  GROUP BY SecurityId, SessionCode, BarTimeUtc;

  CREATE CLUSTERED INDEX CX_D ON #D(SecurityId, SessionCode, BarTimeUtc);

  /* 3) LAG par (Sec, Sess) */
  SELECT
      d.SecurityId,
      d.SessionCode,
      d.BarTimeUtc,
      d.CloseVal,
      LAG(d.BarTimeUtc) OVER (PARTITION BY d.SecurityId, d.SessionCode ORDER BY d.BarTimeUtc) AS prev_ts,
      LAG(d.CloseVal)   OVER (PARTITION BY d.SecurityId, d.SessionCode ORDER BY d.BarTimeUtc) AS prev_close
  INTO #Lag
  FROM #D d;

  CREATE CLUSTERED INDEX CX_Lag ON #Lag(SecurityId, SessionCode, BarTimeUtc);

  /* 4) Final */
  SELECT
      l.SecurityId,
      l.BarTimeUtc,
      @TimeframeMinute                AS TimeframeMinute,
      CAST(l.CloseVal AS decimal(18,8))      AS [Open],
      CAST(l.CloseVal AS decimal(18,8))      AS High,
      CAST(l.CloseVal AS decimal(18,8))      AS [Low],
      CAST(l.CloseVal AS decimal(18,8))      AS [Close],
      CAST(NULL AS bigint)                  AS Volume,
      CAST(@Source AS nvarchar(32))         AS Source,
      CAST(
           CASE WHEN l.prev_ts IS NULL
                     OR DATEADD(MINUTE, @TimeframeMinute, l.prev_ts) <> l.BarTimeUtc
                THEN 1 ELSE 0 END
           AS bit)                         AS IsSessionOpen,
      CAST(
           CASE WHEN l.prev_ts IS NULL
                     OR DATEADD(MINUTE, @TimeframeMinute, l.prev_ts) <> l.BarTimeUtc
                THEN NULL ELSE l.prev_close END
           AS decimal(18,8))              AS PrevCloseInSess,
      @FlattenVersion                 AS FlattenVersion,
      SUSER_SNAME()                   AS CreatedBy,
      l.SessionCode                   AS SessionCode
  INTO #Final
  FROM #Lag l;

  CREATE CLUSTERED INDEX CX_Final ON #Final(SecurityId, SessionCode, BarTimeUtc);

  /* 5) Replace par clés exactes */
  BEGIN TRAN;

    DELETE fb
    FROM mkt_test.FlatBar fb
    INNER JOIN #Final f
      ON  f.SecurityId      = fb.SecurityId
      AND f.BarTimeUtc      = fb.BarTimeUtc
      AND f.SessionCode     = fb.SessionCode
      AND @TimeframeMinute  = fb.TimeframeMinute;

    /* ⬇️ CreatedUtc omis → le DEFAULT(sysutcdatetime()) s’applique */
    INSERT INTO mkt_test.FlatBar WITH (TABLOCK)
      (SecurityId, BarTimeUtc, TimeframeMinute,
       [Open], High, [Low], [Close], Volume, Source,
       IsSessionOpen, PrevCloseInSess, FlattenVersion, CreatedBy, SessionCode)
    SELECT
      SecurityId, BarTimeUtc, TimeframeMinute,
      [Open], High, [Low], [Close], Volume, Source,
      IsSessionOpen, PrevCloseInSess, FlattenVersion, CreatedBy, SessionCode
    FROM #Final;

  COMMIT;

  SELECT (SELECT COUNT(*) FROM #Final) AS RowsUpserted;
END
GO
