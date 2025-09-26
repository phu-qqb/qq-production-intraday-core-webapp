/*
    Creates the table used to persist Wakett order submissions.
    Execute inside the Intraday database context.
*/
IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas s
    INNER JOIN sys.tables t ON s.schema_id = t.schema_id
    WHERE s.name = N'model'
      AND t.name = N'WakettOrder'
)
BEGIN
    CREATE TABLE [model].[WakettOrder]
    (
        WakettOrderId       BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ModelId             INT                  NOT NULL,
        OrderCode           NVARCHAR(64)         NOT NULL,
        ScheduledTimestamp  DATETIMEOFFSET(7)    NOT NULL,
        SubmittedAtUtc      DATETIME2(7)         NOT NULL,
        ReceivedAtUtc       DATETIME2(7)         NOT NULL,
        Symbol              NVARCHAR(32)         NOT NULL,
        Side                NVARCHAR(8)          NOT NULL,
        SizeValue           DECIMAL(19,9)        NULL,
        Aum                 DECIMAL(19,4)        NULL,
        ErrorCode           NVARCHAR(128)        NULL,
        ErrorMessage        NVARCHAR(512)        NULL,
        TradesJson          NVARCHAR(MAX)        NULL
    );

    CREATE INDEX IX_WakettOrder_ScheduledTimestamp
        ON [model].[WakettOrder] (ScheduledTimestamp);

    CREATE INDEX IX_WakettOrder_OrderCode
        ON [model].[WakettOrder] (OrderCode);
END;
