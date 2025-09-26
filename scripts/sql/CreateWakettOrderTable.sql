/*
    Creates the table used to persist Wakett order submissions.
    Execute inside the Wakett database context.
*/

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas s
    INNER JOIN sys.tables t ON s.schema_id = t.schema_id
    WHERE s.name = N'wakett'
      AND t.name = N'Order'
)
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'wakett'
    )
    BEGIN
        EXEC ('CREATE SCHEMA [wakett] AUTHORIZATION dbo;');
    END;

    CREATE TABLE [wakett].[Order]
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
        ON [wakett].[Order] (ScheduledTimestamp);

    CREATE INDEX IX_WakettOrder_OrderCode
        ON [wakett].[Order] (OrderCode);
END;
