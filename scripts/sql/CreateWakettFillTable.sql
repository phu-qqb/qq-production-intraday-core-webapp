/*
    Creates the table used to persist Wakett fills returned by the /trades endpoint.
    Execute inside the Wakett database context.
*/

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas s
    INNER JOIN sys.tables t ON s.schema_id = t.schema_id
    WHERE s.name = N'wakett'
      AND t.name = N'Fill'
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

    CREATE TABLE [wakett].[Fill]
    (
        WakettFillId        BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ExecuteId           NVARCHAR(64)         NOT NULL,
        Account             NVARCHAR(64)         NOT NULL,
        RequestedFrom       CHAR(8)              NOT NULL,
        RequestedTo         CHAR(8)              NOT NULL,
        RequestedStrategy   NVARCHAR(64)         NULL,
        PortfolioId         NVARCHAR(32)         NULL,
        Portfolio           NVARCHAR(64)         NULL,
        Alias               NVARCHAR(128)        NULL,
        Broker              NVARCHAR(64)         NULL,
        StrategyId          NVARCHAR(64)         NULL,
        SymbolId            NVARCHAR(64)         NULL,
        Symbol              NVARCHAR(32)         NOT NULL,
        InstrumentId        NVARCHAR(64)         NULL,
        Reference           NVARCHAR(64)         NULL,
        CoreOrderId         INT                  NULL,
        SubOrderId          INT                  NULL,
        Label               NVARCHAR(64)         NULL,
        Side                NVARCHAR(8)          NULL,
        OrderPrice          DECIMAL(19,9)        NULL,
        OrderId             NVARCHAR(64)         NULL,
        OrderTimestamp      DATETIMEOFFSET(7)    NULL,
        OrderSize           DECIMAL(19,9)        NULL,
        OrderChannel        NVARCHAR(32)         NULL,
        ProviderId          NVARCHAR(64)         NULL,
        ProviderTimestamp   DATETIMEOFFSET(7)    NULL,
        ExecuteTimestamp    DATETIMEOFFSET(7)    NULL,
        EntitySize          DECIMAL(19,9)        NULL,
        ExecuteSize         DECIMAL(19,9)        NULL,
        ExecutePrice        DECIMAL(19,9)        NULL,
        TradeTimestamp      DATETIMEOFFSET(7)    NULL,
        Event               NVARCHAR(32)         NULL,
        UserName            NVARCHAR(64)         NULL,
        Code                NVARCHAR(64)         NULL,
        RecordType          NVARCHAR(32)         NULL,
        Quote               DECIMAL(19,9)        NULL,
        Amount              DECIMAL(19,9)        NULL,
        Rate                DECIMAL(19,9)        NULL,
        CreatedAtUtc        DATETIME2(7)         NOT NULL CONSTRAINT DF_WakettFill_CreatedAtUtc DEFAULT SYSUTCDATETIME(),
        UpdatedAtUtc        DATETIME2(7)         NOT NULL CONSTRAINT DF_WakettFill_UpdatedAtUtc DEFAULT SYSUTCDATETIME()
    );

    CREATE UNIQUE INDEX UX_WakettFill_Account_ExecuteId_SubOrderId_ExecuteTimestamp
        ON [wakett].[Fill] (Account, ExecuteId, SubOrderId, ExecuteTimestamp);

    CREATE INDEX IX_WakettFill_RequestedWindow
        ON [wakett].[Fill] (Account, RequestedFrom, RequestedTo);

    CREATE INDEX IX_WakettFill_ExecuteTimestamp
        ON [wakett].[Fill] (ExecuteTimestamp);
END;
