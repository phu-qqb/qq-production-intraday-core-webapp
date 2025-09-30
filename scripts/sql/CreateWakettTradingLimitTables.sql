/*
    Creates the tables used to store Wakett trading limits and limit breach reports.
    Execute inside the Wakett database context.
*/

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = N'wakett'
)
BEGIN
    EXEC ('CREATE SCHEMA [wakett] AUTHORIZATION dbo;');
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas s
    INNER JOIN sys.tables t ON s.schema_id = t.schema_id
    WHERE s.name = N'wakett'
      AND t.name = N'TradingLimit'
)
BEGIN
    CREATE TABLE [wakett].[TradingLimit]
    (
        TradingLimitId           INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ModelId                  INT              NOT NULL,
        SingleTradeGrossLimit    DECIMAL(19,9)    NULL,
        PortfolioGrossLimit      DECIMAL(19,9)    NULL,
        PortfolioNetLimit        DECIMAL(19,9)    NULL,
        SingleTradeTurnoverLimit DECIMAL(19,9)    NULL,
        TotalTurnoverLimit       DECIMAL(19,9)    NULL,
        CreatedAtUtc             DATETIME2(7)     NOT NULL DEFAULT SYSUTCDATETIME(),
        UpdatedAtUtc             DATETIME2(7)     NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_TradingLimit_ModelId
        ON [wakett].[TradingLimit] (ModelId, TradingLimitId DESC);
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas s
    INNER JOIN sys.tables t ON s.schema_id = t.schema_id
    WHERE s.name = N'wakett'
      AND t.name = N'TradingLimitBreachReport'
)
BEGIN
    CREATE TABLE [wakett].[TradingLimitBreachReport]
    (
        TradingLimitBreachReportId BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ModelId                    INT                  NOT NULL,
        OccurredAtUtc              DATETIME2(7)         NOT NULL DEFAULT SYSUTCDATETIME(),
        LimitType                  NVARCHAR(64)         NOT NULL,
        LimitValue                 DECIMAL(19,9)        NULL,
        ObservedValue              DECIMAL(19,9)        NULL,
        Details                    NVARCHAR(1024)       NULL,
        OrdersJson                 NVARCHAR(MAX)        NULL,
        Aum                        DECIMAL(19,4)        NULL
    );

    CREATE INDEX IX_TradingLimitBreachReport_ModelId
        ON [wakett].[TradingLimitBreachReport] (ModelId, OccurredAtUtc DESC);
END;
