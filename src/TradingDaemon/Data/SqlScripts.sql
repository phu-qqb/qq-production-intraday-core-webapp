IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'report')
BEGIN
    EXEC('CREATE SCHEMA [report]');
END;

IF OBJECT_ID('[report].[DailyPnL]', 'U') IS NULL
BEGIN
    CREATE TABLE [report].[DailyPnL]
    (
        TradingDate date NOT NULL CONSTRAINT PK_ReportDailyPnL PRIMARY KEY,
        CalculatedAtUtc datetime2(7) NOT NULL,
        PnL decimal(38, 10) NOT NULL
    );
END;


