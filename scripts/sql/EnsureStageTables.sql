/*
    Script: EnsureStageTables.sql
    Purpose: Create the Wakett staging tables inside the currently
             selected database when they have not yet been provisioned.

    Usage:
        1. Connect to the intraday database (e.g., Intraday_Test) in
           SQL Server Management Studio or sqlcmd.
        2. Execute this script. Existing tables are left untouched; any
           missing table is created with the expected schema.
*/

SET NOCOUNT ON;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'mkt')
BEGIN
    PRINT 'Schema [mkt] does not exist. Creating it...';
    EXEC('CREATE SCHEMA [mkt];');
END;

IF OBJECT_ID(N'[mkt].[Stage_HistClose]', N'U') IS NULL
BEGIN
    PRINT 'Creating [mkt].[Stage_HistClose]...';

    CREATE TABLE [mkt].[Stage_HistClose]
    (
        [SecurityId] INT NOT NULL,
        [BarTimeUtc] DATETIME2(3) NOT NULL,
        [Close] DECIMAL(18, 8) NOT NULL,
        CONSTRAINT [PK_Stage_HistClose]
            PRIMARY KEY CLUSTERED ([SecurityId], [BarTimeUtc])
    );
END;
ELSE
BEGIN
    PRINT '[mkt].[Stage_HistClose] already exists. Skipping creation.';
END;

IF OBJECT_ID(N'[mkt].[Stage_HistClose_Flat]', N'U') IS NULL
BEGIN
    PRINT 'Creating [mkt].[Stage_HistClose_Flat]...';

    CREATE TABLE [mkt].[Stage_HistClose_Flat]
    (
        [SecurityId] INT NOT NULL,
        [Session] NVARCHAR(16) NOT NULL,
        [BarTimeUtc] DATETIME2(3) NOT NULL,
        [Close] DECIMAL(18, 8) NOT NULL,
        CONSTRAINT [PK_Stage_HistClose_Flat]
            PRIMARY KEY CLUSTERED ([SecurityId], [Session], [BarTimeUtc])
    );
END;
ELSE
BEGIN
    PRINT '[mkt].[Stage_HistClose_Flat] already exists. Skipping creation.';
END;
