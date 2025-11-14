/*
    Script: RefreshTestTables.sql
    Purpose: Truncate each test table and reload it with the contents of the
             corresponding production table so the duplicated environment stays
             in sync.

    Usage:
        1. Review the table pairs below and adjust them if new tables are added.
        2. Run the script in SQL Server Management Studio (or sqlcmd) while
           connected to the server that hosts the tables. You can execute it
           from any database because the commands fully qualify each object.
        3. The script runs within a transaction; if any copy fails the entire
           refresh is rolled back.

    Notes:
        * The script checks that both the production and test tables exist
          before attempting to copy data.
        * Identity columns are detected automatically so SET IDENTITY_INSERT is
          only applied when needed.
        * Foreign key constraints that reference the target table must be
          disabled beforehand if they prevent TRUNCATE TABLE from succeeding.
*/

SET NOCOUNT ON;

BEGIN TRAN;

BEGIN TRY
    DECLARE @TablePairs TABLE (
        ProdQualified sysname NOT NULL,
        TestQualified sysname NOT NULL
    );

    INSERT INTO @TablePairs (ProdQualified, TestQualified)
    VALUES
        (N'Intraday.model.NettedWeight',            N'Intraday.model_test.NettedWeight'),
        (N'Intraday.model.Model',                   N'Intraday.model_test.Model'),
        (N'Intraday.core.Security',                 N'Intraday.core_test.Security'),
        (N'Intraday.mkt.PriceBar',                  N'Intraday.mkt_test.PriceBar'),
        (N'Intraday.mkt.FlatBar',                   N'Intraday.mkt_test.FlatBar'),
        (N'Intraday.mkt.Stage_HistClose',           N'Intraday.mkt_test.Stage_HistClose'),
        (N'Intraday.dbo.mkt_FlatBar_Staging',       N'Intraday.dbo_test.mkt_FlatBar_Staging'),
        (N'Intraday.wakett.Fill',                   N'Intraday.wakett_test.Fill'),
        (N'Intraday.wakett.TradingLimit',           N'Intraday.wakett_test.TradingLimit'),
        (N'Intraday.wakett.TradingLimitBreachReport', N'Intraday.wakett_test.TradingLimitBreachReport'),
        (N'Intraday.wakett.[Order]',                N'Intraday.wakett_test.[Order]');

    DECLARE
        @ProdQualified sysname,
        @TestQualified sysname,
        @ProdDatabase sysname,
        @ProdSchema sysname,
        @ProdTable sysname,
        @TestDatabase sysname,
        @TestSchema sysname,
        @TestTable sysname,
        @ColumnList nvarchar(max),
        @HasIdentity bit,
        @Sql nvarchar(max),
        @Exists int,
        @ExistsSql nvarchar(max),
        @ColumnSql nvarchar(max),
        @IdentitySql nvarchar(max),
        @ErrorMessage nvarchar(4000);

    DECLARE TableCursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT ProdQualified, TestQualified
        FROM @TablePairs;

    OPEN TableCursor;

    FETCH NEXT FROM TableCursor INTO @ProdQualified, @TestQualified;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Break the fully qualified names into their components.
        SELECT
            @ProdDatabase = COALESCE(PARSENAME(@ProdQualified, 3), DB_NAME()),
            @ProdSchema   = PARSENAME(@ProdQualified, 2),
            @ProdTable    = PARSENAME(@ProdQualified, 1),
            @TestDatabase = COALESCE(PARSENAME(@TestQualified, 3), DB_NAME()),
            @TestSchema   = PARSENAME(@TestQualified, 2),
            @TestTable    = PARSENAME(@TestQualified, 1);

        IF @ProdSchema IS NULL OR @ProdTable IS NULL OR @TestSchema IS NULL OR @TestTable IS NULL
        BEGIN
            THROW 50001, 'Each table name must include at least a schema and table.', 1;
        END;

        -- Confirm that the production table exists.
        SET @Exists = 0;
        SET @ExistsSql =
            N'SELECT @ExistsOut = COUNT(*)
              FROM ' + QUOTENAME(@ProdDatabase) + N'.sys.tables t
              INNER JOIN ' + QUOTENAME(@ProdDatabase) + N'.sys.schemas s ON t.schema_id = s.schema_id
              WHERE t.name = @TableName AND s.name = @SchemaName;';

        EXEC sys.sp_executesql
            @ExistsSql,
            N'@TableName sysname, @SchemaName sysname, @ExistsOut int OUTPUT',
            @TableName = @ProdTable,
            @SchemaName = @ProdSchema,
            @ExistsOut = @Exists OUTPUT;

        IF @Exists = 0
        BEGIN
            SET @ErrorMessage = FORMATMESSAGE('Production table not found: %s', @ProdQualified);
            THROW 50002, @ErrorMessage, 1;
        END;

        -- Confirm that the test table exists.
        SET @Exists = 0;
        SET @ExistsSql =
            N'SELECT @ExistsOut = COUNT(*)
              FROM ' + QUOTENAME(@TestDatabase) + N'.sys.tables t
              INNER JOIN ' + QUOTENAME(@TestDatabase) + N'.sys.schemas s ON t.schema_id = s.schema_id
              WHERE t.name = @TableName AND s.name = @SchemaName;';

        EXEC sys.sp_executesql
            @ExistsSql,
            N'@TableName sysname, @SchemaName sysname, @ExistsOut int OUTPUT',
            @TableName = @TestTable,
            @SchemaName = @TestSchema,
            @ExistsOut = @Exists OUTPUT;

        IF @Exists = 0
        BEGIN
            PRINT 'Test table not found for ' + @TestQualified + '. Creating an empty copy from the production table...';

            DECLARE @CreateSql nvarchar(max);

            SET @CreateSql =
                N'USE ' + QUOTENAME(@TestDatabase) + N';' + CHAR(13) + CHAR(10) +
                N'IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N''' + REPLACE(@TestSchema, '''', '''''') + N''')' + CHAR(13) + CHAR(10) +
                N'BEGIN' + CHAR(13) + CHAR(10) +
                N'    EXEC(N''CREATE SCHEMA ' + REPLACE(QUOTENAME(@TestSchema), '''', '''''') + N''');' + CHAR(13) + CHAR(10) +
                N'END;' + CHAR(13) + CHAR(10) +
                N'IF OBJECT_ID(N''' + REPLACE(QUOTENAME(@TestSchema) + N'.' + QUOTENAME(@TestTable), '''', '''''') + N''', N''U'') IS NULL' + CHAR(13) + CHAR(10) +
                N'BEGIN' + CHAR(13) + CHAR(10) +
                N'    SELECT TOP (0) *' + CHAR(13) + CHAR(10) +
                N'    INTO ' + QUOTENAME(@TestSchema) + N'.' + QUOTENAME(@TestTable) + CHAR(13) + CHAR(10) +
                N'    FROM ' + QUOTENAME(@ProdDatabase) + N'.' + QUOTENAME(@ProdSchema) + N'.' + QUOTENAME(@ProdTable) + N';' + CHAR(13) + CHAR(10) +
                N'END;';

            EXEC sys.sp_executesql @CreateSql;

            -- Re-check that the table now exists.
            SET @Exists = 0;
            EXEC sys.sp_executesql
                @ExistsSql,
                N'@TableName sysname, @SchemaName sysname, @ExistsOut int OUTPUT',
                @TableName = @TestTable,
                @SchemaName = @TestSchema,
                @ExistsOut = @Exists OUTPUT;

            IF @Exists = 0
            BEGIN
                SET @ErrorMessage = FORMATMESSAGE('Test table not found and could not be created: %s', @TestQualified);
                THROW 50003, @ErrorMessage, 1;
            END;
        END;

        -- Build the ordered column list from the production table.
        SET @ColumnList = NULL;
        SET @ColumnSql =
            N'SELECT @ColumnsOut = STRING_AGG(QUOTENAME(c.name), '','') WITHIN GROUP (ORDER BY c.column_id)
              FROM ' + QUOTENAME(@ProdDatabase) + N'.sys.columns c
              INNER JOIN ' + QUOTENAME(@ProdDatabase) + N'.sys.tables t ON c.object_id = t.object_id
              INNER JOIN ' + QUOTENAME(@ProdDatabase) + N'.sys.schemas s ON t.schema_id = s.schema_id
              WHERE t.name = @TableName AND s.name = @SchemaName;';

        EXEC sys.sp_executesql
            @ColumnSql,
            N'@TableName sysname, @SchemaName sysname, @ColumnsOut nvarchar(max) OUTPUT',
            @TableName = @ProdTable,
            @SchemaName = @ProdSchema,
            @ColumnsOut = @ColumnList OUTPUT;

        IF @ColumnList IS NULL
        BEGIN
            SET @ErrorMessage = FORMATMESSAGE('Could not read the column list from the production table: %s', @ProdQualified);
            THROW 50004, @ErrorMessage, 1;
        END;

        -- Determine whether the target table has an identity column.
        SET @HasIdentity = 0;
        SET @IdentitySql =
            N'SELECT @HasIdentityOut = CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
              FROM ' + QUOTENAME(@TestDatabase) + N'.sys.identity_columns ic
              INNER JOIN ' + QUOTENAME(@TestDatabase) + N'.sys.tables t ON ic.object_id = t.object_id
              INNER JOIN ' + QUOTENAME(@TestDatabase) + N'.sys.schemas s ON t.schema_id = s.schema_id
              WHERE t.name = @TableName AND s.name = @SchemaName;';

        EXEC sys.sp_executesql
            @IdentitySql,
            N'@TableName sysname, @SchemaName sysname, @HasIdentityOut bit OUTPUT',
            @TableName = @TestTable,
            @SchemaName = @TestSchema,
            @HasIdentityOut = @HasIdentity OUTPUT;

        -- Build the refresh command.
        SET @Sql =
            N'TRUNCATE TABLE ' + QUOTENAME(@TestDatabase) + N'.' + QUOTENAME(@TestSchema) + N'.' + QUOTENAME(@TestTable) + N';' +
            CHAR(13) + CHAR(10);

        IF @HasIdentity = 1
        BEGIN
            SET @Sql +=
                N'SET IDENTITY_INSERT ' + QUOTENAME(@TestDatabase) + N'.' + QUOTENAME(@TestSchema) + N'.' + QUOTENAME(@TestTable) + N' ON;' +
                CHAR(13) + CHAR(10);
        END;

        SET @Sql +=
            N'INSERT INTO ' + QUOTENAME(@TestDatabase) + N'.' + QUOTENAME(@TestSchema) + N'.' + QUOTENAME(@TestTable) +
            N' (' + @ColumnList + N')' + CHAR(13) + CHAR(10) +
            N'SELECT ' + @ColumnList + CHAR(13) + CHAR(10) +
            N'FROM ' + QUOTENAME(@ProdDatabase) + N'.' + QUOTENAME(@ProdSchema) + N'.' + QUOTENAME(@ProdTable) + N';' +
            CHAR(13) + CHAR(10);

        IF @HasIdentity = 1
        BEGIN
            SET @Sql +=
                N'SET IDENTITY_INSERT ' + QUOTENAME(@TestDatabase) + N'.' + QUOTENAME(@TestSchema) + N'.' + QUOTENAME(@TestTable) + N' OFF;' +
                CHAR(13) + CHAR(10);
        END;

        PRINT 'Refreshing ' + @ProdQualified + ' -> ' + @TestQualified + '...';
        EXEC sys.sp_executesql @Sql;

        FETCH NEXT FROM TableCursor INTO @ProdQualified, @TestQualified;
    END;

    CLOSE TableCursor;
    DEALLOCATE TableCursor;

    COMMIT TRAN;
END TRY
BEGIN CATCH
    IF CURSOR_STATUS('local', 'TableCursor') >= -1
    BEGIN
        CLOSE TableCursor;
        DEALLOCATE TableCursor;
    END;

    IF XACT_STATE() <> 0
    BEGIN
        ROLLBACK TRAN;
    END;

    THROW;
END CATCH;
