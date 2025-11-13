BEGIN
    IF EXISTS (
        SELECT 1
        FROM sys.indexes i
        INNER JOIN sys.objects o ON o.object_id = i.object_id
        INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE i.name = N'UX_WakettFill_ExecuteId'
            AND o.name = N'Fill'
            AND s.name = N'wakett')
    BEGIN
        DROP INDEX UX_WakettFill_ExecuteId ON [wakett].[Fill];
    END;

    IF EXISTS (
        SELECT 1
        FROM sys.indexes i
        INNER JOIN sys.objects o ON o.object_id = i.object_id
        INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE i.name = N'UX_WakettFill_ExecuteId_SubOrderId'
            AND o.name = N'Fill'
            AND s.name = N'wakett')
    BEGIN
        DROP INDEX UX_WakettFill_ExecuteId_SubOrderId ON [wakett].[Fill];
    END;

    IF NOT EXISTS (
        SELECT 1
        FROM sys.indexes i
        INNER JOIN sys.objects o ON o.object_id = i.object_id
        INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE i.name = N'UX_WakettFill_Account_ExecuteId_SubOrderId_ExecuteTimestamp'
            AND o.name = N'Fill'
            AND s.name = N'wakett')
    BEGIN
        CREATE UNIQUE INDEX UX_WakettFill_Account_ExecuteId_SubOrderId_ExecuteTimestamp
            ON [wakett].[Fill] (Account, ExecuteId, SubOrderId, ExecuteTimestamp);
    END;
END;
