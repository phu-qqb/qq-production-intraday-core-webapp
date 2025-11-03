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

    IF NOT EXISTS (
        SELECT 1
        FROM sys.indexes i
        INNER JOIN sys.objects o ON o.object_id = i.object_id
        INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE i.name = N'IX_WakettFill_ExecuteId_SubOrderId'
            AND o.name = N'Fill'
            AND s.name = N'wakett')
    BEGIN
        CREATE INDEX IX_WakettFill_ExecuteId_SubOrderId
            ON [wakett].[Fill] (ExecuteId, SubOrderId);
    END;
END;
