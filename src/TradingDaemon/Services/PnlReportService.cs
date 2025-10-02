using System.Threading;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using TradingDaemon.Data;

namespace TradingDaemon.Services;

public class PnlReportService
{
    private readonly DapperContext _context;
    private readonly ILogger<PnlReportService> _logger;

    private static readonly SemaphoreSlim SchemaSemaphore = new(1, 1);
    private static bool _storageEnsured;

    private const string EnsureSchemaSql = @"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'report')
BEGIN
    EXEC('CREATE SCHEMA [report]');
END";

    private const string EnsureTableSql = @"IF OBJECT_ID('[report].[DailyPnL]', 'U') IS NULL
BEGIN
    CREATE TABLE [report].[DailyPnL]
    (
        TradingDate date NOT NULL CONSTRAINT PK_DailyPnL PRIMARY KEY,
        CalculatedAtUtc datetime2(7) NOT NULL,
        PnL decimal(38, 10) NOT NULL
    );
END";

    private const string UpsertSql = @"MERGE [report].[DailyPnL] AS target
USING (VALUES (@TradingDate, @CalculatedAtUtc, @PnL)) AS source (TradingDate, CalculatedAtUtc, PnL)
    ON target.TradingDate = source.TradingDate
WHEN MATCHED THEN
    UPDATE SET
        CalculatedAtUtc = source.CalculatedAtUtc,
        PnL = source.PnL
WHEN NOT MATCHED THEN
    INSERT (TradingDate, CalculatedAtUtc, PnL)
    VALUES (source.TradingDate, source.CalculatedAtUtc, source.PnL);";

    private const string PnlSql = @"WITH LatestPrices AS (
    SELECT
        pb.SecurityId,
        pb.[Close],
        ROW_NUMBER() OVER (PARTITION BY pb.SecurityId ORDER BY pb.BarTimeUtc DESC) AS rn
    FROM [Intraday].[mkt].[PriceBar] pb
    WHERE
        pb.TimeframeMinute = 60
        AND pb.BarTimeUtc >= @StartUtc
        AND pb.BarTimeUtc < @EndUtc
)
SELECT
    SUM(
        CASE
            WHEN UPPER(f.Side) IN ('SELL', 'S', 'SHORT', 'SS') THEN -1
            WHEN UPPER(f.Side) IN ('BUY', 'B', 'LONG', 'L') THEN 1
            ELSE CASE WHEN f.ExecuteSize < 0 THEN -1 ELSE 1 END
        END
        * COALESCE(f.ExecuteSize, 0)
        * (COALESCE(lp.[Close], f.ExecutePrice) - COALESCE(f.ExecutePrice, 0))
    )
FROM [wakett].[Fill] f
LEFT JOIN LatestPrices lp ON lp.SecurityId = f.SymbolId AND lp.rn = 1
WHERE
    f.TradeTimestamp >= @StartUtc
    AND f.TradeTimestamp < @EndUtc;";

    public PnlReportService(DapperContext context, ILogger<PnlReportService> logger)
    {
        _context = context;
        _logger = logger;
    }

    public async Task<decimal> ComputeAndStoreCurrentDayPnlAsync(DateTime? clock = null, CancellationToken cancellationToken = default)
    {
        var nowUtc = (clock ?? DateTime.UtcNow).ToUniversalTime();
        var tradingDate = DateOnly.FromDateTime(nowUtc);
        var startUtc = tradingDate.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc);
        var endUtc = startUtc.AddDays(1);

        await using var connection = (SqlConnection)_context.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        await EnsureStorageAsync(connection, cancellationToken);

        var pnl = await connection.ExecuteScalarAsync<decimal?>(
            new CommandDefinition(PnlSql, new { StartUtc = startUtc, EndUtc = endUtc }, cancellationToken: cancellationToken));

        var pnlValue = pnl ?? 0m;

        await connection.ExecuteAsync(
            new CommandDefinition(
                UpsertSql,
                new
                {
                    TradingDate = tradingDate.ToDateTime(TimeOnly.MinValue),
                    CalculatedAtUtc = nowUtc,
                    PnL = pnlValue
                },
                cancellationToken: cancellationToken));

        _logger.LogInformation("Computed current day PnL for {Date}: {PnL}", tradingDate, pnlValue);
        Console.WriteLine($"Current day PnL: {pnlValue}");

        return pnlValue;
    }

    private async Task EnsureStorageAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        if (_storageEnsured)
        {
            return;
        }

        await SchemaSemaphore.WaitAsync(cancellationToken);
        try
        {
            if (_storageEnsured)
            {
                return;
            }

            await connection.ExecuteAsync(new CommandDefinition(EnsureSchemaSql, cancellationToken: cancellationToken));
            await connection.ExecuteAsync(new CommandDefinition(EnsureTableSql, cancellationToken: cancellationToken));

            _storageEnsured = true;
        }
        finally
        {
            SchemaSemaphore.Release();
        }
    }
}
