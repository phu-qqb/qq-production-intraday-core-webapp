using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using TradingDaemon.Data;

namespace TradingDaemon.Services;

internal interface IPriceProcessingProcedureExecutor
{
    Task LoadRawFromStageAsync(
        IDbConnection connection,
        int timeframeMinute,
        CancellationToken cancellationToken = default);

    Task LoadFlatFromMinimalAsync(
        IDbConnection connection,
        int timeframeMinute,
        CancellationToken cancellationToken = default);
}

internal sealed class PriceProcessingProcedureExecutor : IPriceProcessingProcedureExecutor
{
    private readonly string _loadRawFromStageProc;
    private readonly string _loadFlatFromMinimalProc;

    public PriceProcessingProcedureExecutor(IDatabaseObjectNameProvider databaseNameProvider)
    {
        _loadRawFromStageProc = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayMarketLoadRawFromStageProc);
        _loadFlatFromMinimalProc = databaseNameProvider.GetObjectName(DatabaseObjects.IntradayMarketLoadFlatFromMinimalProc);
    }

    public Task LoadRawFromStageAsync(
        IDbConnection connection,
        int timeframeMinute,
        CancellationToken cancellationToken = default)
    {
        var command = new CommandDefinition(
            _loadRawFromStageProc,
            new { TimeframeMinute = timeframeMinute },
            commandType: CommandType.StoredProcedure,
            cancellationToken: cancellationToken);

        return connection.ExecuteAsync(command);
    }

    public Task LoadFlatFromMinimalAsync(
        IDbConnection connection,
        int timeframeMinute,
        CancellationToken cancellationToken = default)
    {
        var command = new CommandDefinition(
            _loadFlatFromMinimalProc,
            new { TimeframeMinute = timeframeMinute },
            commandType: CommandType.StoredProcedure,
            cancellationToken: cancellationToken);

        return connection.ExecuteAsync(command);
    }
}
