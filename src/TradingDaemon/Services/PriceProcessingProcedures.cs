using System.Data;
using System.Threading;
using Dapper;

namespace TradingDaemon.Services;

internal static class PriceProcessingProcedures
{

    private const string LoadRawFromStageProc = "mkt.LoadRawFromStage";
    private const string LoadFlatFromMinimalProc = "mkt.LoadFlatFromMinimal";


    public static Task LoadRawFromStageAsync(
        IDbConnection connection,
        int timeframeMinute,
        CancellationToken cancellationToken = default)
    {
        var command = new CommandDefinition(

            LoadRawFromStageProc,
            new { TimeframeMinute = timeframeMinute },
            commandType: CommandType.StoredProcedure,

            cancellationToken: cancellationToken);

        return connection.ExecuteAsync(command);
    }

    public static Task LoadFlatFromMinimalAsync(
        IDbConnection connection,
        int timeframeMinute,
        CancellationToken cancellationToken = default)
    {
        var command = new CommandDefinition(

            LoadFlatFromMinimalProc,
            new { TimeframeMinute = timeframeMinute },
            commandType: CommandType.StoredProcedure,

            cancellationToken: cancellationToken);

        return connection.ExecuteAsync(command);
    }
}
