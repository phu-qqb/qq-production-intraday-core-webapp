using System.Data;
using System.Threading;
using Dapper;

namespace TradingDaemon.Services;

internal static class PriceProcessingProcedures
{
    private const string LoadRawFromStageSql =
        "EXEC mkt.LoadRawFromStage @TimeframeMinute = @TimeframeMinute";

    private const string LoadFlatFromMinimalSql =
        "EXEC mkt.LoadFlatFromMinimal @TimeframeMinute = @TimeframeMinute";

    public static Task LoadRawFromStageAsync(
        IDbConnection connection,
        int timeframeMinute,
        CancellationToken cancellationToken = default)
    {
        var command = new CommandDefinition(
            LoadRawFromStageSql,
            new { TimeframeMinute = timeframeMinute },
            commandType: CommandType.Text,
            cancellationToken: cancellationToken);

        return connection.ExecuteAsync(command);
    }

    public static Task LoadFlatFromMinimalAsync(
        IDbConnection connection,
        int timeframeMinute,
        CancellationToken cancellationToken = default)
    {
        var command = new CommandDefinition(
            LoadFlatFromMinimalSql,
            new { TimeframeMinute = timeframeMinute },
            commandType: CommandType.Text,
            cancellationToken: cancellationToken);

        return connection.ExecuteAsync(command);
    }
}
