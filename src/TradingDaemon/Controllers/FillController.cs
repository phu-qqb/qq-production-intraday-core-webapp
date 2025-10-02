using Dapper;
using Microsoft.Extensions.Logging;
using TradingDaemon.Data;
using TradingDaemon.Models;

namespace TradingDaemon.Controllers;

public static class FillController
{
    public static void MapFillEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/fills", async (Fill fill, DapperContext context) =>
        {
            using var connection = context.CreateConnection();
            var sql = @"INSERT INTO fills (symbol, quantity, price, timestamp)
                        VALUES (@Symbol, @Quantity, @Price, @Timestamp)";
            Console.WriteLine($"Executing SQL: {sql}");
            await connection.ExecuteAsync(sql, fill);
            return Results.Created($"/api/fills/{fill.Id}", fill);
        });

        app.MapGet("/api/pnl", async (DateTime date, DapperContext context, ILogger<FillEndpointsLogger> logger) =>
        {
            using var connection = context.CreateConnection();
            var fillsSql = "SELECT * FROM fills WHERE DATE(timestamp) = @Date";
            logger.LogInformation("Executing SQL: {Sql}", fillsSql);
            var fills = await connection.QueryAsync<Fill>(fillsSql, new { Date = date.Date });
            var weightsSql = "SELECT * FROM weights WHERE DATE(asof) = @Date";
            logger.LogInformation("Executing SQL: {Sql}", weightsSql);
            var weights = await connection.QueryAsync<Weight>(weightsSql, new { Date = date.Date });

            var pnl = (from f in fills
                       join w in weights on f.Symbol equals w.Symbol
                       select f.Quantity * (w.Value - f.Price)).Sum();

            return Results.Ok(new { Date = date.Date, PnL = pnl });
        });
    }
}

internal sealed class FillEndpointsLogger
{
}
