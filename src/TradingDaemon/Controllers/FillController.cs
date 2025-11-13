using System.Collections.Generic;
using System.Linq;
using Dapper;
using Microsoft.Extensions.Logging;
using TradingDaemon.Data;
using TradingDaemon.Models;
using TradingDaemon.Services;
using TradingDaemon.Utils;

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


        app.MapGet("/api/pnl", async (DateTime date, DapperContext context, IEmailNotificationService emailNotificationService, ILogger<FillEndpointsLogger> logger) =>

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

            var positionGroups = fills
                .GroupBy(f => f.Symbol)
                .Select(group => new
                {
                    Symbol = group.Key,
                    Quantity = group.Sum(f => f.Quantity),
                    LastPrice = group.OrderByDescending(f => f.Timestamp).FirstOrDefault()?.Price
                })
                .Where(p => p.Quantity != 0m)
                .ToList();

            var positions = new List<PnlReportPosition>();
            foreach (var position in positionGroups)
            {
                if (!CurrencyPairParser.TryParse(position.Symbol, out var pair))
                {
                    continue;
                }

                decimal? lastPrice = position.LastPrice;
                decimal? usdValue = lastPrice.HasValue ? position.Quantity * lastPrice.Value : null;

                positions.Add(new PnlReportPosition(
                    pair.FormattedSymbol,
                    pair.BaseCurrency,
                    pair.QuoteCurrency,
                    position.Quantity,
                    lastPrice,
                    usdValue));
            }

            var grossMarketValue = positions.Sum(p => Math.Abs(p.MarketValueUsd ?? 0m));
            var totalNetExposure = positions.Sum(p => p.MarketValueUsd ?? 0m);
            var report = new PnlReport(DateOnly.FromDateTime(date.Date), pnl, grossMarketValue, totalNetExposure, positions);

            try
            {
                await emailNotificationService.SendPnLReportAsync(report);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to send PnL email notification");
                return Results.Problem("Failed to send PnL email notification.", statusCode: StatusCodes.Status500InternalServerError);
            }

            return Results.Ok(new
            {
                Date = date.Date,
                PnL = pnl,
                GrossMarketValue = grossMarketValue,
                TotalNetExposure = totalNetExposure,
                Positions = positions
            });
        });
    }
}

internal sealed class FillEndpointsLogger
{
}
