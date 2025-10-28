using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.OpenApi;
using Microsoft.OpenApi.Models;
using TradingDaemon.Services;

namespace TradingDaemon.Controllers;

public static class TradingController
{
    public static void MapTradingEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/trading/run", async Task<Ok<TradingRunResponse>> (
            WakettPriceFetcher priceFetcher,
            WeightCalculator weightCalculator,
            OrderSender orderSender,
            CancellationToken cancellationToken) =>
        {
            await priceFetcher.FetchAndStoreAsync(cancellationToken);
            await weightCalculator.CalculateAndStoreAsync();
            await orderSender.SendOrdersAsync(cancellationToken: cancellationToken);
            return TypedResults.Ok(new TradingRunResponse("Completed"));
        })
        .WithName("RunTradingPipeline")
        .Produces<TradingRunResponse>()
        .WithOpenApi(op =>
        {
            op.Summary = "Run the intraday trading pipeline with Wakett order submission.";
            op.Description = "Fetches FX prices, computes theoretical weights, and submits the resulting orders to Wakett.";
            op.Responses["200"] = new OpenApiResponse
            {
                Description = "The trading workflow completed and Wakett orders were submitted.",
                Content = new Dictionary<string, OpenApiMediaType>
                {
                    ["application/json"] = new OpenApiMediaType
                    {
                        Schema = new OpenApiSchema
                        {
                            Reference = new OpenApiReference
                            {
                                Type = ReferenceType.Schema,
                                Id = nameof(TradingRunResponse)
                            }
                        }
                    }
                }
            };
            return op;
        });
    }
}

public sealed record TradingRunResponse(string Status);
