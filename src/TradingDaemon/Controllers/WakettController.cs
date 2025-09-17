using System.Collections.Generic;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.OpenApi;
using Microsoft.OpenApi.Models;
using TradingDaemon.Models;
using TradingDaemon.Services;

namespace TradingDaemon.Controllers;

public static class WakettController
{
    public static void MapWakettEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/wakett/prices", async Task<Ok<WakettPriceUploadResponse>> (WakettPriceFetcher fetcher, WakettPriceRequest request) =>
        {
            var result = await fetcher.FetchAndStoreAsync(request.Ts);
            var response = result is not null
                ? WakettPriceUploadResponse.FromResult(result)
                : WakettPriceUploadResponse.Empty();
            return TypedResults.Ok(response);
        })
        .WithName("FetchWakettPrices")
        .Produces<WakettPriceUploadResponse>()
        .WithOpenApi(op =>
        {
            op.Summary = "Fetch and store Wakett FX prices.";
            op.Description = "Retrieves FX prices from Wakett, reconstructs missing crosses, and stores them in the database.";
            op.Responses["200"] = new OpenApiResponse
            {
                Description = "Summary of the FX prices that were uploaded to Stage_HistClose.",
                Content = new Dictionary<string, OpenApiMediaType>
                {
                    ["application/json"] = new OpenApiMediaType
                    {
                        Schema = new OpenApiSchema
                        {
                            Reference = new OpenApiReference
                            {
                                Type = ReferenceType.Schema,
                                Id = nameof(WakettPriceUploadResponse)
                            }
                        }
                    }
                }
            };
            return op;
        });
    }
}
