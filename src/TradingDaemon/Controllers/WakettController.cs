using System.Collections.Generic;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.OpenApi;
using Microsoft.OpenApi.Models;
using TradingDaemon.Services;

namespace TradingDaemon.Controllers;

public static class WakettController
{
    public static void MapWakettEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/wakett/prices", async Task<Ok<WakettPriceUploadResponse>> (WakettPriceFetcher fetcher) =>
        {
            var result = await fetcher.FetchAndStoreAsync();
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
            op.Description = "Retrieves FX prices from Wakett, reconstructs missing crosses, and stores any missing bars from the last 24 hours in the database.";
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

        app.MapPost("/api/wakett/orders/send", async Task<Ok<WakettOrderSubmissionResponse>> (OrderSender orderSender) =>
        {
            await orderSender.SendOrdersAsync();
            return TypedResults.Ok(new WakettOrderSubmissionResponse("OrdersSent"));
        })
        .WithName("SendWakettOrders")
        .Produces<WakettOrderSubmissionResponse>()
        .WithOpenApi(op =>
        {
            op.Summary = "Submit orders to Wakett.";
            op.Description = "Triggers the Wakett order sender to translate the latest theoretical weights into Wakett orders a"
                + "nd submit them through the Wakett trading API.";
            op.Responses["200"] = new OpenApiResponse
            {
                Description = "The Wakett order sender completed and attempted to submit all orders.",
                Content = new Dictionary<string, OpenApiMediaType>
                {
                    ["application/json"] = new OpenApiMediaType
                    {
                        Schema = new OpenApiSchema
                        {
                            Reference = new OpenApiReference
                            {
                                Type = ReferenceType.Schema,
                                Id = nameof(WakettOrderSubmissionResponse)
                            }
                        }
                    }
                }
            };
            return op;
        });
    }
}

public sealed record WakettOrderSubmissionResponse(string Status);
