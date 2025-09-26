using System.Collections.Generic;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.OpenApi;
using Microsoft.OpenApi.Models;
using TradingDaemon.Models;
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

        app.MapPost("/api/wakett/orders/send", async Task<Ok<WakettOrderSubmissionResponse>> (
            OrderSender orderSender,
            SendWakettOrdersRequest? request) =>
        {
            await orderSender.SendOrdersAsync(request?.Aum);
            return TypedResults.Ok(new WakettOrderSubmissionResponse("OrdersSent"));
        })
        .WithName("SendWakettOrders")
        .Produces<WakettOrderSubmissionResponse>()
        .WithOpenApi(op =>
        {
            op.Summary = "Submit orders to Wakett.";
            op.Description = "Triggers the Wakett order sender to translate the latest theoretical weights into Wakett orders a"
                + "nd submit them through the Wakett trading API.";
            op.RequestBody = new OpenApiRequestBody
            {
                Required = false,
                Content = new Dictionary<string, OpenApiMediaType>
                {
                    ["application/json"] = new OpenApiMediaType
                    {
                        Schema = new OpenApiSchema
                        {
                            Reference = new OpenApiReference
                            {
                                Type = ReferenceType.Schema,
                                Id = nameof(SendWakettOrdersRequest)
                            }
                        }
                    }
                }
            };
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

        app.MapPost("/api/wakett/fills/fetch", async Task<Results<BadRequest<ProblemDetails>, Ok<WakettFillUploadResponse>>>(
            WakettTradeFetcher tradeFetcher,
            FetchWakettFillsRequest request,
            CancellationToken cancellationToken) =>
        {
            try
            {
                var result = await tradeFetcher.FetchAndStoreAsync(request, cancellationToken);
                return TypedResults.Ok(result);
            }
            catch (ArgumentException ex)
            {
                return TypedResults.BadRequest(CreateProblem("InvalidRequest", ex.Message));
            }
            catch (WakettTradeFetcherException ex)
            {
                return TypedResults.BadRequest(CreateProblem(ex.Status, ex.Message));
            }
        })
        .WithName("FetchWakettFills")
        .Produces<WakettFillUploadResponse>()
        .ProducesProblem(StatusCodes.Status400BadRequest)
        .WithOpenApi(op =>
        {
            op.Summary = "Fetch Wakett executions and persist them as fills.";
            op.Description = "Calls the Wakett /trades endpoint for the specified account and date window (5pm NY cut), then upserts each execution into the [wakett].[Fill] table.";
            op.RequestBody = new OpenApiRequestBody
            {
                Required = true,
                Content = new Dictionary<string, OpenApiMediaType>
                {
                    ["application/json"] = new OpenApiMediaType
                    {
                        Schema = new OpenApiSchema
                        {
                            Reference = new OpenApiReference
                            {
                                Type = ReferenceType.Schema,
                                Id = nameof(FetchWakettFillsRequest)
                            }
                        }
                    }
                }
            };
            op.Responses[StatusCodes.Status200OK.ToString()] = new OpenApiResponse
            {
                Description = "Summary of the Wakett executions that were written to the wakett.Fill table.",
                Content = new Dictionary<string, OpenApiMediaType>
                {
                    ["application/json"] = new OpenApiMediaType
                    {
                        Schema = new OpenApiSchema
                        {
                            Reference = new OpenApiReference
                            {
                                Type = ReferenceType.Schema,
                                Id = nameof(WakettFillUploadResponse)
                            }
                        }
                    }
                }
            };

            op.Responses[StatusCodes.Status400BadRequest.ToString()] = new OpenApiResponse
            {
                Description = "The request parameters were invalid or the Wakett API returned an error.",
                Content = new Dictionary<string, OpenApiMediaType>
                {
                    ["application/problem+json"] = new OpenApiMediaType
                    {
                        Schema = new OpenApiSchema
                        {
                            Reference = new OpenApiReference
                            {
                                Type = ReferenceType.Schema,
                                Id = nameof(ProblemDetails)
                            }
                        }
                    }
                }
            };
            return op;
        });
    }

    private static ProblemDetails CreateProblem(string title, string detail)
        => new()
        {
            Title = title,
            Detail = detail
        };
}

public sealed record WakettOrderSubmissionResponse(string Status);

public sealed record SendWakettOrdersRequest(double? Aum);
