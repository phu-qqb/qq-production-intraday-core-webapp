using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.OpenApi;
using Microsoft.OpenApi.Models;
using TradingDaemon.Services;

namespace TradingDaemon.Controllers;

public static class OrderController
{
    public static void MapOrderEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/orders/send", async Task<Ok<OrderSendResponse>> (OrderSender orderSender) =>
        {
            await orderSender.SendOrdersAsync();
            return TypedResults.Ok(new OrderSendResponse("OrdersSent"));
        })
        .WithName("SendOrders")
        .Produces<OrderSendResponse>()
        .WithOpenApi(op =>
        {
            op.Summary = "Submit Wakett orders";
            op.Description = "Loads the latest theoretical weights, converts them into Wakett orders, and submits them through the Wakett API.";
            op.Responses["200"] = new OpenApiResponse
            {
                Description = "The order submission workflow completed and Wakett orders were sent.",
                Content = new Dictionary<string, OpenApiMediaType>
                {
                    ["application/json"] = new OpenApiMediaType
                    {
                        Schema = new OpenApiSchema
                        {
                            Reference = new OpenApiReference
                            {
                                Type = ReferenceType.Schema,
                                Id = nameof(OrderSendResponse)
                            }
                        }
                    }
                }
            };
            return op;
        });
    }
}

public sealed record OrderSendResponse(string Status);
