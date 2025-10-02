using System.Collections.Generic;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.OpenApi;
using Microsoft.OpenApi.Models;
using TradingDaemon.Services;

namespace TradingDaemon.Controllers;

public static class EmailController
{
    public static void MapEmailEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/email/test", async Task<Results<Ok<TestEmailResponse>, ProblemHttpResult>> (TestEmailRequest? request, IEmailNotificationService emailService, ILogger<EmailEndpointsLogger> logger, CancellationToken cancellationToken) =>
        {
            try
            {
                await emailService.SendTestEmailAsync(request?.Subject, request?.Body, cancellationToken);
                return TypedResults.Ok(new TestEmailResponse("TestEmailSent"));
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to send test email");
                return TypedResults.Problem(
                    title: "Failed to send test email.",
                    detail: ex.Message,
                    statusCode: StatusCodes.Status500InternalServerError);
            }
        })
        .WithName("SendTestEmail")
        .Produces<TestEmailResponse>()
        .ProducesProblem(StatusCodes.Status500InternalServerError)
        .WithOpenApi(op =>
        {
            op.Summary = "Send a test email";
            op.Description = "Sends a basic test email using the configured Amazon SES settings.";
            op.Responses["200"] = new OpenApiResponse
            {
                Description = "The test email was sent successfully.",
                Content = new Dictionary<string, OpenApiMediaType>
                {
                    ["application/json"] = new OpenApiMediaType
                    {
                        Schema = new OpenApiSchema
                        {
                            Reference = new OpenApiReference
                            {
                                Type = ReferenceType.Schema,
                                Id = nameof(TestEmailResponse)
                            }
                        }
                    }
                }
            };

            op.RequestBody = new OpenApiRequestBody
            {
                Required = false,
                Description = "Optional subject and body overrides for the test email.",
                Content = new Dictionary<string, OpenApiMediaType>
                {
                    ["application/json"] = new OpenApiMediaType
                    {
                        Schema = new OpenApiSchema
                        {
                            Reference = new OpenApiReference
                            {
                                Type = ReferenceType.Schema,
                                Id = nameof(TestEmailRequest)
                            }
                        }
                    }
                }
            };

            return op;
        });
    }
}

public sealed record TestEmailRequest(string? Subject, string? Body);

public sealed record TestEmailResponse(string Status);

internal sealed class EmailEndpointsLogger;
