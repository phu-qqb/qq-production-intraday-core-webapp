using System.Linq;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace TradingDaemon.Middleware;

/// <summary>
/// Captures SQL timeout exceptions and logs the HTTP request information so that
/// the failing endpoint can be identified from the logs.
/// </summary>
public sealed class SqlTimeoutLoggingMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<SqlTimeoutLoggingMiddleware> _logger;

    public SqlTimeoutLoggingMiddleware(RequestDelegate next, ILogger<SqlTimeoutLoggingMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        try
        {
            await _next(context);
        }
        catch (SqlException ex) when (IsTimeout(ex))
        {
            var queryString = context.Request.QueryString.HasValue
                ? context.Request.QueryString.Value
                : string.Empty;

            _logger.LogError(
                ex,
                "SQL timeout while handling {Method} {Path}{QueryString}. TraceIdentifier: {TraceIdentifier}",
                context.Request.Method,
                context.Request.Path,
                queryString,
                context.TraceIdentifier);

            throw;
        }
    }

    private static bool IsTimeout(SqlException exception)
    {
        if (exception.Number == -2)
        {
            return true;
        }

        // SqlException may contain multiple errors, so fall back to inspecting them as well.
        return exception.Errors
            .Cast<SqlError>()
            .Any(error => error.Number == -2);
    }
}
