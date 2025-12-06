using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace TradingDaemon.Middleware;

/// <summary>
/// Captures SQL timeout exceptions and logs the HTTP request information so that
/// the failing endpoint can be identified from the logs.
/// </summary>
public sealed class SqlTimeoutLoggingMiddleware
{
    private const int MaxLoggedBodyLength = 2_048;

    private readonly RequestDelegate _next;
    private readonly ILogger<SqlTimeoutLoggingMiddleware> _logger;

    public SqlTimeoutLoggingMiddleware(RequestDelegate next, ILogger<SqlTimeoutLoggingMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        context.Request.EnableBuffering();

        try
        {
            await _next(context);
        }
        catch (SqlException ex) when (IsTimeout(ex))
        {
            var queryString = context.Request.QueryString.HasValue
                ? context.Request.QueryString.Value
                : string.Empty;

            var requestBody = await ReadRequestBodyAsync(context.Request);
            var routeValues = FormatRouteValues(context.Request.RouteValues);
            var remoteIp = context.Connection.RemoteIpAddress?.ToString() ?? "unknown";

            _logger.LogError(
                ex,
                "SQL timeout while handling {Method} {Path}{QueryString}. TraceIdentifier: {TraceIdentifier}. RemoteIp: {RemoteIp}. RouteValues: {RouteValues}. Body: {Body}",
                context.Request.Method,
                context.Request.Path,
                queryString,
                context.TraceIdentifier,
                remoteIp,
                routeValues,
                requestBody);

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

    private static async Task<string> ReadRequestBodyAsync(HttpRequest request)
    {
        if (request.Body == null)
        {
            return "[no body stream]";
        }

        if (!request.Body.CanSeek)
        {
            return "[body not seekable]";
        }

        request.Body.Position = 0;

        using var reader = new StreamReader(
            request.Body,
            encoding: Encoding.UTF8,
            detectEncodingFromByteOrderMarks: false,
            bufferSize: 1024,
            leaveOpen: true);

        var builder = new StringBuilder();
        var buffer = new char[512];
        var totalRead = 0;
        var truncated = false;

        while (totalRead < MaxLoggedBodyLength)
        {
            var charsToRead = Math.Min(buffer.Length, MaxLoggedBodyLength - totalRead);
            var read = await reader.ReadAsync(buffer.AsMemory(0, charsToRead));

            if (read == 0)
            {
                break;
            }

            builder.Append(buffer, 0, read);
            totalRead += read;

            if (totalRead >= MaxLoggedBodyLength && !reader.EndOfStream)
            {
                truncated = true;
                break;
            }
        }

        request.Body.Position = 0;

        if (builder.Length == 0)
        {
            return "[empty]";
        }

        if (truncated)
        {
            builder.Append("… (truncated)");
        }

        return builder.ToString();
    }

    private static string FormatRouteValues(RouteValueDictionary routeValues)
    {
        if (routeValues == null || routeValues.Count == 0)
        {
            return "[none]";
        }

        IEnumerable<string> FormatKeyValuePairs()
        {
            foreach (var pair in routeValues)
            {
                var value = pair.Value?.ToString();
                yield return $"{pair.Key}={(string.IsNullOrWhiteSpace(value) ? "<empty>" : value)}";
            }
        }

        return string.Join(", ", FormatKeyValuePairs());
    }
}
