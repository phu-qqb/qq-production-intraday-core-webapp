using System;
using System.Collections.Generic;
using System.Net;
using System.Globalization;
using System.Linq;
using System.Text;
using Amazon;
using Amazon.SimpleEmail;
using Amazon.SimpleEmail.Model;
using TradingDaemon.Models;

namespace TradingDaemon.Services;

public interface IEmailNotificationService
{
    Task SendPnLReportAsync(
        PnlReport report,
        SlippageResult? slippageResult = null,
        CancellationToken cancellationToken = default);
    Task SendTestEmailAsync(string? subject = null, string? body = null, CancellationToken cancellationToken = default);
}

public class EmailNotificationService : IEmailNotificationService, IDisposable
{
    private readonly IAmazonSimpleEmailService _sesClient;
    private readonly ILogger<EmailNotificationService> _logger;
    private readonly string _fromAddress;
    private readonly IReadOnlyList<string> _recipients;
    private bool _disposedValue;

    public EmailNotificationService(IConfiguration configuration, ILogger<EmailNotificationService> logger)
    {
        _logger = logger;

        var regionName = configuration["AWS:Region"] ?? Environment.GetEnvironmentVariable("AWS_REGION") ?? "eu-west-2";
        _sesClient = new AmazonSimpleEmailServiceClient(RegionEndpoint.GetBySystemName(regionName));

        _fromAddress = configuration["Email:From"]
            ?? configuration["Automation:Email:From"]
            ?? "intraday_bot@quantumqb.com";

        var recipients = configuration.GetSection("Email:Recipients").Get<string[]>();
        if (recipients is null || recipients.Length == 0)
        {
            recipients = configuration.GetSection("Automation:Email:Recipients").Get<string[]>() ?? Array.Empty<string>();
        }
        _recipients = Array.AsReadOnly(recipients);

        if (_recipients.Count == 0)
        {
            _logger.LogWarning("No email recipients configured. PnL emails will be skipped.");
        }
    }

    public async Task SendPnLReportAsync(
        PnlReport report,
        SlippageResult? slippageResult = null,
        CancellationToken cancellationToken = default)
    {
        if (_recipients.Count == 0)
        {
            return;
        }

        var subject = $"Intraday PnL for {report.TradingDate:yyyy-MM-dd}";
        var bodyText = BuildPlainTextBody(report, slippageResult);
        var bodyHtml = BuildHtmlBody(report, slippageResult);

        await SendEmailInternalAsync(subject, bodyText, bodyHtml, cancellationToken);
    }

    private static string BuildPlainTextBody(PnlReport report, SlippageResult? slippageResult)
    {
        var sb = new StringBuilder();

        if (slippageResult is not null)
        {
            sb.AppendLine($"Date: {report.TradingDate:yyyy-MM-dd}");
            sb.AppendLine("Execution summary:");
            AppendExecutionSummaryText(sb, slippageResult);
            sb.AppendLine();
            sb.AppendLine($"Gross Market Value: {FormatPnl(report.GrossMarketValue)}");
            sb.AppendLine($"Total Net Exposure: {FormatPnl(report.TotalNetExposure)}");
            sb.AppendLine();
            sb.AppendLine("Positions:");

            if (report.Positions.Count == 0)
            {
                sb.AppendLine("  (no open positions)");
            }
            else
            {
                foreach (var position in report.Positions.OrderByDescending(p => Math.Abs(p.MarketValueUsd ?? 0m)))
                {
                    sb.Append("  - ");
                    sb.Append(position.Symbol);
                    sb.Append(": Qty=");
                    sb.Append(position.NetQuantity.ToString("F2", CultureInfo.InvariantCulture));
                    sb.Append(", Price=");
                    sb.Append(FormatOptionalNumber(position.LastPrice));
                    sb.Append(", USD Value=");
                    sb.Append(FormatOptionalPnl(position.MarketValueUsd));
                    sb.AppendLine();
                }
            }

            sb.AppendLine();

            sb.AppendLine("  - Theoretical PnL by currency (USD basis):");
            AppendPnlByCurrency(sb, slippageResult.TheoreticalPnlByCurrency);

            sb.AppendLine("  - Real PnL by currency (USD using last available close):");
            AppendPnlByCurrency(sb, slippageResult.RealPnlByCurrency);

            sb.AppendLine(
                "  Note: Theoretical and real PnL values are presented in USD using the last available close prices (currency list retained for clarity).");
        }

        return sb.ToString();
    }

    private static string BuildHtmlBody(PnlReport report, SlippageResult? slippageResult)
    {
        var sb = new StringBuilder();
        sb.Append("<html><body><h1>Intraday PnL</h1>");
        sb.AppendFormat(CultureInfo.InvariantCulture, "<p><strong>Date:</strong> {0:yyyy-MM-dd}</p>", report.TradingDate);
        if (slippageResult is not null)
        {
            AppendExecutionSummaryTable(sb, slippageResult);
        }

        sb.Append("<h2>Positions</h2>");

        if (report.Positions.Count == 0)
        {
            sb.Append("<p>No open positions.</p>");
        }
        else
        {
            sb.Append("<table border=\"1\" cellpadding=\"6\" cellspacing=\"0\">");
            sb.Append("<thead><tr><th>Symbol</th><th>Quantity</th><th>Price</th><th>USD Value</th></tr></thead><tbody>");
            foreach (var position in report.Positions.OrderByDescending(p => Math.Abs(p.MarketValueUsd ?? 0m)))
            {
                sb.Append("<tr>");
                sb.AppendFormat(CultureInfo.InvariantCulture, "<td>{0}</td>", WebUtility.HtmlEncode(position.Symbol));
                sb.AppendFormat(CultureInfo.InvariantCulture, "<td>{0}</td>", position.NetQuantity.ToString("F2", CultureInfo.InvariantCulture));
                sb.AppendFormat(CultureInfo.InvariantCulture, "<td>{0}</td>", WebUtility.HtmlEncode(FormatOptionalNumber(position.LastPrice)));
                sb.AppendFormat(CultureInfo.InvariantCulture, "<td>{0}</td>", WebUtility.HtmlEncode(FormatOptionalPnl(position.MarketValueUsd)));
                sb.Append("</tr>");
            }
            sb.Append("</tbody></table>");
        }

        if (slippageResult is not null)
        {
            sb.Append("<h2>PnL by currency</h2>");
            AppendCombinedPnlTable(
                sb,
                slippageResult.TheoreticalPnlByCurrency,
                slippageResult.RealPnlByCurrency);

            sb.Append(
                "<p><em>Note: Theoretical and real PnL values are presented in USD using the last available close prices (currency list retained for clarity).</em></p>");
        }

        sb.Append("</body></html>");
        return sb.ToString();
    }

    private static void AppendPnlByCurrency(StringBuilder sb, IReadOnlyDictionary<string, decimal> pnlByCurrency)
    {
        if (pnlByCurrency.Count == 0)
        {
            sb.AppendLine("    (no data)");
            return;
        }

        foreach (var entry in pnlByCurrency.OrderBy(e => e.Key, StringComparer.OrdinalIgnoreCase))
        {
            sb.Append("    ");
            sb.Append(entry.Key);
            sb.Append(": ");
            sb.AppendLine(FormatPnl(entry.Value));
        }
    }

    private static void AppendCombinedPnlTable(
        StringBuilder sb,
        IReadOnlyDictionary<string, decimal> theoreticalPnlByCurrency,
        IReadOnlyDictionary<string, decimal> realPnlByCurrency)
    {
        if (theoreticalPnlByCurrency.Count == 0 && realPnlByCurrency.Count == 0)
        {
            sb.Append("<p>(no data)</p>");
            return;
        }

        var currencies = theoreticalPnlByCurrency.Keys
            .Union(realPnlByCurrency.Keys, StringComparer.OrdinalIgnoreCase)
            .OrderBy(c => c, StringComparer.OrdinalIgnoreCase);

        sb.Append(
            "<table border=\"1\" cellpadding=\"6\" cellspacing=\"0\"><thead><tr><th>Currency</th>" +
            "<th>Theoretical PnL</th><th>Real PnL</th></tr></thead><tbody>");

        foreach (var currency in currencies)
        {
            sb.Append("<tr>");
            sb.AppendFormat(CultureInfo.InvariantCulture, "<td>{0}</td>", WebUtility.HtmlEncode(currency));
            sb.AppendFormat(
                CultureInfo.InvariantCulture,
                "<td>{0}</td>",
                WebUtility.HtmlEncode(FormatOptionalPnl(GetValueOrNull(theoreticalPnlByCurrency, currency))));
            sb.AppendFormat(
                CultureInfo.InvariantCulture,
                "<td>{0}</td>",
                WebUtility.HtmlEncode(FormatOptionalPnl(GetValueOrNull(realPnlByCurrency, currency))));
            sb.Append("</tr>");
        }

        sb.Append("</tbody></table>");

        static decimal? GetValueOrNull(IReadOnlyDictionary<string, decimal> source, string key)
            => source.TryGetValue(key, out var value) ? value : (decimal?)null;
    }

    private static string FormatPnl(decimal value)
        => value.ToString("#,##0", CultureInfo.InvariantCulture);

    private static string FormatOptionalPnl(decimal? value)
        => value.HasValue
            ? value.Value.ToString("#,##0", CultureInfo.InvariantCulture)
            : "n/a";

    private static string FormatOptionalNumber(decimal? value)
        => value.HasValue
            ? value.Value.ToString("F5", CultureInfo.InvariantCulture)
            : "n/a";

    private static void AppendExecutionSummaryText(StringBuilder sb, SlippageResult slippageResult)
    {
        var realLines = new (string Label, decimal? Value)[]
        {
            ("Real PnL (net)", slippageResult.RealPnlUsd),
            ("Commissions ($5/M)", slippageResult.CommissionsUsd),
            ("Execution slippage", slippageResult.ExecutionSlippageUsd),
            ("Missed trades PnL", slippageResult.MissedTradesPnlUsd)
        };

        var theoreticalLines = new (string Label, decimal? Value)[]
        {
            ("Theoretical PnL (net)", slippageResult.TheoreticalNetPnlUsd),
            ("Theoretical PnL (gross)", slippageResult.TheoreticalPnlUsd),
            ("Theoretical costs ($10/M)", slippageResult.TheoreticalTradingCostUsd)
        };

        AppendAlignedBlock(sb, "Real", realLines);
        AppendAlignedBlock(sb, "Theoretical", theoreticalLines);

        static void AppendAlignedBlock(StringBuilder builder, string title, IReadOnlyCollection<(string Label, decimal? Value)> lines)
        {
            builder.AppendLine($"- {title}:");
            var labelWidth = lines.Max(l => l.Label.Length);

            foreach (var (label, value) in lines)
            {
                builder.Append("  - ");
                builder.Append(label.PadRight(labelWidth));
                builder.Append(": ");
                builder.AppendLine(FormatOptionalPnl(value));
            }
        }
    }

    private static void AppendExecutionSummaryTable(StringBuilder sb, SlippageResult slippageResult)
    {
        var realLines = new (string Label, decimal? Value)[]
        {
            ("Real PnL (net)", slippageResult.RealPnlUsd),
            ("Commissions ($5/M)", slippageResult.CommissionsUsd),
            ("Execution slippage", slippageResult.ExecutionSlippageUsd),
            ("Missed trades PnL", slippageResult.MissedTradesPnlUsd)
        };

        var theoreticalLines = new (string Label, decimal? Value)[]
        {
            ("Theoretical PnL (net)", slippageResult.TheoreticalNetPnlUsd),
            ("Theoretical PnL (gross)", slippageResult.TheoreticalPnlUsd),
            ("Theoretical costs ($10/M)", slippageResult.TheoreticalTradingCostUsd)
        };

        sb.Append("<h2>Execution summary</h2>");
        sb.Append("<table style=\"border-collapse:collapse;\">");

        AppendSection("Real", realLines);
        AppendSection("Theoretical", theoreticalLines);

        sb.Append("</table>");

        void AppendSection(string title, IReadOnlyCollection<(string Label, decimal? Value)> lines)
        {
            sb.AppendFormat(
                CultureInfo.InvariantCulture,
                "<tr><th colspan=\"2\" style=\"text-align:left;padding:8px 0 4px;font-size:14px;\">{0}</th></tr>",
                WebUtility.HtmlEncode(title));

            foreach (var (label, value) in lines)
            {
                sb.Append("<tr>");
                sb.AppendFormat(
                    CultureInfo.InvariantCulture,
                    "<td style=\"padding:4px 12px 4px 0;white-space:nowrap;\">{0}</td>",
                    WebUtility.HtmlEncode(label));
                sb.AppendFormat(
                    CultureInfo.InvariantCulture,
                    "<td style=\"padding:4px 0 4px 12px;text-align:right;font-family:monospace;\">{0}</td>",
                    WebUtility.HtmlEncode(FormatOptionalPnl(value)));
                sb.Append("</tr>");
            }
        }
    }

    public async Task SendTestEmailAsync(string? subject = null, string? body = null, CancellationToken cancellationToken = default)
    {
        if (_recipients.Count == 0)
        {
            _logger.LogWarning("Skipping test email send because no recipients are configured.");
            return;
        }

        var emailSubject = string.IsNullOrWhiteSpace(subject)
            ? "TradingDaemon Email Test"
            : subject!;
        var bodyText = string.IsNullOrWhiteSpace(body)
            ? "This is a test email from the TradingDaemon service."
            : body!;
        var sanitizedHtmlBody = WebUtility.HtmlEncode(bodyText).Replace("\n", "<br />");
        var bodyHtml = $"<html><body><p>{sanitizedHtmlBody}</p></body></html>";

        await SendEmailInternalAsync(emailSubject, bodyText, bodyHtml, cancellationToken);
    }

    private async Task SendEmailInternalAsync(string subject, string bodyText, string bodyHtml, CancellationToken cancellationToken)
    {
        var request = new SendEmailRequest
        {
            Source = _fromAddress,
            Destination = new Destination
            {
                ToAddresses = _recipients.ToList()
            },
            Message = new Message
            {
                Subject = new Content(subject),
                Body = new Body
                {
                    Text = new Content(bodyText),
                    Html = new Content(bodyHtml)
                }
            }
        };

        _logger.LogInformation("Sending email to {Recipients} with subject {Subject}", string.Join(", ", _recipients), subject);

        await _sesClient.SendEmailAsync(request, cancellationToken);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposedValue)
        {
            if (disposing)
            {
                _sesClient.Dispose();
            }

            _disposedValue = true;
        }
    }

    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}
