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
    Task SendPnLReportAsync(PnlReport report, CancellationToken cancellationToken = default);
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

    public async Task SendPnLReportAsync(PnlReport report, CancellationToken cancellationToken = default)
    {
        if (_recipients.Count == 0)
        {
            return;
        }

        var subject = $"Intraday PnL for {report.TradingDate:yyyy-MM-dd}";
        var bodyText = BuildPlainTextBody(report);
        var bodyHtml = BuildHtmlBody(report);

        await SendEmailInternalAsync(subject, bodyText, bodyHtml, cancellationToken);
    }

    private static string BuildPlainTextBody(PnlReport report)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"Date: {report.TradingDate:yyyy-MM-dd}");
        sb.AppendLine($"PnL: {FormatCurrency(report.Pnl)}");
        sb.AppendLine($"Gross Market Value: {FormatCurrency(report.GrossMarketValue)}");
        sb.AppendLine($"Total Net Exposure: {FormatCurrency(report.TotalNetExposure)}");
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
                sb.Append(FormatOptionalCurrency(position.MarketValueUsd));
                sb.AppendLine();
            }
        }

        return sb.ToString();
    }

    private static string BuildHtmlBody(PnlReport report)
    {
        var sb = new StringBuilder();
        sb.Append("<html><body><h1>Intraday PnL</h1>");
        sb.AppendFormat(CultureInfo.InvariantCulture, "<p><strong>Date:</strong> {0:yyyy-MM-dd}</p>", report.TradingDate);
        sb.AppendFormat(CultureInfo.InvariantCulture, "<p><strong>PnL:</strong> {0}</p>", FormatCurrency(report.Pnl));
        sb.AppendFormat(CultureInfo.InvariantCulture, "<p><strong>Gross Market Value:</strong> {0}</p>", FormatCurrency(report.GrossMarketValue));
        sb.AppendFormat(CultureInfo.InvariantCulture, "<p><strong>Total Net Exposure:</strong> {0}</p>", FormatCurrency(report.TotalNetExposure));

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
                sb.AppendFormat(CultureInfo.InvariantCulture, "<td>{0}</td>", WebUtility.HtmlEncode(FormatOptionalCurrency(position.MarketValueUsd)));
                sb.Append("</tr>");
            }
            sb.Append("</tbody></table>");
        }

        sb.Append("</body></html>");
        return sb.ToString();
    }

    private static string FormatCurrency(decimal value)
        => value.ToString("F2", CultureInfo.InvariantCulture);

    private static string FormatOptionalCurrency(decimal? value)
        => value.HasValue
            ? value.Value.ToString("F2", CultureInfo.InvariantCulture)
            : "n/a";

    private static string FormatOptionalNumber(decimal? value)
        => value.HasValue
            ? value.Value.ToString("F5", CultureInfo.InvariantCulture)
            : "n/a";

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
