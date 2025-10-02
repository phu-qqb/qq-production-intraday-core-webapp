using Amazon;
using Amazon.SimpleEmail;
using Amazon.SimpleEmail.Model;

namespace TradingDaemon.Services;

public interface IEmailNotificationService
{
    Task SendPnLReportAsync(DateTime date, decimal pnl, CancellationToken cancellationToken = default);
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

        _fromAddress = configuration["Email:From"] ?? "intraday_bot@quantumqb.com";
        var recipients = configuration.GetSection("Email:Recipients").Get<string[]>() ?? Array.Empty<string>();
        _recipients = Array.AsReadOnly(recipients);

        if (_recipients.Count == 0)
        {
            _logger.LogWarning("No email recipients configured. PnL emails will be skipped.");
        }
    }

    public async Task SendPnLReportAsync(DateTime date, decimal pnl, CancellationToken cancellationToken = default)
    {
        if (_recipients.Count == 0)
        {
            return;
        }

        var subject = $"Intraday PnL for {date:yyyy-MM-dd}";
        var bodyText = $"Date: {date:yyyy-MM-dd}\nPnL: {pnl:F2}";
        var bodyHtml = $"<html><body><h1>Intraday PnL</h1><p><strong>Date:</strong> {date:yyyy-MM-dd}</p><p><strong>PnL:</strong> {pnl:F2}</p></body></html>";

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

        _logger.LogInformation("Sending PnL email to {Recipients} with subject {Subject}", string.Join(", ", _recipients), subject);

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
