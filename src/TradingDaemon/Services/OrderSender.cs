using System.Text;
using System.Text.Json;
using Dapper;
using TradingDaemon.Data;
using TradingDaemon.Models;

namespace TradingDaemon.Services;

public class OrderSender
{
    private readonly IHttpClientFactory _clientFactory;
    private readonly DapperContext _context;
    private readonly ILogger<OrderSender> _logger;

    public OrderSender(IHttpClientFactory clientFactory, DapperContext context, ILogger<OrderSender> logger)
    {
        _clientFactory = clientFactory;
        _context = context;
        _logger = logger;
    }

    public async Task SendOrdersAsync()
    {
        using var connection = _context.CreateConnection();
        var sql = "SELECT symbol, value FROM weights";
        _logger.LogInformation("Executing SQL: {Sql}", sql);
        var weights = await connection.QueryAsync<Weight>(sql);
        var client = _clientFactory.CreateClient("OrderApi");
        var apiKey = Environment.GetEnvironmentVariable("ORDER_API_KEY") ?? string.Empty;

        foreach (var weight in weights)
        {
            var payload = JsonSerializer.Serialize(new { symbol = weight.Symbol, weight = weight.Value });
            var request = new HttpRequestMessage(HttpMethod.Post, "/orders")
            {
                Content = new StringContent(payload, Encoding.UTF8, "application/json")
            };
            request.Headers.Add("Authorization", $"Bearer {apiKey}");
            var response = await client.SendAsync(request);
            response.EnsureSuccessStatusCode();
        }
    }
}
