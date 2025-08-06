using System.Text.Json;
using Dapper;
using TradingDaemon.Data;
using TradingDaemon.Models;

namespace TradingDaemon.Services;

public class PriceFetcher
{
    private readonly IHttpClientFactory _clientFactory;
    private readonly DapperContext _context;
    private readonly ILogger<PriceFetcher> _logger;

    public PriceFetcher(IHttpClientFactory clientFactory, DapperContext context, ILogger<PriceFetcher> logger)
    {
        _clientFactory = clientFactory;
        _context = context;
        _logger = logger;
    }

    public async Task FetchAndStoreAsync()
    {
        var client = _clientFactory.CreateClient("PriceApi");
        var apiKey = Environment.GetEnvironmentVariable("PRICE_API_KEY");
        if (!string.IsNullOrEmpty(apiKey))
            client.DefaultRequestHeaders.Add("Authorization", $"Bearer {apiKey}");

        var response = await client.GetAsync("/prices");
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadAsStringAsync();
        var prices = JsonSerializer.Deserialize<IEnumerable<Price>>(json) ?? Enumerable.Empty<Price>();

        using var connection = _context.CreateConnection();
        foreach (var price in prices)
        {
            var sql = @"INSERT INTO prices (symbol, timestamp, value)
                        VALUES (@Symbol, @Timestamp, @Value)
                        ON CONFLICT (symbol, timestamp) DO UPDATE SET value = excluded.value;";
            await connection.ExecuteAsync(sql, price);
        }
    }
}
