using System.Text.Json;
using System.Net.Http.Json;
using System.Linq;
using TradingDaemon.Models;

namespace TradingDaemon.Services;

public class WakettApiClient
{
    private readonly IHttpClientFactory _clientFactory;
    private readonly JsonSerializerOptions _jsonOptions = new() { PropertyNameCaseInsensitive = true };

    public WakettApiClient(IHttpClientFactory clientFactory)
    {
        _clientFactory = clientFactory;
    }

    public async Task<WakettPriceResponse?> GetPricesAsync(IEnumerable<WakettSecuritySymbol> symbols, DateTimeOffset? ts = null)
    {
        var client = _clientFactory.CreateClient("WakettApi");
        var payload = new
        {
            ts = ts?.ToString("yyyy-MM-dd HH:mm:ss.fffzzz"),
            symbols = symbols.Select(s => new { securityid = s.SecurityId, symbol = s.Symbol })
        };
        var response = await client.PostAsJsonAsync("prices", payload, _jsonOptions);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<WakettPriceResponse>(json, _jsonOptions);
    }

    public async Task<WakettOrderResponse?> SendOrdersAsync(WakettOrderRequest request)
    {
        var client = _clientFactory.CreateClient("WakettApi");
        var response = await client.PostAsJsonAsync("orders", request, _jsonOptions);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<WakettOrderResponse>(json, _jsonOptions);
    }

    public async Task<WakettTradeResponse?> GetTradesAsync(WakettTradeRequest request)
    {
        var client = _clientFactory.CreateClient("WakettApi");
        var response = await client.PostAsJsonAsync("trades", request, _jsonOptions);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<WakettTradeResponse>(json, _jsonOptions);
    }
}
