using System.Text.Json;
using System.Text.Json.Serialization;
using System.Net.Http.Json;
using System.Linq;
using System.Globalization;
using System.Text;
using System.Collections.Generic;
using TradingDaemon.Models;

namespace TradingDaemon.Services;

public class WakettApiClient
{
    private readonly IHttpClientFactory _clientFactory;
    private readonly JsonSerializerOptions _jsonOptions = new() { PropertyNameCaseInsensitive = true };
    private static readonly JsonSerializerOptions OrderRequestJsonOptions = new()
    {
        PropertyNamingPolicy = LowerCaseNamingPolicy.Instance,
        DictionaryKeyPolicy = LowerCaseNamingPolicy.Instance
    };

    private static readonly JsonSerializerOptions TradeRequestJsonOptions = new()
    {
        PropertyNamingPolicy = LowerCaseNamingPolicy.Instance,
        DictionaryKeyPolicy = LowerCaseNamingPolicy.Instance,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public WakettApiClient(IHttpClientFactory clientFactory)
    {
        _clientFactory = clientFactory;
    }

    public async Task<WakettPriceResponse?> GetPricesAsync(IEnumerable<WakettSecuritySymbol> symbols, DateTimeOffset? ts = null)
    {
        var client = _clientFactory.CreateClient("WakettApi");
        string? formattedTs = null;
        if (ts.HasValue)
        {
            var etZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");

            var etTime = TimeZoneInfo.ConvertTime(ts.Value, etZone);

            // Avec offset (+HH:mm)
            formattedTs = etTime.ToString("yyyy-MM-dd HH:mm:ss.fffK", CultureInfo.InvariantCulture);
        }

        var payload = new
        {
            ts = formattedTs,
            symbols = symbols.Select(s => s.Symbol)
        };
        var response = await client.PostAsJsonAsync("prices", payload, _jsonOptions);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<WakettPriceResponse>(json, _jsonOptions);
    }

    public async Task<WakettOrderResponse?> SendOrdersAsync(WakettOrderRequest request)
    {
        var client = _clientFactory.CreateClient("WakettApi");
        var jsonBody = JsonSerializer.Serialize(request, OrderRequestJsonOptions);
        System.Console.WriteLine($"Sending Wakett order request: {jsonBody}");
        var content = new StringContent(jsonBody, Encoding.UTF8, "application/json");
        var response = await client.PostAsync("orders", content);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<WakettOrderResponse>(json, _jsonOptions);
    }

    public async Task<WakettTradeResponse?> GetTradesAsync(WakettTradeRequest request)
    {
        var client = _clientFactory.CreateClient("WakettApi");

        var jsonBody = JsonSerializer.Serialize(request, TradeRequestJsonOptions);
        var content = new StringContent(jsonBody, Encoding.UTF8, "application/json");

        var response = await client.PostAsync("trades", content);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<WakettTradeResponse>(json, _jsonOptions);
    }
}

internal sealed class LowerCaseNamingPolicy : JsonNamingPolicy
{
    public static LowerCaseNamingPolicy Instance { get; } = new();

    private LowerCaseNamingPolicy()
    {
    }

    public override string ConvertName(string name)
    {
        return name.ToLowerInvariant();
    }
}
