using System.Text.Json.Serialization;

namespace TradingDaemon.Models;

public class WakettSecuritySymbol
{
    public int SecurityId { get; set; }
    public string Symbol { get; set; } = string.Empty;
}

public class WakettPriceResponse
{
    public string Ts { get; set; } = string.Empty;
    public List<WakettPrice> Prices { get; set; } = new();
}

public class WakettPrice
{
    public string Symbol { get; set; } = string.Empty;
    public decimal? Bid { get; set; }
    public decimal? Ask { get; set; }
    public decimal? Mid { get; set; }
    public decimal? Size { get; set; }
    public WakettError? Error { get; set; }
}

public class WakettOrderRequest
{
    public string? ts { get; set; }
    public double? aum { get; set; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? execution { get; set; }
    public List<WakettOrderItem> orders { get; set; } = new();
}

public class WakettOrderItem
{
    public string symbol { get; set; } = string.Empty;
    public string side { get; set; } = string.Empty;
    public string code { get; set; } = string.Empty;
    public WakettOrderSize size { get; set; } = new();
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public WakettOrderSteps? steps { get; set; }
}

public class WakettOrderSize
{
    public double value { get; set; }
    public string type { get; set; } = string.Empty;
}

public class WakettOrderSteps
{
    public double? start { get; set; }
    public double? end { get; set; }
    public double? speed { get; set; }
}

public class WakettOrderResponse
{
    [JsonPropertyName("ts")]
    public string Timestamp { get; set; } = string.Empty;

    [JsonPropertyName("orders")]
    public List<WakettOrderResult> Orders { get; set; } = new();
}

public class WakettOrderResult
{
    [JsonPropertyName("symbol")]
    public string Symbol { get; set; } = string.Empty;

    [JsonPropertyName("side")]
    public string Side { get; set; } = string.Empty;

    [JsonPropertyName("size")]
    public double? Size { get; set; }

    [JsonPropertyName("code")]
    public string Code { get; set; } = string.Empty;

    [JsonPropertyName("orderID")]
    public string? OrderId { get; set; }

    [JsonPropertyName("trades")]
    public List<WakettOrderTrade> Trades { get; set; } = new();

    [JsonPropertyName("error")]
    public WakettError? Error { get; set; }
}

public class WakettOrderTrade
{
    [JsonPropertyName("id")]
    public string Id { get; set; } = string.Empty;

    [JsonPropertyName("side")]
    public string Side { get; set; } = string.Empty;

    [JsonPropertyName("qt")]
    public double Quantity { get; set; }
}

public class WakettTradeRequest
{
    public string Account { get; set; } = string.Empty;
    public string From { get; set; } = string.Empty;
    public string To { get; set; } = string.Empty;

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Strategy { get; set; }
}

public class WakettTradeResponse
{
    public string Status { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public List<WakettTrade> Data { get; set; } = new();
}

public class WakettTrade
{
    [JsonPropertyName("portfolioID")]
    public string? PortfolioId { get; set; }

    [JsonPropertyName("portfolio")]
    public string? Portfolio { get; set; }

    [JsonPropertyName("alias")]
    public string? Alias { get; set; }

    [JsonPropertyName("broker")]
    public string? Broker { get; set; }

    [JsonPropertyName("strategyID")]
    public string? StrategyId { get; set; }

    [JsonPropertyName("symbolID")]
    public string? SymbolId { get; set; }

    [JsonPropertyName("symbol")]
    public string? Symbol { get; set; }

    [JsonPropertyName("instrumentID")]
    public string? InstrumentId { get; set; }

    [JsonPropertyName("ref")]
    public string? Reference { get; set; }

    [JsonPropertyName("id")]
    public int? CoreOrderId { get; set; }

    [JsonPropertyName("sub")]
    public int? SubOrderId { get; set; }

    [JsonPropertyName("label")]
    public string? Label { get; set; }

    [JsonPropertyName("side")]
    public string? Side { get; set; }

    [JsonPropertyName("price")]
    public double? Price { get; set; }

    [JsonPropertyName("orderID")]
    public string? OrderId { get; set; }

    [JsonPropertyName("orderTS")]
    public string? OrderTimestamp { get; set; }

    [JsonPropertyName("orderSize")]
    public double? OrderSize { get; set; }

    [JsonPropertyName("orderChannel")]
    public string? OrderChannel { get; set; }

    [JsonPropertyName("providerID")]
    public string? ProviderId { get; set; }

    [JsonPropertyName("providerTS")]
    public string? ProviderTimestamp { get; set; }

    [JsonPropertyName("executeID")]
    public string? ExecuteId { get; set; }

    [JsonPropertyName("executeTS")]
    public string? ExecuteTimestamp { get; set; }

    [JsonPropertyName("entitySize")]
    public double? EntitySize { get; set; }

    [JsonPropertyName("executeSize")]
    public double? ExecuteSize { get; set; }

    [JsonPropertyName("executePrice")]
    public double? ExecutePrice { get; set; }

    [JsonPropertyName("tradets")]
    public string? TradeTimestamp { get; set; }

    [JsonPropertyName("event")]
    public string? Event { get; set; }

    [JsonPropertyName("user")]
    public string? User { get; set; }

    [JsonPropertyName("code")]
    public string? Code { get; set; }

    [JsonPropertyName("type")]
    public string? Type { get; set; }

    [JsonPropertyName("quote")]
    public double? Quote { get; set; }

    [JsonPropertyName("amount")]
    public double? Amount { get; set; }

    [JsonPropertyName("rate")]
    public double? Rate { get; set; }
}

public class WakettError
{
    public string Code { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
}
