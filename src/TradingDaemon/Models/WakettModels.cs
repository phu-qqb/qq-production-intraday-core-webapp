namespace TradingDaemon.Models;

public class WakettPriceRequest
{
    public List<string> Symbols { get; set; } = new();
    public DateTimeOffset? Ts { get; set; }
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
    public string? Ts { get; set; }
    public double? Aum { get; set; }
    public List<WakettOrderItem> Orders { get; set; } = new();
}

public class WakettOrderItem
{
    public string Symbol { get; set; } = string.Empty;
    public string Side { get; set; } = string.Empty;
    public string Code { get; set; } = string.Empty;
    public WakettOrderSize Size { get; set; } = new();
}

public class WakettOrderSize
{
    public double Value { get; set; }
    public string Type { get; set; } = string.Empty;
}

public class WakettOrderResponse
{
    public string Ts { get; set; } = string.Empty;
    public List<WakettOrderResult> Orders { get; set; } = new();
}

public class WakettOrderResult
{
    public string Symbol { get; set; } = string.Empty;
    public string Side { get; set; } = string.Empty;
    public double? Qt { get; set; }
    public string Code { get; set; } = string.Empty;
    public string? OrderID { get; set; }
    public WakettError? Error { get; set; }
}

public class WakettTradeRequest
{
    public string Account { get; set; } = string.Empty;
    public string From { get; set; } = string.Empty;
    public string To { get; set; } = string.Empty;
    public string Strategy { get; set; } = string.Empty;
}

public class WakettTradeResponse
{
    public string Status { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public List<WakettTrade> Data { get; set; } = new();
}

public class WakettTrade
{
    public string Portfolio { get; set; } = string.Empty;
    public string Broker { get; set; } = string.Empty;
    public string StrategyID { get; set; } = string.Empty;
    public string Symbol { get; set; } = string.Empty;
    public string Side { get; set; } = string.Empty;
    public double Price { get; set; }
    public double OrderSize { get; set; }
    public double ExecuteSize { get; set; }
    public double ExecutePrice { get; set; }
    public string Event { get; set; } = string.Empty;
}

public class WakettError
{
    public string Code { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
}
