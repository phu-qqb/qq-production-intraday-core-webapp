namespace TradingDaemon.Models;

public class Price
{
    public string Symbol { get; set; } = string.Empty;
    public decimal Value { get; set; }
    public DateTime Timestamp { get; set; }
}
