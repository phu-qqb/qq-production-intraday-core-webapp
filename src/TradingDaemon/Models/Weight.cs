namespace TradingDaemon.Models;

public class Weight
{
    public string Symbol { get; set; } = string.Empty;
    public decimal Value { get; set; }
    public DateTime AsOf { get; set; }
}
