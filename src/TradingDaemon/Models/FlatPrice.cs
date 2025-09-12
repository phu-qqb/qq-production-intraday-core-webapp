namespace TradingDaemon.Models;

public class FlatPrice
{
    public string SecurityId { get; set; } = string.Empty;
    public DateTime BarTimeUtc { get; set; }
    public decimal Close { get; set; }
    public string Session { get; set; } = string.Empty;
}
