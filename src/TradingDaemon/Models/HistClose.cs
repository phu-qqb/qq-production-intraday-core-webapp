namespace TradingDaemon.Models;

public class HistClose
{
    public string SecurityId { get; set; } = string.Empty;
    public DateTime BarTimeUtc { get; set; }
    public decimal Close { get; set; }
}
