namespace TradingDaemon.Models;

public class Fill
{
    public int Id { get; set; }
    public string Symbol { get; set; } = string.Empty;
    public decimal Quantity { get; set; }
    public decimal Price { get; set; }
    public DateTime Timestamp { get; set; }
}
