namespace TradingDaemon.Options;

public sealed class PriceBarOptions
{
    public int TimeframeMinute { get; set; } = 15;
    public int SourceId { get; set; } = 1;
}
