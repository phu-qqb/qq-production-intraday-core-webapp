namespace TradingDaemon.Models;

public sealed class WakettAutomationOptions
{
    public int WorkflowMinuteOffset { get; set; } = 6;

    public int FillIntervalMinutes { get; set; } = 10;

    public string FillAccount { get; set; } = string.Empty;

    public string? FillStrategy { get; set; }
}

