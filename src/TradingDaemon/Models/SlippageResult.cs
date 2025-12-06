namespace TradingDaemon.Models;

public sealed record SlippageResult(
    DateOnly TradingDate,
    decimal TheoreticalPnlUsd,
    decimal RealPnlUsd,
    decimal SlippageAndMissedCostUsd);
