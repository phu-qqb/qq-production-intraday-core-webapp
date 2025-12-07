namespace TradingDaemon.Models;

public sealed record SlippageResult(
    DateOnly TradingDate,
    IReadOnlyDictionary<string, decimal> TheoreticalPnlByCurrency,
    IReadOnlyDictionary<string, decimal> RealPnlByCurrency,
    decimal? TheoreticalPnlUsd,
    decimal? RealPnlUsd,
    decimal? SlippageAndMissedCostUsd,
    bool HasFivePmBar);
