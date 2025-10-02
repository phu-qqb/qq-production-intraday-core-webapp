using System.Collections.Generic;

namespace TradingDaemon.Models;

public sealed record PnlReport(
    DateOnly TradingDate,
    decimal Pnl,
    decimal GrossMarketValue,
    decimal TotalNetExposure,
    IReadOnlyList<PnlReportPosition> Positions);

public sealed record PnlReportPosition(
    string Symbol,
    string BaseCurrency,
    string QuoteCurrency,
    decimal NetQuantity,
    decimal? LastPrice,
    decimal? MarketValueUsd);
