namespace TradingDaemon.Models;

public class ReportRequest
{
    public int ModelId { get; set; }
    public int Timeframe { get; set; }
    public string? FromDate { get; set; }
    public string? ToDate { get; set; }
    public int? AnnualizeDays { get; set; }
    public int? TopNPairs { get; set; }
    public string? OutputDir { get; set; }
}
