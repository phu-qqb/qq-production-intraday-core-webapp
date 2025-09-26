using System;
using System.Text.Json.Serialization;

namespace TradingDaemon.Models;

public sealed class FetchWakettFillsRequest
{
    public string Account { get; set; } = string.Empty;
    public string From { get; set; } = string.Empty;
    public string To { get; set; } = string.Empty;
    public string? Strategy { get; set; }
}

public sealed record WakettFillUploadResponse(
    string Account,
    string From,
    string To,
    string? Strategy,
    string Status,
    string Message,
    int TotalRecords,
    int InsertedRecords,
    int UpdatedRecords,
    DateTime RequestedAtUtc,
    IReadOnlyList<WakettFillUploadSkippedRecord> SkippedRecords)
{
    [JsonIgnore]
    public int PersistedRecords => InsertedRecords + UpdatedRecords;
}

public sealed record WakettFillUploadSkippedRecord(
    string Reason,
    string? ExecuteId,
    string? Symbol);
