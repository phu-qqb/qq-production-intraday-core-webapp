using System;
using System.Linq;

namespace TradingDaemon.Options;

public sealed class PriceBarOptions
{
    public int TimeframeMinute { get; set; } = 15;
    public int[] TimeframeMinutes { get; set; } = Array.Empty<int>();
    public int SourceId { get; set; } = 1;

    public IReadOnlyList<int> GetOrderedTimeframeMinutes()
    {
        var candidates = (TimeframeMinutes?.Length > 0 ? TimeframeMinutes : Array.Empty<int>())
            .Append(TimeframeMinute)
            .Select(min => Math.Max(1, min))
            .Distinct()
            .OrderBy(min => min)
            .ToArray();

        if (candidates.Length > 0)
        {
            return candidates;
        }

        return new[] { Math.Max(1, TimeframeMinute) };
    }
}
