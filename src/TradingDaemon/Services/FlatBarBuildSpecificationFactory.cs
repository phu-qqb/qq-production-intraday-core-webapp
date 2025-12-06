using System;
using System.Collections.Generic;

namespace TradingDaemon.Services;

public readonly record struct FlatBarBuildSpecification(int TimeframeMinute, int OffsetMinute);

public static class FlatBarBuildSpecificationFactory
{
    private static readonly IReadOnlyList<FlatBarBuildSpecification> AdditionalSpecifications = new[]
    {
        new FlatBarBuildSpecification(30, 6),
        new FlatBarBuildSpecification(60, 6)
    };

    public static IReadOnlyList<FlatBarBuildSpecification> CreateDefault(int baseTimeframeMinute, int baseOffsetMinute)
        => Create(baseTimeframeMinute, baseOffsetMinute, AdditionalSpecifications);

    private static IReadOnlyList<FlatBarBuildSpecification> Create(
        int baseTimeframeMinute,
        int baseOffsetMinute,
        IReadOnlyList<FlatBarBuildSpecification> additionalSpecifications)
    {
        var builds = new List<FlatBarBuildSpecification>();
        var seen = new HashSet<(int TimeframeMinute, int OffsetMinute)>();

        void TryAdd(int timeframeMinute, int offsetMinute)
        {
            if (timeframeMinute <= 0)
            {
                return;
            }

            var normalized = NormalizeOffset(offsetMinute);
            var sanitizedTimeframe = Math.Max(1, timeframeMinute);
            if (seen.Add((sanitizedTimeframe, normalized)))
            {
                builds.Add(new FlatBarBuildSpecification(sanitizedTimeframe, normalized));
            }
        }

        TryAdd(baseTimeframeMinute, baseOffsetMinute);

        foreach (var spec in additionalSpecifications)
        {
            TryAdd(spec.TimeframeMinute, spec.OffsetMinute);
        }

        return builds;
    }

    private static int NormalizeOffset(int offsetMinute)
    {
        var normalized = offsetMinute % 60;
        if (normalized < 0)
        {
            normalized += 60;
        }

        return normalized;
    }
}
