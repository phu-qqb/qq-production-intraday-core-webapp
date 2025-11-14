using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;

namespace TradingDaemon.Utils;

public static class ConfigurationEnvironmentExtensions
{
    public static void ApplyEnvironmentOverrides(
        ConfigurationManager configuration,
        string sectionName,
        string? defaultEnvironment = null)
    {
        if (configuration is null)
        {
            throw new ArgumentNullException(nameof(configuration));
        }

        var activeEnvironment = configuration[$"{sectionName}:ActiveEnvironment"]
            ?? defaultEnvironment;

        if (string.IsNullOrWhiteSpace(activeEnvironment))
        {
            return;
        }

        var overrides = configuration.GetSection($"{sectionName}:Environments:{activeEnvironment}");
        if (!overrides.Exists())
        {
            return;
        }

        var flattened = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        FlattenSection(overrides, sectionName, flattened);

        if (flattened.Count > 0)
        {
            configuration.AddInMemoryCollection(flattened);
        }
    }

    private static void FlattenSection(
        IConfiguration section,
        string prefix,
        IDictionary<string, string?> target)
    {
        foreach (var child in section.GetChildren())
        {
            var key = string.IsNullOrEmpty(prefix)
                ? child.Key
                : string.Concat(prefix, ":", child.Key);

            if (!child.GetChildren().Any())
            {
                target[key] = child.Value;
            }
            else
            {
                FlattenSection(child, key, target);
            }
        }
    }
}
