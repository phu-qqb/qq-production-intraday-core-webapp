using System;
using System.Collections.Generic;
using Microsoft.Extensions.Options;
using TradingDaemon.Options;

namespace TradingDaemon.Data;

public interface IDatabaseObjectNameProvider
{
    string GetObjectName(DatabaseObjectIdentifier identifier);
}

public sealed class DatabaseObjectNameProvider : IDatabaseObjectNameProvider
{
    private const string ProdIntradayDatabaseName = "Intraday";
    private const string TestIntradayDatabaseName = "Intraday_Test";
    private const string IntradayDatabaseToken = "[Intraday]";

    private readonly IReadOnlyDictionary<string, string> _names;

    public DatabaseObjectNameProvider(IOptions<DatabaseObjectNameOptions>? optionsAccessor = null)
    {
        var options = optionsAccessor?.Value;
        var intradayDatabaseName = ResolveIntradayDatabaseName(options);
        var names = CreateDefaultNames(intradayDatabaseName);
        DatabaseObjectNameEnvironment? environment = null;

        if (options is not null)
        {
            environment = ResolveEnvironment(options);
            Merge(names, options.Objects);

            if (environment?.Objects is not null)
            {
                Merge(names, environment.Objects);
            }
        }

        ApplyIntradayDatabaseName(names, intradayDatabaseName);

        _names = names;
    }

    public string GetObjectName(DatabaseObjectIdentifier identifier)
    {
        if (!_names.TryGetValue(identifier.Key, out var value))
        {
            throw new InvalidOperationException($"Database object '{identifier.Key}' is not configured.");
        }

        return value;
    }

    private static void Merge(Dictionary<string, string> target, IDictionary<string, string>? source)
    {
        if (source is null)
        {
            return;
        }

        foreach (var pair in source)
        {
            if (string.IsNullOrWhiteSpace(pair.Key) || string.IsNullOrWhiteSpace(pair.Value))
            {
                continue;
            }

            target[pair.Key] = pair.Value;
        }
    }

    private static Dictionary<string, string> CreateDefaultNames(string intradayDatabaseName)
    {
        var names = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            [DatabaseObjects.IntradayModelNettedWeight.Key] = BuildIntradayObjectName(intradayDatabaseName, "model", "NettedWeight"),
            [DatabaseObjects.IntradayModel.Key] = BuildIntradayObjectName(intradayDatabaseName, "model", "Model"),
            [DatabaseObjects.IntradayCoreSecurity.Key] = BuildIntradayObjectName(intradayDatabaseName, "core", "Security"),
            [DatabaseObjects.IntradayMarketPriceBar.Key] = BuildIntradayObjectName(intradayDatabaseName, "mkt", "PriceBar"),
            [DatabaseObjects.IntradayMarketFlatBar.Key] = BuildIntradayObjectName(intradayDatabaseName, "mkt", "FlatBar"),
            [DatabaseObjects.IntradayMarketStageHistClose.Key] = BuildIntradayObjectName(intradayDatabaseName, "mkt", "Stage_HistClose"),
            [DatabaseObjects.IntradayStagingFlatBar.Key] = BuildIntradayObjectName(intradayDatabaseName, "dbo", "mkt_FlatBar_Staging"),
            [DatabaseObjects.IntradayMarketLoadRawFromStageProc.Key] = BuildIntradayObjectName(intradayDatabaseName, "mkt", "LoadRawFromStage"),
            [DatabaseObjects.IntradayMarketLoadFlatFromMinimalProc.Key] = BuildIntradayObjectName(intradayDatabaseName, "mkt", "LoadFlatFromMinimal"),
            [DatabaseObjects.WakettFill.Key] = "[wakett].[Fill]",
            [DatabaseObjects.WakettTradingLimit.Key] = "[wakett].[TradingLimit]",
            [DatabaseObjects.WakettTradingLimitBreachReport.Key] = "[wakett].[TradingLimitBreachReport]",
            [DatabaseObjects.WakettOrder.Key] = "[wakett].[Order]"
        };

        return names;
    }

    private static string ResolveIntradayDatabaseName(DatabaseObjectNameOptions? options)
    {
        return string.Equals(options?.ActiveEnvironment, "Test", StringComparison.OrdinalIgnoreCase)
            ? TestIntradayDatabaseName
            : ProdIntradayDatabaseName;
    }

    private static void ApplyIntradayDatabaseName(Dictionary<string, string> names, string databaseName)
    {
        if (string.Equals(databaseName, ProdIntradayDatabaseName, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var replacement = $"[{databaseName}]";
        var keys = new List<string>(names.Keys);

        foreach (var key in keys)
        {
            names[key] = names[key].Replace(IntradayDatabaseToken, replacement, StringComparison.OrdinalIgnoreCase);
        }
    }

    private static string BuildIntradayObjectName(string databaseName, string schema, string objectName)
    {
        return $"[{databaseName}].[{schema}].[{objectName}]";
    }

    private static DatabaseObjectNameEnvironment? ResolveEnvironment(DatabaseObjectNameOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.ActiveEnvironment))
        {
            return null;
        }

        return options.Environments != null
            && options.Environments.TryGetValue(options.ActiveEnvironment, out var environment)
            ? environment
            : null;
    }
}
