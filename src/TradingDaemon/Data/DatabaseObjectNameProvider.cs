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

    private static readonly IReadOnlyDictionary<string, string> DefaultNames = new Dictionary<string, string>(
        StringComparer.OrdinalIgnoreCase)
    {
        [DatabaseObjects.IntradayModelNettedWeight.Key] = "[Intraday].[model].[NettedWeight]",
        [DatabaseObjects.IntradayModel.Key] = "[Intraday].[model].[Model]",
        [DatabaseObjects.IntradayCoreSecurity.Key] = "[Intraday].[core].[Security]",
        [DatabaseObjects.IntradayMarketPriceBar.Key] = "[Intraday].[mkt].[PriceBar]",
        [DatabaseObjects.IntradayMarketFlatBar.Key] = "[Intraday].[mkt].[FlatBar]",
        [DatabaseObjects.IntradayMarketStageHistClose.Key] = "[Intraday].[mkt].[Stage_HistClose]",
        [DatabaseObjects.IntradayStagingFlatBar.Key] = "[Intraday].[dbo].[mkt_FlatBar_Staging]",
        [DatabaseObjects.IntradayMarketLoadRawFromStageProc.Key] = "[Intraday].[mkt].[LoadRawFromStage]",
        [DatabaseObjects.IntradayMarketLoadFlatFromMinimalProc.Key] = "[Intraday].[mkt].[LoadFlatFromMinimal]",
        [DatabaseObjects.WakettFill.Key] = "[wakett].[Fill]",
        [DatabaseObjects.WakettTradingLimit.Key] = "[wakett].[TradingLimit]",
        [DatabaseObjects.WakettTradingLimitBreachReport.Key] = "[wakett].[TradingLimitBreachReport]",
        [DatabaseObjects.WakettOrder.Key] = "[wakett].[Order]"
    };

    private readonly IReadOnlyDictionary<string, string> _names;

    public DatabaseObjectNameProvider(IOptions<DatabaseObjectNameOptions>? optionsAccessor = null)
    {
        var names = new Dictionary<string, string>(DefaultNames, StringComparer.OrdinalIgnoreCase);
        var options = optionsAccessor?.Value;
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

        var intradayDatabaseName = ResolveIntradayDatabaseName(options);
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
