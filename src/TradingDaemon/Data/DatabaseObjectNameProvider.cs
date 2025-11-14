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

        if (options is not null)
        {
            Merge(names, options.Objects);

            var environment = ResolveEnvironment(options);
            if (environment?.Objects is not null)
            {
                Merge(names, environment.Objects);
            }
        }

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
