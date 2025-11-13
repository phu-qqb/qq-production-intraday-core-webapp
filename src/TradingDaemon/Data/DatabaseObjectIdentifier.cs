namespace TradingDaemon.Data;

public readonly record struct DatabaseObjectIdentifier(string Key)
{
    public override string ToString() => Key;
}

public static class DatabaseObjects
{
    public static readonly DatabaseObjectIdentifier IntradayModelNettedWeight = new("Intraday.model.NettedWeight");
    public static readonly DatabaseObjectIdentifier IntradayModel = new("Intraday.model.Model");
    public static readonly DatabaseObjectIdentifier IntradayCoreSecurity = new("Intraday.core.Security");
    public static readonly DatabaseObjectIdentifier IntradayMarketPriceBar = new("Intraday.mkt.PriceBar");
    public static readonly DatabaseObjectIdentifier IntradayMarketStageHistClose = new("Intraday.mkt.Stage_HistClose");
    public static readonly DatabaseObjectIdentifier IntradayStagingFlatBar = new("Intraday.dbo.mkt_FlatBar_Staging");
    public static readonly DatabaseObjectIdentifier WakettFill = new("wakett.Fill");
    public static readonly DatabaseObjectIdentifier WakettTradingLimit = new("wakett.TradingLimit");
    public static readonly DatabaseObjectIdentifier WakettTradingLimitBreachReport = new("wakett.TradingLimitBreachReport");
    public static readonly DatabaseObjectIdentifier WakettOrder = new("wakett.Order");
}
