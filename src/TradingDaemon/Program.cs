using Serilog;
using TradingDaemon.Controllers;
using TradingDaemon.Data;
using TradingDaemon.Logging;
using TradingDaemon.Services;
using TradingDaemon.Utils;

var builder = WebApplication.CreateBuilder(args);

SerilogConfig.Configure(builder.Configuration);
builder.Host.UseSerilog();

builder.Services.AddSingleton<DapperContext>();

builder.Services.AddHttpClient("PriceApi", client =>
{
    client.BaseAddress = new Uri(builder.Configuration["ExternalApis:PriceApi:BaseUrl"] ?? "");
}).AddPolicyHandler(RetryPolicyFactory.GetPolicy());

builder.Services.AddHttpClient("OrderApi", client =>
{
    client.BaseAddress = new Uri(builder.Configuration["ExternalApis:OrderApi:BaseUrl"] ?? "");
}).AddPolicyHandler(RetryPolicyFactory.GetPolicy());

builder.Services.AddHttpClient("WakettApi", client =>
{
    client.BaseAddress = new Uri(builder.Configuration["ExternalApis:WakettApi:BaseUrl"] ?? "");
}).AddPolicyHandler(RetryPolicyFactory.GetPolicy());

builder.Services.AddHttpClient("WakettTradeApi", client =>
{
    var tradeBaseUrl = builder.Configuration["ExternalApis:WakettApi:TradeBaseUrl"]
        ?? builder.Configuration["ExternalApis:WakettApi:BaseUrl"]
        ?? string.Empty;

    if (!string.IsNullOrWhiteSpace(tradeBaseUrl))
    {
        client.BaseAddress = new Uri(tradeBaseUrl);
    }
}).AddPolicyHandler(RetryPolicyFactory.GetPolicy());

builder.Services.AddTransient<PriceFetcher>();
builder.Services.AddTransient<WeightCalculator>();
builder.Services.AddTransient<OrderSender>();

builder.Services.AddTransient<ReportRunner>();

builder.Services.AddTransient<WakettApiClient>();
builder.Services.AddTransient<WakettPriceFetcher>();
builder.Services.AddTransient<WakettTradeFetcher>();


builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

//if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "TradingDaemon API v1");
        c.RoutePrefix = string.Empty;  // Swagger accessible à la racine
    });
}

app.MapFillEndpoints();
app.MapPriceEndpoints();
app.MapWeightEndpoints();
app.MapOrderEndpoints();
app.MapTradingEndpoints();
app.MapReportEndpoints();
app.MapWakettEndpoints();

app.Run();
