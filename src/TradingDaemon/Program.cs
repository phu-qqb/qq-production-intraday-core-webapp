using Serilog;
using TradingDaemon.Controllers;
using TradingDaemon.Data;
using TradingDaemon.Logging;
using TradingDaemon.Middleware;
using TradingDaemon.Services;
using TradingDaemon.Models;
using TradingDaemon.Utils;
using TradingDaemon.Options;

var builder = WebApplication.CreateBuilder(args);

ConfigurationEnvironmentExtensions.ApplyEnvironmentOverrides(
    builder.Configuration,
    "ExternalApis",
    builder.Configuration["Database:ActiveEnvironment"]);

ConfigurationEnvironmentExtensions.ApplyEnvironmentOverrides(
    builder.Configuration,
    "Automation",
    builder.Configuration["Database:ActiveEnvironment"]);

SerilogConfig.Configure(builder.Configuration);
builder.Host.UseSerilog();

builder.Services.Configure<DatabaseObjectNameOptions>(builder.Configuration.GetSection("Database"));
builder.Services.AddSingleton<IDatabaseObjectNameProvider, DatabaseObjectNameProvider>();
builder.Services.AddSingleton<IPriceProcessingProcedureExecutor, PriceProcessingProcedureExecutor>();
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
builder.Services.AddTransient<PnlReportService>();

builder.Services.AddTransient<ReportRunner>();

builder.Services.AddTransient<WakettApiClient>();
builder.Services.AddTransient<WakettPriceFetcher>();
builder.Services.AddTransient<WakettTradeFetcher>();
builder.Services.Configure<WakettAutomationOptions>(builder.Configuration.GetSection("Automation:Wakett"));
builder.Services.Configure<TradingOptions>(builder.Configuration.GetSection("Trading"));
builder.Services.Configure<PriceBarOptions>(builder.Configuration.GetSection("PriceBars"));
builder.Services.AddHostedService<WakettAutomationService>();

builder.Services.AddSingleton<IEmailNotificationService, EmailNotificationService>();



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

app.UseMiddleware<SqlTimeoutLoggingMiddleware>();

app.MapFillEndpoints();
app.MapPriceEndpoints();
app.MapWeightEndpoints();
app.MapOrderEndpoints();
app.MapTradingEndpoints();
app.MapReportEndpoints();
app.MapWakettEndpoints();
app.MapEmailEndpoints();

app.Run();
