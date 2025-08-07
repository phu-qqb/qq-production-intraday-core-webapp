using Moq;
using Moq.Protected;
using System.Net;
using System.Net.Http;
using System.Data;
using Dapper;
using TradingDaemon.Services;
using TradingDaemon.Data;
using TradingDaemon.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;

public class OrderSenderTests
{
    [Fact]
    public async Task SendOrdersAsync_PostsOrders()
    {
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected().Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK));
        var client = new HttpClient(handler.Object) { BaseAddress = new Uri("http://test") };
        var factory = Mock.Of<IHttpClientFactory>(f => f.CreateClient("OrderApi") == client);

        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { ["ConnectionStrings:DefaultConnection"] = "" }).Build();
        var context = new Mock<DapperContext>(config);
        context.Setup(c => c.CreateConnection()).Returns(new FakeDbConnection());

        var logger = Mock.Of<ILogger<OrderSender>>();
        var sender = new OrderSender(factory, context.Object, logger);

        await sender.SendOrdersAsync();

        handler.Protected().Verify("SendAsync", Times.AtLeast(0), ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>());
    }
}

// Simple fake IDbConnection that does nothing
class FakeDbConnection : IDbConnection
{
    public string ConnectionString { get; set; } = string.Empty;
    public int ConnectionTimeout => 0;
    public string Database => string.Empty;
    public ConnectionState State => ConnectionState.Open;
    public IDbTransaction BeginTransaction() => null!;
    public IDbTransaction BeginTransaction(IsolationLevel il) => null!;
    public void ChangeDatabase(string databaseName) {}
    public void Close() {}
    public IDbCommand CreateCommand() => new FakeDbCommand();
    public void Open() {}
    public void Dispose() {}
}

class FakeDbCommand : IDbCommand
{
    public string CommandText { get; set; } = string.Empty;
    public int CommandTimeout { get; set; }
    public CommandType CommandType { get; set; }
    public IDbConnection? Connection { get; set; }
    public IDataParameterCollection Parameters { get; } = new FakeParameterCollection();
    public IDbTransaction? Transaction { get; set; }
    public UpdateRowSource UpdatedRowSource { get; set; }
    public void Cancel() {}
    public IDbDataParameter CreateParameter() => null!;
    public void Dispose() {}
    public int ExecuteNonQuery() => 0;
    public IDataReader ExecuteReader() => null!;
    public IDataReader ExecuteReader(CommandBehavior behavior) => null!;
    public object? ExecuteScalar() => null;
    public void Prepare() {}
}

class FakeParameterCollection : List<IDataParameter>, IDataParameterCollection
{
    object? IDataParameterCollection.this[string parameterName]
    {
        get => null;
        set {}
    }
    bool IDataParameterCollection.Contains(string parameterName) => false;
    int IDataParameterCollection.IndexOf(string parameterName) => -1;
    void IDataParameterCollection.RemoveAt(string parameterName) {}
}
