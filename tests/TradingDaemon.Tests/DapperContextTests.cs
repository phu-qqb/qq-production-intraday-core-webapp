using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using TradingDaemon.Data;
using Xunit;

namespace TradingDaemon.Tests;

public class DapperContextTests
{
    [Fact]
    public void UsesEnvironmentConnectionStringWhenConfigured()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Database:ActiveEnvironment"] = "Test",
                ["Database:Environments:Test:ConnectionString"] = "Server=.;Database=IntradayTest;User Id=user;Password=pass;"
            })
            .Build();

        var context = new DapperContext(configuration);

        using var connection = context.CreateConnection();
        Assert.Equal(
            "Server=.;Database=IntradayTest;User Id=user;Password=pass;",
            connection.ConnectionString);
    }

    [Fact]
    public void PrefersDefaultConnectionStringWhenProvided()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:DefaultConnection"] = "Server=.;Database=Primary;User Id=prod;Password=prod;",
                ["Database:ActiveEnvironment"] = "Test",
                ["Database:Environments:Test:ConnectionString"] = "Server=.;Database=IntradayTest;User Id=user;Password=pass;"
            })
            .Build();

        var context = new DapperContext(configuration);

        using var connection = context.CreateConnection();
        Assert.Equal(
            "Server=.;Database=Primary;User Id=prod;Password=prod;",
            connection.ConnectionString);
    }

    [Fact]
    public void UsesTopLevelDatabaseConnectionStringWhenNoDefaultConnectionIsConfigured()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Database:ConnectionString"] = "Server=.;Database=TopLevel;User Id=top;Password=top;",
                ["Database:ActiveEnvironment"] = "Test"
            })
            .Build();

        var context = new DapperContext(configuration);

        using var connection = context.CreateConnection();
        Assert.Equal(
            "Server=.;Database=TopLevel;User Id=top;Password=top;",
            connection.ConnectionString);
    }
}
