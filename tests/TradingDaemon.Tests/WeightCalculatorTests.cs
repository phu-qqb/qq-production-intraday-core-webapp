using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using Xunit;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using TradingDaemon.Data;
using TradingDaemon.Services;

public class WeightCalculatorTests
{
    [Fact(Skip = "Requires GPU executable")]
    public async Task CalculateAndStoreAsync_ParsesOutput()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Executables:GenBinariesExecutable"] = "bash",
            ["Executables:PythonExecutable"] = "python3",
            ["Programmes:0:Universe"] = "INFXUS",
            ["Programmes:0:Session"] = "EUUS",
            ["Programmes:0:Timeframe"] = "60",
            ["Programmes:0:StartDate"] = "2022-01-01T00:00:00Z"
        }).Build();
        var context = new DapperContext(config);
        var logger = Mock.Of<ILogger<WeightCalculator>>();
        var calc = new WeightCalculator(context, config, logger);
        await calc.CalculateAndStoreAsync();
    }

    [Fact]
    public void ZeroPenultimateRows_ZeroesExpectedPenultimateBar()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ConnectionStrings:DefaultConnection"] = "Server=.;Database=Dummy;User Id=dummy;Password=dummy;"
        }).Build();
        var context = new DapperContext(config);
        var logger = Mock.Of<ILogger<WeightCalculator>>();
        var calc = new WeightCalculator(context, config, logger);

        var weightRowType = typeof(WeightCalculator).GetNestedType("WeightRow", BindingFlags.NonPublic);
        Assert.NotNull(weightRowType);

        var rows = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(weightRowType!))!;
        var barTimeProperty = weightRowType!.GetProperty("BarTimeUtc", BindingFlags.Public | BindingFlags.Instance);
        var weightsProperty = weightRowType.GetProperty("Weights", BindingFlags.Public | BindingFlags.Instance);
        Assert.NotNull(barTimeProperty);
        Assert.NotNull(weightsProperty);

        AddRow(rows, weightRowType, new DateTime(2024, 5, 1, 17, 6, 0, DateTimeKind.Utc), 1m);
        AddRow(rows, weightRowType, new DateTime(2024, 5, 1, 18, 6, 0, DateTimeKind.Utc), 1m);
        AddRow(rows, weightRowType, new DateTime(2024, 5, 1, 19, 6, 0, DateTimeKind.Utc), 1m);

        var method = typeof(WeightCalculator).GetMethod(
            "ZeroPenultimateRows",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        method!.Invoke(calc, new object?[] { rows, "US", 60, 6 });

        foreach (var row in rows)
        {
            var barTime = (DateTime)barTimeProperty!.GetValue(row)!;
            var weights = (decimal?[])weightsProperty!.GetValue(row)!;

            if (barTime == new DateTime(2024, 5, 1, 18, 6, 0, DateTimeKind.Utc))
            {
                Assert.All(weights, value => Assert.Equal(0m, value));
            }
            else
            {
                Assert.All(weights, value => Assert.Equal(1m, value));
            }
        }
    }

    [Fact]
    public void ZeroPenultimateRows_ZeroesPenultimateEvenWhenLastRowAvailable()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ConnectionStrings:DefaultConnection"] = "Server=.;Database=Dummy;User Id=dummy;Password=dummy;"
        }).Build();
        var context = new DapperContext(config);
        var logger = Mock.Of<ILogger<WeightCalculator>>();
        var calc = new WeightCalculator(context, config, logger);

        var weightRowType = typeof(WeightCalculator).GetNestedType("WeightRow", BindingFlags.NonPublic);
        Assert.NotNull(weightRowType);

        var rows = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(weightRowType!))!;
        var barTimeProperty = weightRowType!.GetProperty("BarTimeUtc", BindingFlags.Public | BindingFlags.Instance);
        var weightsProperty = weightRowType.GetProperty("Weights", BindingFlags.Public | BindingFlags.Instance);
        Assert.NotNull(barTimeProperty);
        Assert.NotNull(weightsProperty);

        AddRow(rows, weightRowType, new DateTime(2024, 5, 1, 18, 6, 0, DateTimeKind.Utc), 1m);

        var method = typeof(WeightCalculator).GetMethod(
            "ZeroPenultimateRows",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        method!.Invoke(calc, new object?[] { rows, "US", 60, 6 });

        foreach (var row in rows)
        {
            var barTime = (DateTime)barTimeProperty!.GetValue(row)!;
            var weights = (decimal?[])weightsProperty!.GetValue(row)!;

            if (barTime == new DateTime(2024, 5, 1, 18, 6, 0, DateTimeKind.Utc))
            {
                Assert.All(weights, value => Assert.Equal(0m, value));
            }
        }
    }

    [Fact]
    public void ZeroPenultimateRows_DoesNotFallbackToSecondLastRow()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ConnectionStrings:DefaultConnection"] = "Server=.;Database=Dummy;User Id=dummy;Password=dummy;"
        }).Build();
        var context = new DapperContext(config);
        var logger = Mock.Of<ILogger<WeightCalculator>>();
        var calc = new WeightCalculator(context, config, logger);

        var weightRowType = typeof(WeightCalculator).GetNestedType("WeightRow", BindingFlags.NonPublic);
        Assert.NotNull(weightRowType);

        var rows = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(weightRowType!))!;
        var barTimeProperty = weightRowType!.GetProperty("BarTimeUtc", BindingFlags.Public | BindingFlags.Instance);
        var weightsProperty = weightRowType.GetProperty("Weights", BindingFlags.Public | BindingFlags.Instance);
        Assert.NotNull(barTimeProperty);
        Assert.NotNull(weightsProperty);

        AddRow(rows, weightRowType, new DateTime(2024, 5, 1, 17, 6, 0, DateTimeKind.Utc), 1m);
        AddRow(rows, weightRowType, new DateTime(2024, 5, 1, 19, 6, 0, DateTimeKind.Utc), 1m);

        var method = typeof(WeightCalculator).GetMethod(
            "ZeroPenultimateRows",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        method!.Invoke(calc, new object?[] { rows, "US", 60, 6 });

        foreach (var row in rows)
        {
            var barTime = (DateTime)barTimeProperty!.GetValue(row)!;
            var weights = (decimal?[])weightsProperty!.GetValue(row)!;

            Assert.All(weights, value => Assert.Equal(1m, value));
        }
    }

    [Fact]
    public void ResolveHomePath_DefaultsToTestRootWhenEnvironmentIsTest()
    {
        var method = typeof(WeightCalculator).GetMethod(
            "ResolveHomePath",
            BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var originalHomeRoot = Environment.GetEnvironmentVariable("HOME_ROOT");
        var originalAspNetEnv = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
        var originalDotnetEnv = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT");

        try
        {
            Environment.SetEnvironmentVariable("HOME_ROOT", null);
            Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "Test");
            Environment.SetEnvironmentVariable("DOTNET_ENVIRONMENT", null);

            var result = (string?)method!.Invoke(null, new object?[] { "/home/data/file.txt" });

            Assert.Equal("/home_test/data/file.txt", result);
        }
        finally
        {
            Environment.SetEnvironmentVariable("HOME_ROOT", originalHomeRoot);
            Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", originalAspNetEnv);
            Environment.SetEnvironmentVariable("DOTNET_ENVIRONMENT", originalDotnetEnv);
        }
    }

    private static void AddRow(IList rows, Type weightRowType, DateTime barTimeUtc, decimal weight)
    {
        var ctor = weightRowType.GetConstructor(new[] { typeof(DateTime), typeof(decimal?[]) });
        Assert.NotNull(ctor);

        var weightArray = new decimal?[] { weight };
        var row = ctor!.Invoke(new object?[] { barTimeUtc, weightArray });
        rows.Add(row);
    }
}
