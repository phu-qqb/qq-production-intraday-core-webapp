using System.Data;
using Amazon;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using Microsoft.Data.SqlClient;
using System.Text.Json;

namespace TradingDaemon.Data;

public class DapperContext
{
    private readonly string _connectionString;

    public DapperContext(IConfiguration configuration)
    {
        var conn = configuration.GetConnectionString("DefaultConnection");
        if (!string.IsNullOrWhiteSpace(conn))
        {
            _connectionString = conn;
            return;
        }

        var secretName = "qq-intraday-credentials";
        var region = configuration["AWS:Region"] ?? Environment.GetEnvironmentVariable("AWS_REGION") ?? "eu-west-2";

        using var client = new AmazonSecretsManagerClient(RegionEndpoint.GetBySystemName(region));
        var request = new GetSecretValueRequest { SecretId = secretName };
        var response = client.GetSecretValueAsync(request).GetAwaiter().GetResult();
        var secretJson = response.SecretString ?? throw new InvalidOperationException("Secret string is empty");
        var doc = JsonDocument.Parse(secretJson).RootElement;
        var host = doc.GetProperty("host").GetString();
        var username = doc.GetProperty("username").GetString();
        var password = doc.GetProperty("password").GetString();
        var dbname = doc.TryGetProperty("database", out var dbEl) ? dbEl.GetString() : string.Empty;
        var port = doc.TryGetProperty("port", out var portEl) ? Int32.Parse(portEl.GetString()) : 1433;

        _connectionString = $"Server={host},{port};Database={dbname};User Id={username};Password={password};Encrypt=True;TrustServerCertificate=True;";
    }

    public virtual IDbConnection CreateConnection()
        => new SqlConnection(_connectionString);
}
