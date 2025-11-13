using System;
using System.Data;
using Amazon;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using Microsoft.Data.SqlClient;
using System.Text.Json;
using System.Globalization;

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

        var secretName = configuration["Database:SecretName"] ?? "qq-intraday-credentials";
        var region = configuration["AWS:Region"] ?? Environment.GetEnvironmentVariable("AWS_REGION") ?? "eu-west-2";

        using var client = new AmazonSecretsManagerClient(RegionEndpoint.GetBySystemName(region));
        var request = new GetSecretValueRequest { SecretId = secretName };
        var response = client.GetSecretValueAsync(request).GetAwaiter().GetResult();
        var secretJson = response.SecretString ?? throw new InvalidOperationException($"Secret '{secretName}' is empty.");
        var doc = JsonDocument.Parse(secretJson).RootElement;

        var host = doc.GetProperty("host").GetString();
        var username = doc.GetProperty("username").GetString();
        var password = doc.GetProperty("password").GetString();
        var dbname = doc.TryGetProperty("database", out var dbEl) ? dbEl.GetString() : string.Empty;
        var port = doc.TryGetProperty("port", out var portEl)
            ? int.Parse(portEl.GetString() ?? "1433", CultureInfo.InvariantCulture)
            : 1433;

        if (string.IsNullOrWhiteSpace(host))
        {
            throw new InvalidOperationException($"Secret '{secretName}' does not contain a valid host value.");
        }

        if (string.IsNullOrWhiteSpace(username))
        {
            throw new InvalidOperationException($"Secret '{secretName}' does not contain a valid username value.");
        }

        if (password is null)
        {
            throw new InvalidOperationException($"Secret '{secretName}' does not contain a password value.");
        }

        var builder = new SqlConnectionStringBuilder
        {
            DataSource = $"{host},{port}",
            InitialCatalog = dbname ?? string.Empty,
            UserID = username,
            Password = password,
            Encrypt = true,
            TrustServerCertificate = true
        };

        _connectionString = builder.ConnectionString;
    }

    public virtual IDbConnection CreateConnection()
        => new SqlConnection(_connectionString);
}
