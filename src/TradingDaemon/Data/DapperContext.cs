using System.Data;
using Npgsql;

namespace TradingDaemon.Data;

public class DapperContext
{
    private readonly string _connectionString;

    public DapperContext(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException("Connection string not found");
    }

    public virtual IDbConnection CreateConnection()
        => new NpgsqlConnection(_connectionString);
}
