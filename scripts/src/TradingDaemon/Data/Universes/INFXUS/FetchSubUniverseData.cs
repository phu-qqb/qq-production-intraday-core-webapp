using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;

/// <summary>
/// Fetches sub-universe ids for a universe and their members from the Intraday.univ tables.
/// Requires environment variable INTRADAY_DB_CONNECTION_STRING with a SQL Server connection string.
/// Usage: dotnet run --project <path> -- <UniverseId>
/// Writes E.csv (sub-universe ids) and F.csv (sub-universe-member pairs) in the current directory.
/// </summary>
public static class FetchSubUniverseData
{
    public static void Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.Error.WriteLine("UniverseId argument is required.");
            return;
        }
        var universeId = args[0];
        var connectionString = Environment.GetEnvironmentVariable("INTRADAY_DB_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            Console.Error.WriteLine("Set INTRADAY_DB_CONNECTION_STRING environment variable.");
            return;
        }

        using var connection = new SqlConnection(connectionString);
        connection.Open();

        var subUniverseIds = new List<int>();
        using (var cmd = new SqlCommand("SELECT SubUniverseId FROM Intraday.univ.SubUniverse WHERE UniverseId = @UniverseId", connection))
        {
            cmd.Parameters.AddWithValue("@UniverseId", universeId);
            using var reader = cmd.ExecuteReader();
            using var sw = new StreamWriter("E.csv");
            while (reader.Read())
            {
                var subId = reader.GetInt32(0);
                subUniverseIds.Add(subId);
                sw.WriteLine(subId);
            }
        }

        if (subUniverseIds.Count == 0)
        {
            Console.Error.WriteLine($"No subuniverses found for UniverseId {universeId}.");
            return;
        }

        var inClause = string.Join(",", subUniverseIds);
        var sql = $"SELECT SubUniverseId, SecurityId FROM Intraday.univ.SubUniverseMember WHERE SubUniverseId IN ({inClause})";
        using (var cmd = new SqlCommand(sql, connection))
        using (var reader = cmd.ExecuteReader())
        using (var sw = new StreamWriter("F.csv"))
        {
            while (reader.Read())
            {
                var subId = reader.GetInt32(0);
                var securityId = reader.GetInt32(1);
                sw.WriteLine($"{subId},{securityId}");
            }
        }
    }
}
