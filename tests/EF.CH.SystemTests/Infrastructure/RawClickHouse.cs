using System.Data;
using ClickHouse.Driver.ADO;

namespace EF.CH.SystemTests.Infrastructure;

/// <summary>
/// Thin helper for running hand-rolled SQL directly against a ClickHouse
/// connection via ClickHouse.Driver. System tests use EF's fluent API to
/// drive the model + deploy + inserts, but read assertions through this
/// helper so the verification path is independent of the EF translation
/// layer.
/// </summary>
public static class RawClickHouse
{
    public static async Task<T> ScalarAsync<T>(string connectionString, string sql)
    {
        await using var conn = new ClickHouseConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = await cmd.ExecuteScalarAsync();
        if (result is null || result is DBNull)
            return default!;
        return (T)Convert.ChangeType(result, typeof(T));
    }

    public static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var conn = new ClickHouseConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    public static async Task<IReadOnlyList<IReadOnlyDictionary<string, object?>>> RowsAsync(
        string connectionString, string sql)
    {
        await using var conn = new ClickHouseConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<IReadOnlyDictionary<string, object?>>();
        while (await reader.ReadAsync())
        {
            var row = new Dictionary<string, object?>(reader.FieldCount);
            for (int i = 0; i < reader.FieldCount; i++)
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            rows.Add(row);
        }
        return rows;
    }

    public static async Task<IReadOnlyList<T>> ColumnAsync<T>(
        string connectionString, string sql)
    {
        var rows = await RowsAsync(connectionString, sql);
        return rows.Select(r =>
        {
            var v = r.Values.First();
            return v is null ? default! : (T)Convert.ChangeType(v, typeof(T));
        }).ToArray();
    }

    public static async Task<bool> TableExistsAsync(string connectionString, string table)
    {
        var n = await ScalarAsync<ulong>(connectionString,
            $"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '{Esc(table)}'");
        return n > 0;
    }

    public static Task<string> EngineFullAsync(string connectionString, string table)
        => ScalarAsync<string>(connectionString,
            $"SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = '{Esc(table)}'");

    public static Task<ulong> RowCountAsync(string connectionString, string table, bool final = false)
        => ScalarAsync<ulong>(connectionString,
            $"SELECT count() FROM \"{table}\"{(final ? " FINAL" : "")}");

    public static async Task WaitForReplicationAsync(string connectionString, string table,
        TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(15));
        while (DateTime.UtcNow < deadline)
        {
            var pending = await ScalarAsync<ulong>(connectionString,
                $"SELECT sum(queue_size) + sum(absolute_delay) FROM system.replicas WHERE database = currentDatabase() AND table = '{Esc(table)}'");
            if (pending == 0)
                return;
            await Task.Delay(200);
        }
        throw new TimeoutException($"Replication of '{table}' did not settle within the timeout.");
    }

    public static async Task WaitForMutationsAsync(string connectionString, string table,
        TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(15));
        while (DateTime.UtcNow < deadline)
        {
            var pending = await ScalarAsync<ulong>(connectionString,
                $"SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = '{Esc(table)}' AND is_done = 0");
            if (pending == 0)
                return;
            await Task.Delay(200);
        }
        throw new TimeoutException($"Mutations on '{table}' did not complete within the timeout.");
    }

    public static async Task SettleMaterializationAsync(string connectionString, string mvTargetTable,
        TimeSpan? timeout = null)
    {
        await ExecuteAsync(connectionString, $"OPTIMIZE TABLE \"{mvTargetTable}\" FINAL");
        await WaitForMutationsAsync(connectionString, mvTargetTable, timeout);
    }

    public static string Esc(string raw) => raw.Replace("'", "''");
}
