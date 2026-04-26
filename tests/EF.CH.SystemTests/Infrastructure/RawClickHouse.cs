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

    /// <summary>
    /// Reads one row from <c>system.view_refreshes</c> for the given refreshable MV.
    /// Returns null if no row exists yet.
    /// </summary>
    public static async Task<IReadOnlyDictionary<string, object?>?> ViewRefreshAsync(string connectionString, string view)
    {
        var rows = await RowsAsync(connectionString,
            $"SELECT * FROM system.view_refreshes WHERE view = '{Esc(view)}' LIMIT 1");
        return rows.Count == 0 ? null : rows[0];
    }

    /// <summary>
    /// Polls <c>system.view_refreshes.last_success_time</c> until it advances past
    /// <paramref name="mustExceed"/> or the timeout expires.
    /// </summary>
    public static async Task WaitForViewRefreshAsync(string connectionString, string view,
        DateTime mustExceed, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        while (DateTime.UtcNow < deadline)
        {
            var rows = await RowsAsync(connectionString,
                $"SELECT last_success_time FROM system.view_refreshes WHERE view = '{Esc(view)}' LIMIT 1");
            if (rows.Count > 0 && rows[0]["last_success_time"] is { } v && v is not DBNull)
            {
                var t = Convert.ToDateTime(v);
                if (t > mustExceed) return;
            }
            await Task.Delay(250);
        }
        throw new TimeoutException($"Refreshable view '{view}' did not refresh after {mustExceed:O} within the timeout.");
    }

    /// <summary>
    /// Returns the rendered ClickHouse type for a column from <c>system.columns</c> (e.g. "LowCardinality(String)",
    /// "Decimal(18, 4)", "Map(String, Int32)"). Useful for asserting that the EF type-mapping produced the right CH type.
    /// </summary>
    public static Task<string> ColumnTypeAsync(string connectionString, string table, string column)
        => ScalarAsync<string>(connectionString,
            $"SELECT type FROM system.columns WHERE database = currentDatabase() AND table = '{Esc(table)}' AND name = '{Esc(column)}'");

    /// <summary>
    /// Returns rows from <c>system.data_skipping_indices</c> for the given table. Each row carries
    /// <c>name</c>, <c>type</c>, <c>expr</c>, <c>granularity</c> — the columns relevant to skip-index assertions.
    /// </summary>
    public static Task<IReadOnlyList<IReadOnlyDictionary<string, object?>>> SkipIndicesAsync(
        string connectionString, string table)
        => RowsAsync(connectionString,
            $"SELECT name, type, expr, granularity FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = '{Esc(table)}'");

    /// <summary>
    /// Issues a query with a <c>SETTINGS</c> clause appended. Used for surfaces that require non-default
    /// ClickHouse settings (e.g. <c>allow_experimental_json_type=1</c>, <c>allow_suspicious_low_cardinality_types=1</c>).
    /// </summary>
    public static async Task ExecuteWithSettingsAsync(
        string connectionString, string sql, IReadOnlyDictionary<string, object> settings)
    {
        var clause = string.Join(", ", settings.Select(kv => $"{kv.Key} = {FormatSettingValue(kv.Value)}"));
        await ExecuteAsync(connectionString, $"{sql} SETTINGS {clause}");
    }

    private static string FormatSettingValue(object value) => value switch
    {
        bool b => b ? "1" : "0",
        string s => $"'{Esc(s)}'",
        _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty,
    };

    public static string Esc(string raw) => raw.Replace("'", "''");
}
