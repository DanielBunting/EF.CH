namespace EF.CH.Dictionaries;

/// <summary>
/// Represents ClickHouse dictionary source configurations.
/// The source defines where dictionary data comes from.
/// </summary>
public abstract class DictionarySource
{
    /// <summary>
    /// Generates the SOURCE(...) SQL clause.
    /// </summary>
    public abstract string ToSql();
}

/// <summary>
/// ClickHouse table source - loads dictionary data from a ClickHouse table.
/// </summary>
public sealed class ClickHouseTableSource : DictionarySource
{
    /// <summary>
    /// The source table name (required).
    /// </summary>
    public required string Table { get; init; }

    /// <summary>
    /// The database containing the source table.
    /// </summary>
    public string? Database { get; init; }

    /// <summary>
    /// Remote ClickHouse host (for distributed setups).
    /// </summary>
    public string? Host { get; init; }

    /// <summary>
    /// Remote ClickHouse port.
    /// </summary>
    public int? Port { get; init; }

    /// <summary>
    /// Username for remote connection.
    /// </summary>
    public string? User { get; init; }

    /// <summary>
    /// Password for remote connection.
    /// </summary>
    public string? Password { get; init; }

    /// <summary>
    /// WHERE clause to filter source data.
    /// </summary>
    public string? Where { get; init; }

    /// <summary>
    /// Column to check for updates (enables incremental updates).
    /// </summary>
    public string? UpdateField { get; init; }

    /// <summary>
    /// Query to check if dictionary needs invalidation.
    /// </summary>
    public string? InvalidateQuery { get; init; }

    public override string ToSql()
    {
        var parts = new List<string>();

        if (Host != null)
            parts.Add($"HOST '{Host}'");
        if (Port.HasValue)
            parts.Add($"PORT {Port.Value}");
        if (User != null)
            parts.Add($"USER '{User}'");
        if (Password != null)
            parts.Add($"PASSWORD '{Password}'");
        if (Database != null)
            parts.Add($"DB '{Database}'");

        parts.Add($"TABLE '{Table}'");

        if (Where != null)
            parts.Add($"WHERE {Where}");
        if (UpdateField != null)
            parts.Add($"UPDATE_FIELD {UpdateField}");
        if (InvalidateQuery != null)
            parts.Add($"INVALIDATE_QUERY '{InvalidateQuery}'");

        return $"CLICKHOUSE({string.Join(" ", parts)})";
    }
}

/// <summary>
/// HTTP source - loads dictionary data from an HTTP endpoint.
/// </summary>
public sealed class HttpDictionarySource : DictionarySource
{
    /// <summary>
    /// The HTTP URL to fetch data from (required).
    /// </summary>
    public required string Url { get; init; }

    /// <summary>
    /// The data format returned by the endpoint.
    /// </summary>
    public HttpDictionaryFormat Format { get; init; } = HttpDictionaryFormat.JSONEachRow;

    /// <summary>
    /// Optional HTTP headers to include in the request.
    /// </summary>
    public IDictionary<string, string>? Headers { get; init; }

    /// <summary>
    /// HTTP request credentials (user:password for basic auth).
    /// </summary>
    public string? Credentials { get; init; }

    public override string ToSql()
    {
        var parts = new List<string>
        {
            $"URL '{Url}'",
            $"FORMAT {Format}"
        };

        if (Credentials != null)
            parts.Add($"CREDENTIALS '{Credentials}'");

        if (Headers != null)
        {
            foreach (var (key, value) in Headers)
            {
                parts.Add($"HEADER '{key}' = '{value}'");
            }
        }

        return $"HTTP({string.Join(" ", parts)})";
    }
}

/// <summary>
/// Data formats supported by HTTP dictionary source.
/// </summary>
public enum HttpDictionaryFormat
{
    /// <summary>
    /// JSON with one object per line.
    /// </summary>
    JSONEachRow,

    /// <summary>
    /// Comma-separated values.
    /// </summary>
    CSV,

    /// <summary>
    /// Tab-separated values.
    /// </summary>
    TabSeparated
}

/// <summary>
/// File source - loads dictionary data from a local file.
/// </summary>
public sealed class FileDictionarySource : DictionarySource
{
    /// <summary>
    /// The file path (required).
    /// </summary>
    public required string Path { get; init; }

    /// <summary>
    /// The file format.
    /// </summary>
    public FileDictionaryFormat Format { get; init; } = FileDictionaryFormat.TabSeparated;

    public override string ToSql()
        => $"FILE(PATH '{Path}' FORMAT {Format})";
}

/// <summary>
/// Data formats supported by file dictionary source.
/// </summary>
public enum FileDictionaryFormat
{
    /// <summary>
    /// Tab-separated values.
    /// </summary>
    TabSeparated,

    /// <summary>
    /// Comma-separated values.
    /// </summary>
    CSV,

    /// <summary>
    /// JSON with one object per line.
    /// </summary>
    JSONEachRow
}

/// <summary>
/// Executable source - loads dictionary data by running an executable.
/// </summary>
public sealed class ExecutableDictionarySource : DictionarySource
{
    /// <summary>
    /// The command to execute (required).
    /// </summary>
    public required string Command { get; init; }

    /// <summary>
    /// The output format of the executable.
    /// </summary>
    public FileDictionaryFormat Format { get; init; } = FileDictionaryFormat.TabSeparated;

    /// <summary>
    /// Whether the executable runs as a pool of processes.
    /// </summary>
    public bool Pool { get; init; }

    public override string ToSql()
    {
        var source = Pool ? "EXECUTABLE_POOL" : "EXECUTABLE";
        return $"{source}(COMMAND '{Command}' FORMAT {Format})";
    }
}
