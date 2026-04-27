namespace EF.CH.Infrastructure;

/// <summary>
/// Strongly-typed handle for a ClickHouse server/query setting.
/// </summary>
/// <remarks>
/// <para>
/// Wrapping a setting name in <see cref="Setting{T}"/> lets the value type be
/// checked at the call site (e.g. <c>Setting&lt;int&gt;</c> rejects a string
/// argument at compile time). The typed entries in
/// <see cref="ClickHouseSettings"/> route through the same string/object
/// pipeline as the raw <c>WithSetting(string, object)</c> overload — there
/// is no parallel formatting path.
/// </para>
/// <para>
/// To add a typed setting that is not in the catalogue, use
/// <see cref="Setting.Of{T}(string)"/>.
/// </para>
/// </remarks>
/// <typeparam name="T">The runtime value type accepted by this setting.</typeparam>
public sealed class Setting<T>
{
    /// <summary>
    /// Gets the underlying ClickHouse setting name (e.g. <c>"max_insert_threads"</c>).
    /// </summary>
    public string Name { get; }

    internal Setting(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        Name = name;
    }
}

/// <summary>
/// Factory helpers for constructing <see cref="Setting{T}"/> handles outside
/// the <see cref="ClickHouseSettings"/> catalogue.
/// </summary>
public static class Setting
{
    /// <summary>
    /// Constructs a typed setting handle for a ClickHouse setting name not in
    /// the canned <see cref="ClickHouseSettings"/> catalogue.
    /// </summary>
    public static Setting<T> Of<T>(string name) => new(name);
}

/// <summary>
/// Catalogue of commonly-used ClickHouse settings as strongly-typed
/// <see cref="Setting{T}"/> handles. Pass these to
/// <c>WithSetting(Setting&lt;T&gt;, T)</c> overloads on
/// <see cref="EF.CH.BulkInsert.ClickHouseBulkInsertOptions"/> and on the
/// per-query <c>WithSetting</c> extension method.
/// </summary>
/// <remarks>
/// The raw <c>WithSetting(string, object)</c> overload remains available for
/// settings not in this catalogue (or for forward compatibility with new
/// ClickHouse releases). Both overloads funnel into the same pipeline, so
/// generated SQL is byte-identical for equivalent calls.
/// </remarks>
public static class ClickHouseSettings
{
    // -------- Bulk insert --------

    /// <summary>Number of threads used for parsing and inserting data.</summary>
    public static readonly Setting<int> MaxInsertThreads = new("max_insert_threads");

    /// <summary>Enables async insert mode — inserts are buffered server-side.</summary>
    public static readonly Setting<bool> AsyncInsert = new("async_insert");

    /// <summary>Wait for an async insert to complete before returning.</summary>
    public static readonly Setting<bool> WaitForAsyncInsert = new("wait_for_async_insert");

    /// <summary>Maximum data size (bytes) for an async insert buffer flush.</summary>
    public static readonly Setting<long> AsyncInsertMaxDataSize = new("async_insert_max_data_size");

    /// <summary>Required quorum for replicated inserts.</summary>
    public static readonly Setting<int> InsertQuorum = new("insert_quorum");

    /// <summary>Whether to deduplicate identical insert blocks (replicated tables).</summary>
    public static readonly Setting<bool> InsertDeduplicate = new("insert_deduplicate");

    // -------- Read-side --------

    /// <summary>Optimise reads in ORDER BY key order to reduce sort work.</summary>
    public static readonly Setting<bool> OptimizeReadInOrder = new("optimize_read_in_order");

    /// <summary>Enable the experimental new query analyzer.</summary>
    public static readonly Setting<bool> AllowExperimentalAnalyzer = new("allow_experimental_analyzer");

    /// <summary>Maximum number of threads used for query execution.</summary>
    public static readonly Setting<int> MaxThreads = new("max_threads");

    /// <summary>Maximum block size (rows) for reading.</summary>
    public static readonly Setting<long> MaxBlockSize = new("max_block_size");

    /// <summary>Maximum number of rows that may be read by a single query.</summary>
    public static readonly Setting<long> MaxRowsToRead = new("max_rows_to_read");

    /// <summary>Maximum query execution time in seconds before it is cancelled.</summary>
    public static readonly Setting<int> MaxExecutionTime = new("max_execution_time");

    /// <summary>Enables HTTP compression on the wire.</summary>
    public static readonly Setting<bool> EnableHttpCompression = new("enable_http_compression");

    /// <summary>Treat unmatched join rows as NULL instead of default values.</summary>
    public static readonly Setting<bool> JoinUseNulls = new("join_use_nulls");

    /// <summary>Enable querying a full-text (text) index on supported columns.</summary>
    public static readonly Setting<bool> EnableFullTextIndex = new("enable_full_text_index");

    // -------- Mutations --------

    /// <summary>Synchronous mutation execution (ALTER UPDATE/DELETE).</summary>
    public static readonly Setting<bool> MutationsSync = new("mutations_sync");

    /// <summary>Synchronous schema-change execution (ALTER).</summary>
    public static readonly Setting<bool> AlterSync = new("alter_sync");

    // -------- Network / timeouts --------

    /// <summary>Connect timeout in seconds.</summary>
    public static readonly Setting<int> ConnectTimeout = new("connect_timeout");

    /// <summary>Receive timeout in seconds.</summary>
    public static readonly Setting<int> ReceiveTimeout = new("receive_timeout");

    /// <summary>Send timeout in seconds.</summary>
    public static readonly Setting<int> SendTimeout = new("send_timeout");
}
