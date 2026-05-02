using EF.CH.Infrastructure;

namespace EF.CH.BulkInsert;

/// <summary>
/// Options for configuring bulk insert operations.
/// </summary>
public class ClickHouseBulkInsertOptions
{
    private bool _useAsyncInsert;
    private bool _waitForAsyncInsert;

    /// <summary>
    /// Gets or sets the number of entities to insert in each batch.
    /// Default is 10,000.
    /// </summary>
    public int BatchSize { get; set; } = 10_000;

    /// <summary>
    /// Gets or sets the format to use for generating the INSERT statement.
    /// Default is <see cref="ClickHouseBulkInsertFormat.Values"/>.
    /// </summary>
    public ClickHouseBulkInsertFormat Format { get; set; } = ClickHouseBulkInsertFormat.Values;

    /// <summary>
    /// Gets whether ClickHouse async_insert mode is enabled. Toggle via
    /// <see cref="WithAsyncInsert"/> / <see cref="WaitForCompletion"/>.
    /// </summary>
    internal bool UseAsyncInsert => _useAsyncInsert;

    /// <summary>
    /// Gets whether the client waits for the async insert to flush. Toggle via
    /// <see cref="WaitForCompletion"/>. Always implies async insert.
    /// </summary>
    internal bool WaitForAsyncInsert => _waitForAsyncInsert;

    /// <summary>
    /// Gets or sets the maximum degree of parallelism for batch insertion.
    /// When greater than 1, batches are inserted in parallel using separate connections.
    /// Default is 1 (sequential insertion).
    /// </summary>
    public int MaxDegreeOfParallelism { get; set; } = 1;

    /// <summary>
    /// Gets or sets the ClickHouse max_insert_threads setting.
    /// Controls the number of threads ClickHouse uses for parsing and inserting data.
    /// </summary>
    public int? MaxInsertThreads { get; set; }

    /// <summary>
    /// Gets additional ClickHouse settings to append to the INSERT statement.
    /// </summary>
    public Dictionary<string, object> Settings { get; } = new();

    /// <summary>
    /// Gets or sets the command timeout for the insert operation.
    /// </summary>
    public TimeSpan? Timeout { get; set; }

    /// <summary>
    /// Gets or sets a callback that is invoked after each batch is inserted.
    /// The callback receives the cumulative number of rows inserted so far.
    /// </summary>
    public Action<long>? OnBatchCompleted { get; set; }

    /// <summary>
    /// Optional sink for exceptions thrown by <see cref="OnBatchCompleted"/>.
    /// Defaults to <c>null</c> — the bulk inserter swallows callback exceptions
    /// to avoid aborting the operation (a buggy user callback shouldn't kill an
    /// in-flight bulk insert), but with no <see cref="OnCallbackException"/>
    /// sink the exception is silently lost. Provide a sink (e.g. a logger
    /// adapter) to surface them. The sink is itself called inside a
    /// try/catch — its own exceptions are also swallowed.
    /// </summary>
    public Action<Exception>? OnCallbackException { get; set; }

    /// <summary>
    /// Sets the batch size.
    /// </summary>
    public ClickHouseBulkInsertOptions WithBatchSize(int batchSize)
    {
        BatchSize = batchSize;
        return this;
    }

    /// <summary>
    /// Sets the insert format.
    /// </summary>
    public ClickHouseBulkInsertOptions WithFormat(ClickHouseBulkInsertFormat format)
    {
        Format = format;
        return this;
    }

    /// <summary>
    /// Enables ClickHouse async insert mode (<c>async_insert = 1</c>). The server
    /// buffers and flushes inserts asynchronously for higher throughput. Pair with
    /// <see cref="WaitForCompletion"/> if you need the call to block until the
    /// buffer is flushed (<c>wait_for_async_insert = 1</c>).
    /// </summary>
    public ClickHouseBulkInsertOptions WithAsyncInsert()
    {
        _useAsyncInsert = true;
        return this;
    }

    /// <summary>
    /// Waits for the async insert buffer to flush before returning
    /// (<c>wait_for_async_insert = 1</c>). Implies <see cref="WithAsyncInsert"/> —
    /// calling this alone is sufficient to enable both flags.
    /// </summary>
    public ClickHouseBulkInsertOptions WaitForCompletion()
    {
        _useAsyncInsert = true;
        _waitForAsyncInsert = true;
        return this;
    }

    /// <summary>
    /// Sets the maximum degree of parallelism for parallel batch insertion.
    /// </summary>
    public ClickHouseBulkInsertOptions WithParallelism(int maxDegreeOfParallelism)
    {
        MaxDegreeOfParallelism = maxDegreeOfParallelism;
        return this;
    }

    /// <summary>
    /// Sets the max_insert_threads ClickHouse setting.
    /// </summary>
    public ClickHouseBulkInsertOptions WithMaxInsertThreads(int threads)
    {
        MaxInsertThreads = threads;
        return this;
    }

    /// <summary>
    /// Adds a ClickHouse setting to the INSERT statement.
    /// </summary>
    public ClickHouseBulkInsertOptions WithSetting(string name, object value)
    {
        Settings[name] = value;
        return this;
    }

    /// <summary>
    /// Adds a strongly-typed ClickHouse setting to the INSERT statement.
    /// </summary>
    /// <remarks>
    /// Forwards into the same dictionary used by
    /// <see cref="WithSetting(string, object)"/>, so the rendered SETTINGS
    /// clause is identical to the raw form.
    /// </remarks>
    public ClickHouseBulkInsertOptions WithSetting<TValue>(Setting<TValue> setting, TValue value)
    {
        ArgumentNullException.ThrowIfNull(setting);
        Settings[setting.Name] = value!;
        return this;
    }

    /// <summary>
    /// Adds multiple ClickHouse settings to the INSERT statement.
    /// </summary>
    public ClickHouseBulkInsertOptions WithSettings(IDictionary<string, object> settings)
    {
        foreach (var kvp in settings)
        {
            Settings[kvp.Key] = kvp.Value;
        }
        return this;
    }

    /// <summary>
    /// Sets the command timeout.
    /// </summary>
    public ClickHouseBulkInsertOptions WithTimeout(TimeSpan timeout)
    {
        Timeout = timeout;
        return this;
    }

    /// <summary>
    /// Sets a callback to be invoked after each batch is inserted.
    /// </summary>
    public ClickHouseBulkInsertOptions WithProgressCallback(Action<long> callback)
    {
        OnBatchCompleted = callback;
        return this;
    }

    /// <summary>
    /// Gets the combined settings dictionary including async_insert and max_insert_threads.
    /// </summary>
    internal Dictionary<string, object> GetEffectiveSettings()
    {
        var result = new Dictionary<string, object>(Settings);

        if (_useAsyncInsert)
        {
            result["async_insert"] = 1;
            if (_waitForAsyncInsert)
            {
                result["wait_for_async_insert"] = 1;
            }
        }

        if (MaxInsertThreads.HasValue)
        {
            result["max_insert_threads"] = MaxInsertThreads.Value;
        }

        return result;
    }
}
