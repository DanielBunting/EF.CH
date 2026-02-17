namespace EF.CH.BulkInsert;

/// <summary>
/// Options for configuring bulk insert operations.
/// </summary>
public class ClickHouseBulkInsertOptions
{
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
    /// Gets or sets whether to use ClickHouse async_insert mode.
    /// When enabled, inserts are buffered and inserted asynchronously for better throughput.
    /// Default is false.
    /// </summary>
    public bool UseAsyncInsert { get; set; }

    /// <summary>
    /// Gets or sets whether to wait for async insert to complete.
    /// Only applicable when <see cref="UseAsyncInsert"/> is true.
    /// Default is false.
    /// </summary>
    public bool WaitForAsyncInsert { get; set; }

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
    /// Enables async insert mode.
    /// </summary>
    /// <param name="wait">Whether to wait for async insert to complete.</param>
    public ClickHouseBulkInsertOptions WithAsyncInsert(bool wait = false)
    {
        UseAsyncInsert = true;
        WaitForAsyncInsert = wait;
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

        if (UseAsyncInsert)
        {
            result["async_insert"] = 1;
            if (WaitForAsyncInsert)
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
