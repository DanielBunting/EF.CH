namespace EF.CH.Services;

/// <summary>
/// Configuration options for the external key-to-ID resolver service.
/// </summary>
public class ExternalKeyIdResolverOptions
{
    /// <summary>
    /// Gets or sets the name of the ClickHouse dictionary used for fast lookups.
    /// </summary>
    public required string DictionaryName { get; init; }

    /// <summary>
    /// Gets or sets the database name for the mapping table.
    /// Defaults to "default".
    /// </summary>
    public string MappingTableDatabase { get; init; } = "default";

    /// <summary>
    /// Gets or sets the name of the local mapping table (ReplacingMergeTree).
    /// </summary>
    public required string MappingTableName { get; init; }

    /// <summary>
    /// Gets or sets the ClickHouse type for the ID column.
    /// Defaults to "UInt32".
    /// </summary>
    public string IdType { get; init; } = "UInt32";

    /// <summary>
    /// Gets or sets whether to enable remote insert when creating new mappings.
    /// When true, new keys are inserted into the external source as well.
    /// Defaults to false (local mapping only).
    /// </summary>
    public bool EnableRemoteInsert { get; init; } = false;

    /// <summary>
    /// Gets or sets the maximum number of retries for dictionary reload.
    /// Defaults to 2.
    /// </summary>
    public int MaxRetries { get; init; } = 2;

    /// <summary>
    /// Gets or sets the delay between retries.
    /// Defaults to 100ms.
    /// </summary>
    public TimeSpan RetryDelay { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Gets or sets the name of the key column in the mapping table.
    /// Defaults to "Key".
    /// </summary>
    public string KeyColumnName { get; init; } = "Key";

    /// <summary>
    /// Gets or sets the name of the ID column in the mapping table.
    /// Defaults to "Id".
    /// </summary>
    public string IdColumnName { get; init; } = "Id";
}
