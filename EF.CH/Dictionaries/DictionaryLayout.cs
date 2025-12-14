namespace EF.CH.Dictionaries;

/// <summary>
/// ClickHouse dictionary layout types that determine how dictionary data is stored in memory.
/// </summary>
public enum DictionaryLayout
{
    /// <summary>
    /// FLAT layout - stores data in a flat array indexed by key.
    /// Best for small dictionaries with sequential integer keys.
    /// Requires UInt64 key type.
    /// </summary>
    Flat,

    /// <summary>
    /// HASHED layout - stores data in a hash table.
    /// Good for larger dictionaries with non-sequential integer keys.
    /// Requires UInt64 key type.
    /// </summary>
    Hashed,

    /// <summary>
    /// HASHED_ARRAY layout - optimized for arrays of values.
    /// Requires UInt64 key type.
    /// </summary>
    HashedArray,

    /// <summary>
    /// COMPLEX_KEY_HASHED layout - hash table for composite keys.
    /// Supports any key types including strings.
    /// </summary>
    ComplexKeyHashed,

    /// <summary>
    /// COMPLEX_KEY_HASHED_ARRAY layout - optimized for composite keys with arrays.
    /// </summary>
    ComplexKeyHashedArray,

    /// <summary>
    /// RANGE_HASHED layout - supports range lookups by date/datetime.
    /// </summary>
    RangeHashed,

    /// <summary>
    /// CACHE layout - LRU cache that loads data on demand.
    /// Good for very large dictionaries where only a subset is accessed.
    /// </summary>
    Cache,

    /// <summary>
    /// DIRECT layout - queries the source directly without caching.
    /// </summary>
    Direct
}

/// <summary>
/// Options for the FLAT dictionary layout.
/// </summary>
public class FlatLayoutOptions
{
    /// <summary>
    /// Maximum size of the array. Default is 500,000.
    /// </summary>
    public ulong? MaxArraySize { get; set; }
}

/// <summary>
/// Options for the HASHED dictionary layout.
/// </summary>
public class HashedLayoutOptions
{
    /// <summary>
    /// Number of shards to use for parallel loading.
    /// </summary>
    public int? ShardCount { get; set; }

    /// <summary>
    /// Use sparse storage to reduce memory usage.
    /// </summary>
    public bool? Sparse { get; set; }
}

/// <summary>
/// Options for the CACHE dictionary layout.
/// </summary>
public class CacheLayoutOptions
{
    /// <summary>
    /// Size of the cache in number of cells. Required for CACHE layout.
    /// </summary>
    public long SizeInCells { get; set; }
}
