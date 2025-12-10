namespace EF.CH.Dictionaries;

/// <summary>
/// Represents ClickHouse dictionary layout types.
/// The layout determines how the dictionary data is stored in memory.
/// </summary>
public abstract class DictionaryLayout
{
    /// <summary>
    /// Generates the LAYOUT(...) SQL clause.
    /// </summary>
    public abstract string ToSql();

    /// <summary>
    /// Creates a FLAT layout - stores data in contiguous array.
    /// Best for small dictionaries with sequential numeric keys starting from 0.
    /// </summary>
    /// <param name="initialArraySize">Initial size of the array.</param>
    /// <param name="maxArraySize">Maximum size of the array.</param>
    public static DictionaryLayout Flat(ulong? initialArraySize = null, ulong? maxArraySize = null)
        => new FlatLayout(initialArraySize, maxArraySize);

    /// <summary>
    /// Creates a HASHED layout - stores data in a hash table.
    /// General purpose layout suitable for most use cases.
    /// </summary>
    /// <param name="initialSize">Initial size hint for the hash table.</param>
    public static DictionaryLayout Hashed(ulong? initialSize = null)
        => new HashedLayout(initialSize);

    /// <summary>
    /// Creates a SPARSE_HASHED layout - memory-optimized hash table.
    /// Best for large dictionaries with sparse keys.
    /// </summary>
    public static DictionaryLayout SparseHashed()
        => new SparseHashedLayout();

    /// <summary>
    /// Creates a CACHE layout - LRU cache that loads data on demand.
    /// Best when only a subset of dictionary data is frequently accessed.
    /// </summary>
    /// <param name="sizeInCells">Maximum number of cells (entries) in the cache.</param>
    public static DictionaryLayout Cache(ulong sizeInCells)
        => new CacheLayout(sizeInCells);

    /// <summary>
    /// Creates a COMPLEX_KEY_HASHED layout - hash table for composite keys.
    /// Required when the dictionary has multiple key columns.
    /// </summary>
    /// <param name="initialSize">Initial size hint for the hash table.</param>
    public static DictionaryLayout ComplexKeyHashed(ulong? initialSize = null)
        => new ComplexKeyHashedLayout(initialSize);

    /// <summary>
    /// Creates a COMPLEX_KEY_CACHE layout - LRU cache for composite keys.
    /// </summary>
    /// <param name="sizeInCells">Maximum number of cells (entries) in the cache.</param>
    public static DictionaryLayout ComplexKeyCache(ulong sizeInCells)
        => new ComplexKeyCacheLayout(sizeInCells);

    /// <summary>
    /// Creates a RANGE_HASHED layout - hash table with date/time range support.
    /// Best for time-series lookups like price history.
    /// </summary>
    public static DictionaryLayout RangeHashed()
        => new RangeHashedLayout();

    /// <summary>
    /// Creates a DIRECT layout - no caching, queries source directly.
    /// Best when data changes frequently and freshness is critical.
    /// </summary>
    public static DictionaryLayout Direct()
        => new DirectLayout();

    /// <summary>
    /// Creates an IP_TRIE layout - optimized for IP address prefix lookups.
    /// </summary>
    public static DictionaryLayout IpTrie()
        => new IpTrieLayout();
}

internal sealed class FlatLayout(ulong? initialArraySize, ulong? maxArraySize) : DictionaryLayout
{
    public override string ToSql()
    {
        var args = new List<string>();
        if (initialArraySize.HasValue)
            args.Add($"INITIAL_ARRAY_SIZE {initialArraySize.Value}");
        if (maxArraySize.HasValue)
            args.Add($"MAX_ARRAY_SIZE {maxArraySize.Value}");

        return args.Count > 0 ? $"FLAT({string.Join(" ", args)})" : "FLAT()";
    }
}

internal sealed class HashedLayout(ulong? initialSize) : DictionaryLayout
{
    public override string ToSql()
        => initialSize.HasValue ? $"HASHED(INITIAL_SIZE {initialSize.Value})" : "HASHED()";
}

internal sealed class SparseHashedLayout : DictionaryLayout
{
    public override string ToSql() => "SPARSE_HASHED()";
}

internal sealed class CacheLayout(ulong sizeInCells) : DictionaryLayout
{
    public override string ToSql() => $"CACHE(SIZE_IN_CELLS {sizeInCells})";
}

internal sealed class ComplexKeyHashedLayout(ulong? initialSize) : DictionaryLayout
{
    public override string ToSql()
        => initialSize.HasValue ? $"COMPLEX_KEY_HASHED(INITIAL_SIZE {initialSize.Value})" : "COMPLEX_KEY_HASHED()";
}

internal sealed class ComplexKeyCacheLayout(ulong sizeInCells) : DictionaryLayout
{
    public override string ToSql() => $"COMPLEX_KEY_CACHE(SIZE_IN_CELLS {sizeInCells})";
}

internal sealed class RangeHashedLayout : DictionaryLayout
{
    public override string ToSql() => "RANGE_HASHED()";
}

internal sealed class DirectLayout : DictionaryLayout
{
    public override string ToSql() => "DIRECT()";
}

internal sealed class IpTrieLayout : DictionaryLayout
{
    public override string ToSql() => "IP_TRIE()";
}
