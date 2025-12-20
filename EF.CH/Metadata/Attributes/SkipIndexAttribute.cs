namespace EF.CH.Metadata.Attributes;

/// <summary>
/// Base class for skip index attributes.
/// Apply to a property to create a ClickHouse data skipping index on that column.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
public abstract class SkipIndexAttribute : Attribute
{
    /// <summary>
    /// Gets or sets the granularity for the skip index.
    /// Default is 3. Valid range: 1-1000.
    /// </summary>
    public int Granularity { get; set; } = 3;

    /// <summary>
    /// Gets or sets the index name. If not specified, a name will be auto-generated.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// Gets the skip index type.
    /// </summary>
    public abstract SkipIndexType IndexType { get; }

    /// <summary>
    /// Gets the index parameters.
    /// </summary>
    public abstract SkipIndexParams GetParams();
}

/// <summary>
/// Creates a minmax skip index on this column.
/// </summary>
/// <remarks>
/// Minmax stores minimum and maximum values per granule.
/// Best for range queries on numeric or date columns.
/// </remarks>
/// <example>
/// <code>
/// public class Order
/// {
///     [MinMaxIndex(Granularity = 2)]
///     public DateTime CreatedAt { get; set; }
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
public sealed class MinMaxIndexAttribute : SkipIndexAttribute
{
    /// <inheritdoc />
    public override SkipIndexType IndexType => SkipIndexType.Minmax;

    /// <inheritdoc />
    public override SkipIndexParams GetParams() => new();
}

/// <summary>
/// Creates a bloom_filter skip index on this column.
/// </summary>
/// <remarks>
/// Bloom filters are probabilistic data structures for exact value matching.
/// Best for equality queries and array contains checks.
/// </remarks>
/// <example>
/// <code>
/// public class Product
/// {
///     [BloomFilterIndex(FalsePositive = 0.025, Granularity = 3)]
///     public string[] Tags { get; set; } = [];
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
public sealed class BloomFilterIndexAttribute : SkipIndexAttribute
{
    /// <summary>
    /// Gets or sets the false positive rate. Default is 0.025 (2.5%).
    /// Valid range: 0.001 to 0.5.
    /// </summary>
    public double FalsePositive { get; set; } = 0.025;

    /// <inheritdoc />
    public override SkipIndexType IndexType => SkipIndexType.BloomFilter;

    /// <inheritdoc />
    public override SkipIndexParams GetParams()
        => SkipIndexParams.ForBloomFilter(FalsePositive);
}

/// <summary>
/// Creates a tokenbf_v1 skip index on this column for tokenized text search.
/// </summary>
/// <remarks>
/// TokenBF tokenizes strings and stores tokens in a bloom filter.
/// Best for log analysis, URL parameter search, and tokenized text.
/// </remarks>
/// <example>
/// <code>
/// public class LogEntry
/// {
///     [TokenBFIndex(Granularity = 4)]
///     public string ErrorMessage { get; set; } = string.Empty;
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
public sealed class TokenBFIndexAttribute : SkipIndexAttribute
{
    /// <summary>
    /// Gets or sets the bloom filter size in bytes. Default is 10240.
    /// Valid range: 256 to 1048576.
    /// </summary>
    public int Size { get; set; } = 10240;

    /// <summary>
    /// Gets or sets the number of hash functions. Default is 3.
    /// Valid range: 1 to 10.
    /// </summary>
    public int Hashes { get; set; } = 3;

    /// <summary>
    /// Gets or sets the random seed for hash functions. Default is 0.
    /// </summary>
    public int Seed { get; set; }

    /// <inheritdoc />
    public override SkipIndexType IndexType => SkipIndexType.TokenBF;

    /// <inheritdoc />
    public override SkipIndexParams GetParams()
        => SkipIndexParams.ForTokenBF(Size, Hashes, Seed);
}

/// <summary>
/// Creates a ngrambf_v1 skip index on this column for fuzzy text matching.
/// </summary>
/// <remarks>
/// NgramBF splits strings into n-grams and stores them in a bloom filter.
/// Best for substring search and partial text matching.
/// </remarks>
/// <example>
/// <code>
/// public class Article
/// {
///     [NgramBFIndex(NgramSize = 4, Granularity = 5)]
///     public string Description { get; set; } = string.Empty;
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
public sealed class NgramBFIndexAttribute : SkipIndexAttribute
{
    /// <summary>
    /// Gets or sets the n-gram size. Default is 4.
    /// Valid range: 1 to 10.
    /// </summary>
    public int NgramSize { get; set; } = 4;

    /// <summary>
    /// Gets or sets the bloom filter size in bytes. Default is 10240.
    /// Valid range: 256 to 1048576.
    /// </summary>
    public int Size { get; set; } = 10240;

    /// <summary>
    /// Gets or sets the number of hash functions. Default is 3.
    /// Valid range: 1 to 10.
    /// </summary>
    public int Hashes { get; set; } = 3;

    /// <summary>
    /// Gets or sets the random seed for hash functions. Default is 0.
    /// </summary>
    public int Seed { get; set; }

    /// <inheritdoc />
    public override SkipIndexType IndexType => SkipIndexType.NgramBF;

    /// <inheritdoc />
    public override SkipIndexParams GetParams()
        => SkipIndexParams.ForNgramBF(NgramSize, Size, Hashes, Seed);
}

/// <summary>
/// Creates a set skip index on this column for low-cardinality exact matching.
/// </summary>
/// <remarks>
/// Set stores all distinct values up to the limit.
/// Best for low-cardinality columns with exact matching queries.
/// </remarks>
/// <example>
/// <code>
/// public class Order
/// {
///     [SetIndex(MaxRows = 100, Granularity = 2)]
///     public string Status { get; set; } = string.Empty;
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
public sealed class SetIndexAttribute : SkipIndexAttribute
{
    /// <summary>
    /// Gets or sets the maximum distinct values to store. Default is 100.
    /// Valid range: 1 to 100000.
    /// </summary>
    public int MaxRows { get; set; } = 100;

    /// <inheritdoc />
    public override SkipIndexType IndexType => SkipIndexType.Set;

    /// <inheritdoc />
    public override SkipIndexParams GetParams()
        => SkipIndexParams.ForSet(MaxRows);
}
