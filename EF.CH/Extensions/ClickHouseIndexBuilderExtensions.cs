using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for configuring ClickHouse data skipping indices.
/// </summary>
public static class ClickHouseIndexBuilderExtensions
{
    #region Granularity

    /// <summary>
    /// Sets the granularity for this skip index.
    /// </summary>
    /// <param name="indexBuilder">The index builder.</param>
    /// <param name="granularity">Granularity value (1-1000). Default is 3.</param>
    /// <returns>The index builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">If granularity is outside valid range.</exception>
    /// <example>
    /// <code>
    /// entity.HasIndex(x => x.CreatedAt)
    ///     .HasGranularity(4);
    /// </code>
    /// </example>
    public static IndexBuilder HasGranularity(this IndexBuilder indexBuilder, int granularity)
    {
        ArgumentNullException.ThrowIfNull(indexBuilder);

        if (granularity is < 1 or > 1000)
        {
            throw new ArgumentOutOfRangeException(
                nameof(granularity),
                granularity,
                "Granularity must be between 1 and 1000.");
        }

        indexBuilder.HasAnnotation(ClickHouseAnnotationNames.SkipIndexGranularity, granularity);
        return indexBuilder;
    }

    /// <summary>
    /// Sets the granularity for this skip index (generic version).
    /// </summary>
    public static IndexBuilder<TEntity> HasGranularity<TEntity>(
        this IndexBuilder<TEntity> indexBuilder,
        int granularity)
    {
        ((IndexBuilder)indexBuilder).HasGranularity(granularity);
        return indexBuilder;
    }

    #endregion

    #region Minmax

    /// <summary>
    /// Configures this index as a minmax skip index (default).
    /// </summary>
    /// <param name="indexBuilder">The index builder.</param>
    /// <returns>The index builder for chaining.</returns>
    /// <remarks>
    /// Minmax stores minimum and maximum values per granule.
    /// Best for range queries on numeric or date columns.
    /// </remarks>
    /// <example>
    /// <code>
    /// entity.HasIndex(x => x.Price)
    ///     .UseMinmax()
    ///     .HasGranularity(2);
    /// </code>
    /// </example>
    public static IndexBuilder UseMinmax(this IndexBuilder indexBuilder)
    {
        ArgumentNullException.ThrowIfNull(indexBuilder);

        indexBuilder.HasAnnotation(ClickHouseAnnotationNames.SkipIndexType, SkipIndexType.Minmax);
        return indexBuilder;
    }

    /// <summary>
    /// Configures this index as a minmax skip index (generic version).
    /// </summary>
    public static IndexBuilder<TEntity> UseMinmax<TEntity>(this IndexBuilder<TEntity> indexBuilder)
    {
        ((IndexBuilder)indexBuilder).UseMinmax();
        return indexBuilder;
    }

    #endregion

    #region BloomFilter

    /// <summary>
    /// Configures this index as a bloom_filter skip index.
    /// </summary>
    /// <param name="indexBuilder">The index builder.</param>
    /// <param name="falsePositive">False positive rate (0.001-0.5). Default is 0.025 (2.5%).</param>
    /// <returns>The index builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">If falsePositive is outside valid range.</exception>
    /// <remarks>
    /// Bloom filters are probabilistic data structures for exact value matching.
    /// Best for equality queries and array contains checks.
    /// </remarks>
    /// <example>
    /// <code>
    /// entity.HasIndex(x => x.Tags)
    ///     .UseBloomFilter(falsePositive: 0.025)
    ///     .HasGranularity(3);
    /// </code>
    /// </example>
    public static IndexBuilder UseBloomFilter(this IndexBuilder indexBuilder, double falsePositive = 0.025)
    {
        ArgumentNullException.ThrowIfNull(indexBuilder);

        if (falsePositive is < 0.001 or > 0.5)
        {
            throw new ArgumentOutOfRangeException(
                nameof(falsePositive),
                falsePositive,
                "False positive rate must be between 0.001 and 0.5.");
        }

        indexBuilder.HasAnnotation(ClickHouseAnnotationNames.SkipIndexType, SkipIndexType.BloomFilter);
        indexBuilder.HasAnnotation(
            ClickHouseAnnotationNames.SkipIndexParams,
            SkipIndexParams.ForBloomFilter(falsePositive));

        return indexBuilder;
    }

    /// <summary>
    /// Configures this index as a bloom_filter skip index (generic version).
    /// </summary>
    public static IndexBuilder<TEntity> UseBloomFilter<TEntity>(
        this IndexBuilder<TEntity> indexBuilder,
        double falsePositive = 0.025)
    {
        ((IndexBuilder)indexBuilder).UseBloomFilter(falsePositive);
        return indexBuilder;
    }

    #endregion

    #region TokenBF

    /// <summary>
    /// Configures this index as a tokenbf_v1 skip index for tokenized text search.
    /// </summary>
    /// <param name="indexBuilder">The index builder.</param>
    /// <param name="size">Bloom filter size in bytes (256-1048576). Default is 10240.</param>
    /// <param name="hashes">Number of hash functions (1-10). Default is 3.</param>
    /// <param name="seed">Random seed for hash functions. Default is 0.</param>
    /// <returns>The index builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">If parameters are outside valid ranges.</exception>
    /// <remarks>
    /// TokenBF tokenizes strings and stores tokens in a bloom filter.
    /// Best for log analysis, URL parameter search, and tokenized text.
    /// </remarks>
    /// <example>
    /// <code>
    /// entity.HasIndex(x => x.ErrorMessage)
    ///     .UseTokenBF(size: 10240, hashes: 3, seed: 0)
    ///     .HasGranularity(4);
    /// </code>
    /// </example>
    public static IndexBuilder UseTokenBF(
        this IndexBuilder indexBuilder,
        int size = 10240,
        int hashes = 3,
        int seed = 0)
    {
        ArgumentNullException.ThrowIfNull(indexBuilder);
        ValidateTokenBFParams(size, hashes);

        indexBuilder.HasAnnotation(ClickHouseAnnotationNames.SkipIndexType, SkipIndexType.TokenBF);
        indexBuilder.HasAnnotation(
            ClickHouseAnnotationNames.SkipIndexParams,
            SkipIndexParams.ForTokenBF(size, hashes, seed));

        return indexBuilder;
    }

    /// <summary>
    /// Configures this index as a tokenbf_v1 skip index (generic version).
    /// </summary>
    public static IndexBuilder<TEntity> UseTokenBF<TEntity>(
        this IndexBuilder<TEntity> indexBuilder,
        int size = 10240,
        int hashes = 3,
        int seed = 0)
    {
        ((IndexBuilder)indexBuilder).UseTokenBF(size, hashes, seed);
        return indexBuilder;
    }

    #endregion

    #region NgramBF

    /// <summary>
    /// Configures this index as a ngrambf_v1 skip index for fuzzy text matching.
    /// </summary>
    /// <param name="indexBuilder">The index builder.</param>
    /// <param name="ngramSize">N-gram size (1-10). Default is 4.</param>
    /// <param name="size">Bloom filter size in bytes (256-1048576). Default is 10240.</param>
    /// <param name="hashes">Number of hash functions (1-10). Default is 3.</param>
    /// <param name="seed">Random seed for hash functions. Default is 0.</param>
    /// <returns>The index builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">If parameters are outside valid ranges.</exception>
    /// <remarks>
    /// NgramBF splits strings into n-grams and stores them in a bloom filter.
    /// Best for substring search and partial text matching.
    /// </remarks>
    /// <example>
    /// <code>
    /// entity.HasIndex(x => x.Description)
    ///     .UseNgramBF(ngramSize: 4, size: 10240, hashes: 3, seed: 0)
    ///     .HasGranularity(5);
    /// </code>
    /// </example>
    public static IndexBuilder UseNgramBF(
        this IndexBuilder indexBuilder,
        int ngramSize = 4,
        int size = 10240,
        int hashes = 3,
        int seed = 0)
    {
        ArgumentNullException.ThrowIfNull(indexBuilder);
        ValidateNgramBFParams(ngramSize, size, hashes);

        indexBuilder.HasAnnotation(ClickHouseAnnotationNames.SkipIndexType, SkipIndexType.NgramBF);
        indexBuilder.HasAnnotation(
            ClickHouseAnnotationNames.SkipIndexParams,
            SkipIndexParams.ForNgramBF(ngramSize, size, hashes, seed));

        return indexBuilder;
    }

    /// <summary>
    /// Configures this index as a ngrambf_v1 skip index (generic version).
    /// </summary>
    public static IndexBuilder<TEntity> UseNgramBF<TEntity>(
        this IndexBuilder<TEntity> indexBuilder,
        int ngramSize = 4,
        int size = 10240,
        int hashes = 3,
        int seed = 0)
    {
        ((IndexBuilder)indexBuilder).UseNgramBF(ngramSize, size, hashes, seed);
        return indexBuilder;
    }

    #endregion

    #region Set

    /// <summary>
    /// Configures this index as a set skip index for low-cardinality columns.
    /// </summary>
    /// <param name="indexBuilder">The index builder.</param>
    /// <param name="maxRows">Maximum distinct values to store (1-100000). Default is 100.</param>
    /// <returns>The index builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">If maxRows is outside valid range.</exception>
    /// <remarks>
    /// Set stores all distinct values up to the limit.
    /// Best for low-cardinality columns with exact matching queries.
    /// </remarks>
    /// <example>
    /// <code>
    /// entity.HasIndex(x => x.Status)
    ///     .UseSet(maxRows: 100)
    ///     .HasGranularity(2);
    /// </code>
    /// </example>
    public static IndexBuilder UseSet(this IndexBuilder indexBuilder, int maxRows = 100)
    {
        ArgumentNullException.ThrowIfNull(indexBuilder);

        if (maxRows is < 1 or > 100000)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxRows),
                maxRows,
                "MaxRows must be between 1 and 100000.");
        }

        indexBuilder.HasAnnotation(ClickHouseAnnotationNames.SkipIndexType, SkipIndexType.Set);
        indexBuilder.HasAnnotation(
            ClickHouseAnnotationNames.SkipIndexParams,
            SkipIndexParams.ForSet(maxRows));

        return indexBuilder;
    }

    /// <summary>
    /// Configures this index as a set skip index (generic version).
    /// </summary>
    public static IndexBuilder<TEntity> UseSet<TEntity>(
        this IndexBuilder<TEntity> indexBuilder,
        int maxRows = 100)
    {
        ((IndexBuilder)indexBuilder).UseSet(maxRows);
        return indexBuilder;
    }

    #endregion

    #region Validation Helpers

    private static void ValidateTokenBFParams(int size, int hashes)
    {
        if (size is < 256 or > 1048576)
        {
            throw new ArgumentOutOfRangeException(
                nameof(size),
                size,
                "Size must be between 256 and 1048576 bytes.");
        }

        if (hashes is < 1 or > 10)
        {
            throw new ArgumentOutOfRangeException(
                nameof(hashes),
                hashes,
                "Hashes must be between 1 and 10.");
        }
    }

    private static void ValidateNgramBFParams(int ngramSize, int size, int hashes)
    {
        if (ngramSize is < 1 or > 10)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ngramSize),
                ngramSize,
                "N-gram size must be between 1 and 10.");
        }

        ValidateTokenBFParams(size, hashes);
    }

    #endregion
}
