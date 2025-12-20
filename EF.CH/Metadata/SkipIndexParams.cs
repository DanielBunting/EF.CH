using System.Globalization;

namespace EF.CH.Metadata;

/// <summary>
/// Parameters for ClickHouse skip index configuration.
/// </summary>
public record SkipIndexParams
{
    #region BloomFilter Parameters

    /// <summary>
    /// False positive rate for bloom_filter index.
    /// Valid range: 0.001 to 0.5. Default: 0.025.
    /// </summary>
    public double? BloomFilterFalsePositive { get; init; }

    #endregion

    #region TokenBF Parameters

    /// <summary>
    /// Size of the bloom filter in bytes for tokenbf_v1.
    /// Valid range: 256 to 1048576. Default: 10240.
    /// </summary>
    public int? TokenBFSize { get; init; }

    /// <summary>
    /// Number of hash functions for tokenbf_v1.
    /// Valid range: 1 to 10. Default: 3.
    /// </summary>
    public int? TokenBFHashes { get; init; }

    /// <summary>
    /// Random seed for tokenbf_v1 hash functions.
    /// Default: 0.
    /// </summary>
    public int? TokenBFSeed { get; init; }

    #endregion

    #region NgramBF Parameters

    /// <summary>
    /// N-gram size for ngrambf_v1.
    /// Valid range: 1 to 10. Default: 4.
    /// </summary>
    public int? NgramSize { get; init; }

    /// <summary>
    /// Size of the bloom filter in bytes for ngrambf_v1.
    /// Valid range: 256 to 1048576. Default: 10240.
    /// </summary>
    public int? NgramBFSize { get; init; }

    /// <summary>
    /// Number of hash functions for ngrambf_v1.
    /// Valid range: 1 to 10. Default: 3.
    /// </summary>
    public int? NgramBFHashes { get; init; }

    /// <summary>
    /// Random seed for ngrambf_v1 hash functions.
    /// Default: 0.
    /// </summary>
    public int? NgramBFSeed { get; init; }

    #endregion

    #region Set Parameters

    /// <summary>
    /// Maximum number of distinct values to store for set index.
    /// Valid range: 1 to 100000. Default: 100.
    /// </summary>
    public int? SetMaxRows { get; init; }

    #endregion

    /// <summary>
    /// Builds the TYPE specification string for DDL generation.
    /// </summary>
    /// <param name="indexType">The skip index type.</param>
    /// <returns>The TYPE clause (e.g., "TYPE bloom_filter(0.025)").</returns>
    public string BuildTypeSpecification(SkipIndexType indexType)
    {
        return indexType switch
        {
            SkipIndexType.Minmax => "TYPE minmax",
            SkipIndexType.BloomFilter => BuildBloomFilterSpec(),
            SkipIndexType.TokenBF => BuildTokenBFSpec(),
            SkipIndexType.NgramBF => BuildNgramBFSpec(),
            SkipIndexType.Set => BuildSetSpec(),
            _ => "TYPE minmax"
        };
    }

    private string BuildBloomFilterSpec()
    {
        var falsePositive = BloomFilterFalsePositive ?? 0.025;
        return $"TYPE bloom_filter({falsePositive.ToString(CultureInfo.InvariantCulture)})";
    }

    private string BuildTokenBFSpec()
    {
        var size = TokenBFSize ?? 10240;
        var hashes = TokenBFHashes ?? 3;
        var seed = TokenBFSeed ?? 0;
        return $"TYPE tokenbf_v1({size}, {hashes}, {seed})";
    }

    private string BuildNgramBFSpec()
    {
        var ngramSize = NgramSize ?? 4;
        var size = NgramBFSize ?? 10240;
        var hashes = NgramBFHashes ?? 3;
        var seed = NgramBFSeed ?? 0;
        return $"TYPE ngrambf_v1({ngramSize}, {size}, {hashes}, {seed})";
    }

    private string BuildSetSpec()
    {
        var maxRows = SetMaxRows ?? 100;
        return $"TYPE set({maxRows})";
    }

    /// <summary>
    /// Creates default parameters for a bloom filter index.
    /// </summary>
    public static SkipIndexParams ForBloomFilter(double falsePositive = 0.025)
        => new() { BloomFilterFalsePositive = falsePositive };

    /// <summary>
    /// Creates default parameters for a tokenbf_v1 index.
    /// </summary>
    public static SkipIndexParams ForTokenBF(int size = 10240, int hashes = 3, int seed = 0)
        => new() { TokenBFSize = size, TokenBFHashes = hashes, TokenBFSeed = seed };

    /// <summary>
    /// Creates default parameters for a ngrambf_v1 index.
    /// </summary>
    public static SkipIndexParams ForNgramBF(int ngramSize = 4, int size = 10240, int hashes = 3, int seed = 0)
        => new() { NgramSize = ngramSize, NgramBFSize = size, NgramBFHashes = hashes, NgramBFSeed = seed };

    /// <summary>
    /// Creates default parameters for a set index.
    /// </summary>
    public static SkipIndexParams ForSet(int maxRows = 100)
        => new() { SetMaxRows = maxRows };
}
