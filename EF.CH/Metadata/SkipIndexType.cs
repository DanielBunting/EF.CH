namespace EF.CH.Metadata;

/// <summary>
/// Types of data skipping indices supported by ClickHouse.
/// </summary>
public enum SkipIndexType
{
    /// <summary>
    /// Stores minimum and maximum values for numeric ranges.
    /// Best for: range queries on numeric or date columns.
    /// </summary>
    Minmax = 0,

    /// <summary>
    /// Probabilistic data structure for exact value matching.
    /// Best for: equality queries, array contains checks.
    /// Parameters: false positive rate (default 0.025).
    /// </summary>
    BloomFilter = 1,

    /// <summary>
    /// Token-based bloom filter for text search.
    /// Best for: log analysis, URL parameter search, tokenized text.
    /// Parameters: size, hashes, seed.
    /// </summary>
    TokenBF = 2,

    /// <summary>
    /// N-gram based bloom filter for fuzzy text matching.
    /// Best for: substring search, partial text matching.
    /// Parameters: ngram size, size, hashes, seed.
    /// </summary>
    NgramBF = 3,

    /// <summary>
    /// Stores all distinct values up to a limit.
    /// Best for: low-cardinality columns with exact matching.
    /// Parameters: max rows (default 100).
    /// </summary>
    Set = 4
}
