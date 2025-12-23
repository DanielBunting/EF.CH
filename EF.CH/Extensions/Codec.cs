namespace EF.CH.Extensions;

/// <summary>
/// Predefined ClickHouse compression codec constants.
/// </summary>
public static class Codec
{
    /// <summary>
    /// Fast general-purpose compression (default for self-hosted ClickHouse).
    /// </summary>
    public const string LZ4 = "LZ4";

    /// <summary>
    /// Higher compression ratio than LZ4, configurable levels 1-22 (default in ClickHouse Cloud).
    /// </summary>
    public const string ZSTD = "ZSTD";

    /// <summary>
    /// Stores differences between consecutive values. Best for monotonically increasing sequences.
    /// </summary>
    public const string Delta = "Delta";

    /// <summary>
    /// Stores delta of deltas. Best for timestamps and slowly changing sequences.
    /// </summary>
    public const string DoubleDelta = "DoubleDelta";

    /// <summary>
    /// XOR-based encoding for floats. Best for slowly changing floating-point values.
    /// </summary>
    public const string Gorilla = "Gorilla";

    /// <summary>
    /// Transposes 64x64 bit matrix, strips unused high bits. Best for sparse integer data.
    /// </summary>
    public const string T64 = "T64";

    /// <summary>
    /// Fast floating-point compression. Configurable levels 1-28.
    /// </summary>
    public const string FPC = "FPC";

    /// <summary>
    /// No compression.
    /// </summary>
    public const string None = "NONE";

    /// <summary>
    /// Creates a ZSTD codec with specified compression level.
    /// </summary>
    /// <param name="level">Compression level from 1 (fastest) to 22 (best compression).</param>
    /// <returns>ZSTD codec string with level, e.g., "ZSTD(3)".</returns>
    /// <exception cref="ArgumentOutOfRangeException">If level is not between 1 and 22.</exception>
    public static string ZstdLevel(int level)
    {
        if (level < 1 || level > 22)
            throw new ArgumentOutOfRangeException(nameof(level), level, "ZSTD level must be between 1 and 22.");
        return $"ZSTD({level})";
    }

    /// <summary>
    /// Creates an FPC codec with specified prediction level.
    /// </summary>
    /// <param name="level">Prediction level from 1 to 28 (default is 12).</param>
    /// <returns>FPC codec string with level, e.g., "FPC(12)".</returns>
    /// <exception cref="ArgumentOutOfRangeException">If level is not between 1 and 28.</exception>
    public static string FpcLevel(int level)
    {
        if (level < 1 || level > 28)
            throw new ArgumentOutOfRangeException(nameof(level), level, "FPC level must be between 1 and 28.");
        return $"FPC({level})";
    }
}
