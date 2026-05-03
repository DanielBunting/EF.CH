namespace EF.CH.Extensions;

/// <summary>
/// Fluent builder for creating codec chains.
/// Codecs are applied in order: preprocessing (Delta, DoubleDelta, Gorilla, T64) first, then compression (LZ4, ZSTD).
/// </summary>
public class CodecChainBuilder
{
    private readonly List<string> _codecs = [];

    /// <summary>
    /// LZ4 - Fast compression (default for self-hosted ClickHouse).
    /// </summary>
    public CodecChainBuilder LZ4()
    {
        _codecs.Add("LZ4");
        return this;
    }

    /// <summary>
    /// ZSTD with the default compression level (level 1, emitted as <c>ZSTD</c>).
    /// </summary>
    public CodecChainBuilder ZSTD()
    {
        _codecs.Add("ZSTD");
        return this;
    }

    /// <summary>
    /// ZSTD with an explicit compression level.
    /// </summary>
    /// <param name="level">Compression level from 1 to 22.</param>
    /// <exception cref="ArgumentOutOfRangeException">If level is not between 1 and 22.</exception>
    public CodecChainBuilder ZSTD(int level)
    {
        if (level < 1 || level > 22)
            throw new ArgumentOutOfRangeException(nameof(level), level, "ZSTD level must be between 1 and 22.");

        _codecs.Add($"ZSTD({level})");
        return this;
    }

    /// <summary>
    /// Delta - Stores differences between consecutive values.
    /// Best for monotonically increasing sequences (IDs, counters).
    /// </summary>
    public CodecChainBuilder Delta()
    {
        _codecs.Add("Delta");
        return this;
    }

    /// <summary>
    /// DoubleDelta - Stores delta of deltas.
    /// Best for timestamps and slowly changing sequences.
    /// </summary>
    public CodecChainBuilder DoubleDelta()
    {
        _codecs.Add("DoubleDelta");
        return this;
    }

    /// <summary>
    /// Gorilla - XOR-based encoding for floats.
    /// Best for slowly changing floating-point values (sensor data).
    /// Warning: Do not combine with Delta or DoubleDelta.
    /// </summary>
    public CodecChainBuilder Gorilla()
    {
        _codecs.Add("Gorilla");
        return this;
    }

    /// <summary>
    /// T64 - Transposes 64x64 bit matrix, strips unused high bits.
    /// Best for integers that don't use their full range.
    /// </summary>
    public CodecChainBuilder T64()
    {
        _codecs.Add("T64");
        return this;
    }

    /// <summary>
    /// FPC with the default prediction level (level 12, emitted as <c>FPC</c>).
    /// </summary>
    public CodecChainBuilder FPC()
    {
        _codecs.Add("FPC");
        return this;
    }

    /// <summary>
    /// FPC with an explicit prediction level.
    /// </summary>
    /// <param name="level">Prediction level from 1 to 28.</param>
    /// <exception cref="ArgumentOutOfRangeException">If level is not between 1 and 28.</exception>
    public CodecChainBuilder FPC(int level)
    {
        if (level < 1 || level > 28)
            throw new ArgumentOutOfRangeException(nameof(level), level, "FPC level must be between 1 and 28.");

        _codecs.Add($"FPC({level})");
        return this;
    }

    /// <summary>
    /// None - No compression. Clears any previously added codecs.
    /// </summary>
    public CodecChainBuilder None()
    {
        _codecs.Clear();
        _codecs.Add("NONE");
        return this;
    }

    /// <summary>
    /// Escape hatch for codecs not yet exposed as typed methods (e.g.
    /// <c>LZ4HC(9)</c>, <c>ZSTD_QAT(3)</c>, <c>AES_128_GCM_SIV</c>).
    /// The argument is emitted verbatim — caller is responsible for valid
    /// ClickHouse codec syntax and any level constraints.
    /// </summary>
    /// <param name="codec">Raw codec spec, e.g. <c>"LZ4HC(9)"</c>.</param>
    /// <exception cref="ArgumentException">If <paramref name="codec"/> is null or whitespace.</exception>
    public CodecChainBuilder Raw(string codec)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(codec);
        _codecs.Add(codec);
        return this;
    }

    /// <summary>
    /// Builds the codec specification string.
    /// </summary>
    /// <returns>Comma-separated codec string (e.g., "DoubleDelta, LZ4").</returns>
    /// <exception cref="InvalidOperationException">If no codecs have been added.</exception>
    public string Build()
    {
        if (_codecs.Count == 0)
            throw new InvalidOperationException("At least one codec must be specified. Use LZ4(), ZSTD(), or another codec method.");

        return string.Join(", ", _codecs);
    }

    /// <inheritdoc />
    public override string ToString() => _codecs.Count > 0 ? Build() : "<empty>";
}
