using EF.CH.Extensions;
using Xunit;

namespace EF.CH.Tests.Config;

/// <summary>
/// Codec ordering rule: preprocessing codecs (Delta, DoubleDelta, T64,
/// Gorilla, FPC) must come BEFORE general compression codecs (LZ4, ZSTD).
/// Putting Delta after ZSTD is a documented foot-gun — ClickHouse accepts
/// the chain at DDL but the result is not what the user intended.
///
/// EF.CH's policy today is "trust the user": the builder emits the chain in
/// the order the user provided it, leaving validation to the server. This
/// theory pins that policy; if a future model validator starts rejecting
/// reversed orderings, the assertion in this test should flip.
/// </summary>
public class CodecChainOrderValidationTests
{
    [Theory]
    // Reversed orderings — preprocessing AFTER compression. Per the current
    // policy, the builder preserves the order verbatim.
    [InlineData("ZSTD then Delta",  /* configure: */ "ZstdThenDelta",  "ZSTD, Delta")]
    [InlineData("LZ4 then Delta",   /* configure: */ "Lz4ThenDelta",   "LZ4, Delta")]
    [InlineData("ZSTD then T64",    /* configure: */ "ZstdThenT64",    "ZSTD, T64")]
    public void CodecChain_DeltaAfterZstd_IsRejectedOrPreservedAsConfigured(
        string label, string configKey, string expectedSpec)
    {
        var builder = new CodecChainBuilder();
        switch (configKey)
        {
            case "ZstdThenDelta": builder.ZSTD().Delta(); break;
            case "Lz4ThenDelta":  builder.LZ4().Delta();  break;
            case "ZstdThenT64":   builder.ZSTD().T64();   break;
            default: throw new ArgumentOutOfRangeException(nameof(configKey), configKey, null);
        }

        var spec = builder.Build();

        // Current contract: preserve as configured. If validation is
        // introduced later, this assertion flips to expect a throw.
        Assert.Equal(expectedSpec, spec);
    }

    /// <summary>
    /// Sane ordering for completeness — Delta before ZSTD is the canonical
    /// shape and must round-trip identically.
    /// </summary>
    [Theory]
    [InlineData("Delta then ZSTD", "DeltaThenZstd", "Delta, ZSTD")]
    [InlineData("DoubleDelta then LZ4", "DoubleDeltaThenLz4", "DoubleDelta, LZ4")]
    public void CodecChain_PreprocessingThenCompression_PreservesOrder(
        string label, string configKey, string expectedSpec)
    {
        var builder = new CodecChainBuilder();
        switch (configKey)
        {
            case "DeltaThenZstd":      builder.Delta().ZSTD(); break;
            case "DoubleDeltaThenLz4": builder.DoubleDelta().LZ4(); break;
            default: throw new ArgumentOutOfRangeException(nameof(configKey), configKey, null);
        }

        Assert.Equal(expectedSpec, builder.Build());
    }
}
