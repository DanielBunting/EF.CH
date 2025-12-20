using EF.CH.Metadata;
using EF.CH.Storage;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for configuring ClickHouse compression codecs on properties.
/// </summary>
public static class ClickHousePropertyBuilderCodecExtensions
{
    /// <summary>
    /// Configures the compression codec for this column using a raw codec string.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="codecSpec">Codec specification (e.g., "DoubleDelta, LZ4" or "ZSTD(9)").</param>
    /// <returns>The property builder for chaining.</returns>
    /// <exception cref="ArgumentException">If codecSpec is null or whitespace.</exception>
    /// <example>
    /// <code>
    /// entity.Property(x => x.Timestamp).HasCodec("DoubleDelta, LZ4");
    /// entity.Property(x => x.RawData).HasCodec("ZSTD(9)");
    /// </code>
    /// </example>
    public static PropertyBuilder<TProperty> HasCodec<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        string codecSpec)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(codecSpec);

        propertyBuilder.HasAnnotation(ClickHouseAnnotationNames.CompressionCodec, codecSpec);
        return propertyBuilder;
    }

    /// <summary>
    /// Configures the compression codec for this column using a fluent builder.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="configure">A function that configures the codec chain.</param>
    /// <returns>The property builder for chaining.</returns>
    /// <exception cref="ArgumentNullException">If configure is null.</exception>
    /// <example>
    /// <code>
    /// entity.Property(x => x.Timestamp)
    ///     .HasCodec(c => c.DoubleDelta().LZ4());
    ///
    /// entity.Property(x => x.SensorId)
    ///     .HasCodec(c => c.Delta().ZSTD(3));
    /// </code>
    /// </example>
    public static PropertyBuilder<TProperty> HasCodec<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        Func<CodecChainBuilder, CodecChainBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new CodecChainBuilder();
        var result = configure(builder);
        var codecSpec = result.Build();

        propertyBuilder.HasAnnotation(ClickHouseAnnotationNames.CompressionCodec, codecSpec);
        return propertyBuilder;
    }

    /// <summary>
    /// Configures optimal codec for timestamp columns (DoubleDelta + LZ4).
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <returns>The property builder for chaining.</returns>
    /// <remarks>
    /// DoubleDelta stores the delta of deltas, ideal for timestamps that increment by similar amounts.
    /// LZ4 provides fast compression of the encoded data.
    /// </remarks>
    public static PropertyBuilder<TProperty> HasTimestampCodec<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder)
        => propertyBuilder.HasCodec(c => c.DoubleDelta().LZ4());

    /// <summary>
    /// Configures optimal codec for sequential/monotonic columns (Delta + ZSTD).
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <returns>The property builder for chaining.</returns>
    /// <remarks>
    /// Delta stores differences between consecutive values, ideal for monotonically increasing IDs or counters.
    /// </remarks>
    public static PropertyBuilder<TProperty> HasSequentialCodec<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder)
        => propertyBuilder.HasCodec(c => c.Delta().ZSTD());

    /// <summary>
    /// Configures optimal codec for floating-point sensor data (Gorilla + ZSTD).
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <returns>The property builder for chaining.</returns>
    /// <remarks>
    /// Gorilla uses XOR-based encoding optimized for slowly changing float values.
    /// Warning: Do not combine with Delta or DoubleDelta.
    /// </remarks>
    public static PropertyBuilder<TProperty> HasFloatCodec<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder)
        => propertyBuilder.HasCodec(c => c.Gorilla().ZSTD(1));

    /// <summary>
    /// Configures high compression for large text/binary data (ZSTD level 9).
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <returns>The property builder for chaining.</returns>
    /// <remarks>
    /// ZSTD level 9 provides high compression at the cost of slower writes.
    /// Ideal for JSON, XML, or binary payloads.
    /// </remarks>
    public static PropertyBuilder<TProperty> HasHighCompressionCodec<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder)
        => propertyBuilder.HasCodec(c => c.ZSTD(9));

    /// <summary>
    /// Configures optimal codec for integers with sparse values (T64 + LZ4).
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <returns>The property builder for chaining.</returns>
    /// <remarks>
    /// T64 transposes a 64x64 bit matrix and strips unused high bits.
    /// Ideal for integers that don't use their full range.
    /// </remarks>
    public static PropertyBuilder<TProperty> HasIntegerCodec<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder)
        => propertyBuilder.HasCodec(c => c.T64().LZ4());

    /// <summary>
    /// Disables compression for this column.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <returns>The property builder for chaining.</returns>
    /// <remarks>
    /// Use for columns where compression overhead outweighs benefits,
    /// or for already-compressed data.
    /// </remarks>
    public static PropertyBuilder<TProperty> HasNoCompression<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder)
        => propertyBuilder.HasCodec(c => c.None());
}
