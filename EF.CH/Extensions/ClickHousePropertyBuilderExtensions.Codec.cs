using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for configuring ClickHouse compression codecs on properties.
/// </summary>
public static class ClickHousePropertyBuilderCodecExtensions
{
    /// <summary>
    /// Configures the compression codec for this column using a fluent builder.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="configure">An action that configures the codec chain.</param>
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
        Action<CodecChainBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new CodecChainBuilder();
        configure(builder);
        var codecSpec = builder.Build();

        propertyBuilder.HasAnnotation(ClickHouseAnnotationNames.CompressionCodec, codecSpec);
        return propertyBuilder;
    }

    /// <summary>
    /// Configures the compression codec for this column using a fluent builder.
    /// </summary>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="configure">An action that configures the codec chain.</param>
    /// <returns>The property builder for chaining.</returns>
    public static PropertyBuilder HasCodec(
        this PropertyBuilder propertyBuilder,
        Action<CodecChainBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new CodecChainBuilder();
        configure(builder);
        var codecSpec = builder.Build();

        propertyBuilder.HasAnnotation(ClickHouseAnnotationNames.CompressionCodec, codecSpec);
        return propertyBuilder;
    }
}
