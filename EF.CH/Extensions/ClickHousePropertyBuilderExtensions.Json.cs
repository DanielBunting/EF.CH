using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for configuring ClickHouse native JSON columns on properties.
/// </summary>
/// <remarks>
/// Requires ClickHouse 24.8+ for native JSON type support.
/// </remarks>
public static class ClickHousePropertyBuilderJsonExtensions
{
    /// <summary>
    /// Configures the maximum number of dynamic paths for this JSON column.
    /// </summary>
    /// <typeparam name="TProperty">The property type (JsonElement, JsonDocument, or typed POCO).</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="maxPaths">Maximum number of paths stored as subcolumns (default: 1024 in ClickHouse).</param>
    /// <returns>The property builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">If maxPaths is negative.</exception>
    /// <example>
    /// <code>
    /// entity.Property(x => x.Metadata)
    ///     .HasColumnType("JSON")
    ///     .HasMaxDynamicPaths(2048);
    /// </code>
    /// </example>
    public static PropertyBuilder<TProperty> HasMaxDynamicPaths<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        int maxPaths)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentOutOfRangeException.ThrowIfNegative(maxPaths);

        propertyBuilder.HasAnnotation(ClickHouseAnnotationNames.JsonMaxDynamicPaths, maxPaths);
        return propertyBuilder;
    }

    /// <summary>
    /// Configures the maximum number of dynamic types per path for this JSON column.
    /// </summary>
    /// <typeparam name="TProperty">The property type (JsonElement, JsonDocument, or typed POCO).</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="maxTypes">Maximum number of types per path (default: 32 in ClickHouse).</param>
    /// <returns>The property builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">If maxTypes is negative.</exception>
    /// <example>
    /// <code>
    /// entity.Property(x => x.Metadata)
    ///     .HasColumnType("JSON")
    ///     .HasMaxDynamicTypes(64);
    /// </code>
    /// </example>
    public static PropertyBuilder<TProperty> HasMaxDynamicTypes<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        int maxTypes)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentOutOfRangeException.ThrowIfNegative(maxTypes);

        propertyBuilder.HasAnnotation(ClickHouseAnnotationNames.JsonMaxDynamicTypes, maxTypes);
        return propertyBuilder;
    }

    /// <summary>
    /// Configures JSON column options in one call.
    /// </summary>
    /// <typeparam name="TProperty">The property type (JsonElement, JsonDocument, or typed POCO).</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="maxDynamicPaths">Maximum number of paths stored as subcolumns (null = ClickHouse default).</param>
    /// <param name="maxDynamicTypes">Maximum number of types per path (null = ClickHouse default).</param>
    /// <returns>The property builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">If any parameter is negative.</exception>
    /// <example>
    /// <code>
    /// entity.Property(x => x.Metadata)
    ///     .HasColumnType("JSON")
    ///     .HasJsonOptions(maxDynamicPaths: 1024, maxDynamicTypes: 32);
    /// </code>
    /// </example>
    public static PropertyBuilder<TProperty> HasJsonOptions<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        int? maxDynamicPaths = null,
        int? maxDynamicTypes = null)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        if (maxDynamicPaths.HasValue)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(maxDynamicPaths.Value);
            propertyBuilder.HasAnnotation(ClickHouseAnnotationNames.JsonMaxDynamicPaths, maxDynamicPaths.Value);
        }

        if (maxDynamicTypes.HasValue)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(maxDynamicTypes.Value);
            propertyBuilder.HasAnnotation(ClickHouseAnnotationNames.JsonMaxDynamicTypes, maxDynamicTypes.Value);
        }

        return propertyBuilder;
    }

    /// <summary>
    /// Marks this property as a typed JSON column with POCO mapping.
    /// </summary>
    /// <typeparam name="TProperty">The POCO type to map.</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <returns>The property builder for chaining.</returns>
    /// <remarks>
    /// When marked as typed JSON, property navigation on the POCO will be translated
    /// to ClickHouse subcolumn access syntax in LINQ queries.
    /// </remarks>
    /// <example>
    /// <code>
    /// public class OrderMetadata
    /// {
    ///     public string CustomerName { get; set; }
    ///     public Address ShippingAddress { get; set; }
    /// }
    ///
    /// entity.Property(x => x.Metadata)
    ///     .HasColumnType("JSON")
    ///     .HasTypedJson();
    ///
    /// // Enables: context.Orders.Select(o => o.Metadata.CustomerName)
    /// // Translates to: SELECT "Metadata"."CustomerName" FROM ...
    /// </code>
    /// </example>
    public static PropertyBuilder<TProperty> HasTypedJson<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder)
        where TProperty : class
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.HasAnnotation(ClickHouseAnnotationNames.JsonTypedMapping, typeof(TProperty));
        propertyBuilder.HasColumnType("JSON");
        return propertyBuilder;
    }
}
