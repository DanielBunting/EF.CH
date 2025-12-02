using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for configuring ClickHouse-specific property options.
/// </summary>
public static class ClickHousePropertyBuilderExtensions
{
    /// <summary>
    /// Supported SimpleAggregateFunction function names.
    /// </summary>
    private static readonly HashSet<string> SupportedFunctions = new(StringComparer.OrdinalIgnoreCase)
    {
        "max", "min", "sum", "any", "anyLast",
        "groupBitAnd", "groupBitOr", "groupBitXor"
    };

    /// <summary>
    /// CLR type to ClickHouse type mapping for SimpleAggregateFunction.
    /// </summary>
    private static readonly Dictionary<Type, string> TypeMappings = new()
    {
        [typeof(double)] = "Float64",
        [typeof(float)] = "Float32",
        [typeof(decimal)] = "Decimal(18, 4)",
        [typeof(long)] = "Int64",
        [typeof(int)] = "Int32",
        [typeof(short)] = "Int16",
        [typeof(sbyte)] = "Int8",
        [typeof(ulong)] = "UInt64",
        [typeof(uint)] = "UInt32",
        [typeof(ushort)] = "UInt16",
        [typeof(byte)] = "UInt8",
        [typeof(DateTime)] = "DateTime64(3)",
        [typeof(DateOnly)] = "Date",
        [typeof(string)] = "String",
    };

    /// <summary>
    /// Configures the property to use ClickHouse SimpleAggregateFunction type.
    /// </summary>
    /// <remarks>
    /// SimpleAggregateFunction is ideal for aggregates where the state equals the final result,
    /// such as max, min, sum, any, and anyLast. The underlying value can be read directly
    /// without using -Merge combinators.
    ///
    /// Use with AggregatingMergeTree or SummingMergeTree engines for automatic aggregation
    /// during background merges.
    /// </remarks>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="functionName">The aggregate function name (max, min, sum, any, anyLast, groupBitAnd, groupBitOr, groupBitXor).</param>
    /// <returns>The property builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when the function name is not supported.</exception>
    /// <exception cref="NotSupportedException">Thrown when the property type is not supported for SimpleAggregateFunction.</exception>
    /// <example>
    /// <code>
    /// modelBuilder.Entity&lt;DailyStats&gt;(entity =>
    /// {
    ///     entity.UseAggregatingMergeTree(x => x.Date);
    ///
    ///     entity.Property(e => e.MaxOrderValue)
    ///         .HasSimpleAggregateFunction("max");
    ///
    ///     entity.Property(e => e.TotalQuantity)
    ///         .HasSimpleAggregateFunction("sum");
    /// });
    /// </code>
    /// </example>
    public static PropertyBuilder<TProperty> HasSimpleAggregateFunction<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        string functionName)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(functionName);

        if (!SupportedFunctions.Contains(functionName))
        {
            throw new ArgumentException(
                $"Unsupported SimpleAggregateFunction: '{functionName}'. " +
                $"Supported functions: {string.Join(", ", SupportedFunctions.Order())}.",
                nameof(functionName));
        }

        var clrType = Nullable.GetUnderlyingType(typeof(TProperty)) ?? typeof(TProperty);

        if (!TypeMappings.TryGetValue(clrType, out var storeType))
        {
            throw new NotSupportedException(
                $"Type '{clrType.Name}' is not supported for SimpleAggregateFunction. " +
                $"Supported types: {string.Join(", ", TypeMappings.Keys.Select(t => t.Name))}.");
        }

        propertyBuilder.HasColumnType($"SimpleAggregateFunction({functionName}, {storeType})");
        return propertyBuilder;
    }

    /// <summary>
    /// Configures the property to use ClickHouse SimpleAggregateFunction type.
    /// </summary>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="functionName">The aggregate function name.</param>
    /// <returns>The property builder for chaining.</returns>
    public static PropertyBuilder HasSimpleAggregateFunction(
        this PropertyBuilder propertyBuilder,
        string functionName)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(functionName);

        if (!SupportedFunctions.Contains(functionName))
        {
            throw new ArgumentException(
                $"Unsupported SimpleAggregateFunction: '{functionName}'. " +
                $"Supported functions: {string.Join(", ", SupportedFunctions.Order())}.",
                nameof(functionName));
        }

        var clrType = propertyBuilder.Metadata.ClrType;
        var underlyingType = Nullable.GetUnderlyingType(clrType) ?? clrType;

        if (!TypeMappings.TryGetValue(underlyingType, out var storeType))
        {
            throw new NotSupportedException(
                $"Type '{underlyingType.Name}' is not supported for SimpleAggregateFunction. " +
                $"Supported types: {string.Join(", ", TypeMappings.Keys.Select(t => t.Name))}.");
        }

        propertyBuilder.HasColumnType($"SimpleAggregateFunction({functionName}, {storeType})");
        return propertyBuilder;
    }

    #region LowCardinality

    /// <summary>
    /// Configures the property to use ClickHouse LowCardinality storage optimization.
    /// </summary>
    /// <remarks>
    /// LowCardinality is a storage optimization for columns with low cardinality (typically &lt;10,000 unique values).
    /// It uses dictionary encoding which significantly reduces storage size and improves query performance
    /// for columns like status codes, country codes, category names, etc.
    ///
    /// ClickHouse automatically determines when to use LowCardinality based on column statistics,
    /// but you can explicitly request it with this method when you know the column has low cardinality.
    ///
    /// For nullable string properties, this method automatically uses LowCardinality(Nullable(String)).
    /// </remarks>
    /// <typeparam name="TProperty">The property type (string or string?).</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <returns>The property builder for chaining.</returns>
    /// <example>
    /// <code>
    /// modelBuilder.Entity&lt;Order&gt;(entity =>
    /// {
    ///     entity.Property(e => e.Status)
    ///         .HasLowCardinality();
    ///
    ///     entity.Property(e => e.CountryCode)
    ///         .HasLowCardinality();
    /// });
    /// </code>
    /// </example>
    public static PropertyBuilder<TProperty> HasLowCardinality<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        // Check if the property is nullable
        var isNullable = propertyBuilder.Metadata.IsNullable;
        var columnType = isNullable
            ? "LowCardinality(Nullable(String))"
            : "LowCardinality(String)";

        propertyBuilder.HasColumnType(columnType);
        return propertyBuilder;
    }

    /// <summary>
    /// Configures the property to use ClickHouse LowCardinality with FixedString storage.
    /// </summary>
    /// <remarks>
    /// FixedString is efficient for strings with a known, fixed maximum length like:
    /// - ISO country codes (2-3 characters)
    /// - Currency codes (3 characters)
    /// - Status codes with fixed length
    ///
    /// Combined with LowCardinality, this provides optimal storage for low-cardinality fixed-length strings.
    ///
    /// For nullable string properties, this method automatically uses LowCardinality(Nullable(FixedString(n))).
    /// </remarks>
    /// <typeparam name="TProperty">The property type (string or string?).</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="length">The fixed string length in bytes.</param>
    /// <returns>The property builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when length is less than 1.</exception>
    /// <example>
    /// <code>
    /// modelBuilder.Entity&lt;Country&gt;(entity =>
    /// {
    ///     entity.Property(e => e.IsoCode)
    ///         .HasLowCardinalityFixedString(2); // "US", "UK", "DE", etc.
    ///
    ///     entity.Property(e => e.CurrencyCode)
    ///         .HasLowCardinalityFixedString(3); // "USD", "EUR", "GBP", etc.
    /// });
    /// </code>
    /// </example>
    public static PropertyBuilder<TProperty> HasLowCardinalityFixedString<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        int length)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentOutOfRangeException.ThrowIfLessThan(length, 1);

        // Check if the property is nullable
        var isNullable = propertyBuilder.Metadata.IsNullable;
        var columnType = isNullable
            ? $"LowCardinality(Nullable(FixedString({length})))"
            : $"LowCardinality(FixedString({length}))";

        propertyBuilder.HasColumnType(columnType);
        return propertyBuilder;
    }

    #endregion
}
