using EF.CH.Metadata;
using EF.CH.Storage.Internal;
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

    #region Default For Null

    /// <summary>
    /// Configures this nullable property to store a default value instead of NULL.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ClickHouse has performance overhead for Nullable(T) columns due to the additional
    /// bitmask required to track null values. This method allows you to use a default value
    /// (e.g., 0, empty string, Guid.Empty) instead of NULL.
    /// </para>
    /// <para>
    /// The column will be generated as non-nullable with this value as the DEFAULT.
    /// </para>
    /// <para>
    /// <b>WARNING:</b> The default value cannot be distinguished from NULL. If you store
    /// the default value explicitly, it will be read back as null. Choose a default that
    /// is never a valid business value for this column.
    /// </para>
    /// <para>
    /// <b>Aggregation behavior:</b> Aggregate functions (AVG, SUM, MIN, MAX) automatically
    /// exclude rows where the column equals the default value, treating them as null.
    /// </para>
    /// <para>
    /// <b>Known limitations:</b>
    /// </para>
    /// <list type="bullet">
    ///   <item>
    ///     <description>
    ///       <b>Use <c>== null</c> instead of <c>.HasValue</c>:</b> The <c>.HasValue</c> property
    ///       is optimized away by EF Core since the database column is non-nullable.
    ///       Use <c>e.Score == null</c> instead of <c>e.Score.HasValue</c>.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       <b>Use conditional instead of <c>??</c> coalesce:</b> The null-coalescing operator
    ///       won't work because ClickHouse stores the default value (not NULL).
    ///       Use <c>e.Score == null ? fallback : e.Score</c> instead of <c>e.Score ?? fallback</c>.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       <b>Raw SQL bypasses conversion:</b> When using raw SQL queries, you'll get
    ///       the stored default value (e.g., 0), not null. The value converter only applies
    ///       to LINQ queries.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       <b>Comparing to the default value matches "null" rows:</b> A query like
    ///       <c>Where(e => e.Score == 0)</c> will match both rows where Score was explicitly
    ///       set to 0 and rows where Score is null.
    ///     </description>
    ///   </item>
    /// </list>
    /// </remarks>
    /// <typeparam name="TProperty">The property type (must be nullable).</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="defaultValue">The default value to represent null. This value will be stored
    /// in the database instead of NULL, and will be converted back to null when read.</param>
    /// <returns>The property builder for chaining.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when called on a non-nullable property.
    /// </exception>
    /// <example>
    /// <code>
    /// modelBuilder.Entity&lt;Order&gt;(entity =>
    /// {
    ///     // Use 0 as default for nullable int
    ///     entity.Property(e => e.DiscountPercent)
    ///         .HasDefaultForNull(0);
    ///
    ///     // Use empty string as default for nullable string
    ///     entity.Property(e => e.Notes)
    ///         .HasDefaultForNull("");
    ///
    ///     // Use Guid.Empty as default for nullable Guid
    ///     entity.Property(e => e.ExternalId)
    ///         .HasDefaultForNull(Guid.Empty);
    /// });
    ///
    /// // Queries work transparently:
    /// context.Orders.Where(o => o.DiscountPercent == null)
    /// // Translates to: WHERE DiscountPercent = 0
    ///
    /// // Use conditional instead of coalesce:
    /// context.Orders.Select(o => o.DiscountPercent == null ? 0 : o.DiscountPercent.Value)
    /// // NOT: context.Orders.Select(o => o.DiscountPercent ?? 0)
    /// </code>
    /// </example>
    public static PropertyBuilder<TProperty> HasDefaultForNull<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        TProperty defaultValue)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        var clrType = propertyBuilder.Metadata.ClrType;
        var isNullableValueType = Nullable.GetUnderlyingType(clrType) != null;
        var isNullableReferenceType = !clrType.IsValueType;

        if (!isNullableValueType && !isNullableReferenceType)
        {
            throw new InvalidOperationException(
                $"HasDefaultForNull can only be used on nullable properties. " +
                $"Property '{propertyBuilder.Metadata.Name}' is of non-nullable type '{clrType.Name}'.");
        }

        // Store the default value as an annotation
        propertyBuilder.HasAnnotation(ClickHouseAnnotationNames.DefaultForNull, defaultValue);

        // Apply the appropriate value converter
        var converter = DefaultForNullValueConverterFactory.Create(clrType, defaultValue!);
        if (converter != null)
        {
            propertyBuilder.HasConversion(converter);
        }

        return propertyBuilder;
    }

    #endregion

    #region Computed Columns

    /// <summary>
    /// Configures the property as a MATERIALIZED column.
    /// The expression is computed on INSERT and stored on disk.
    /// </summary>
    /// <remarks>
    /// <para>
    /// MATERIALIZED columns are not returned by <c>SELECT *</c> by default.
    /// Use explicit column selection to read them.
    /// </para>
    /// <para>
    /// This method also sets <c>ValueGeneratedOnAdd</c> to exclude the column from INSERTs,
    /// since ClickHouse computes the value.
    /// </para>
    /// </remarks>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="expression">ClickHouse SQL expression (e.g., "Amount * 1.1", "toYear(CreatedAt)")</param>
    /// <returns>The property builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="expression"/> is null or whitespace.</exception>
    /// <example>
    /// <code>
    /// modelBuilder.Entity&lt;Order&gt;(entity =>
    /// {
    ///     entity.Property(e => e.TotalWithTax)
    ///         .IsMaterialized("Amount * 1.1");
    ///
    ///     entity.Property(e => e.OrderYear)
    ///         .IsMaterialized("toYear(OrderDate)");
    /// });
    /// </code>
    /// </example>
    public static PropertyBuilder<TProperty> IsMaterialized<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        string expression)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);

        propertyBuilder.HasAnnotation(ClickHouseAnnotationNames.MaterializedExpression, expression);
        propertyBuilder.ValueGeneratedOnAdd();
        return propertyBuilder;
    }

    /// <summary>
    /// Configures the property as a MATERIALIZED column.
    /// </summary>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="expression">ClickHouse SQL expression.</param>
    /// <returns>The property builder for chaining.</returns>
    public static PropertyBuilder IsMaterialized(
        this PropertyBuilder propertyBuilder,
        string expression)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);

        propertyBuilder.HasAnnotation(ClickHouseAnnotationNames.MaterializedExpression, expression);
        propertyBuilder.ValueGeneratedOnAdd();
        return propertyBuilder;
    }

    /// <summary>
    /// Configures the property as an ALIAS column.
    /// The expression is computed at query time and not stored.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ALIAS columns cannot be inserted into and have no storage cost.
    /// They are computed on every read.
    /// </para>
    /// <para>
    /// This method also sets <c>ValueGeneratedOnAddOrUpdate</c> to exclude the column
    /// from all modifications.
    /// </para>
    /// </remarks>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="expression">ClickHouse SQL expression (e.g., "concat(FirstName, ' ', LastName)")</param>
    /// <returns>The property builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="expression"/> is null or whitespace.</exception>
    /// <example>
    /// <code>
    /// modelBuilder.Entity&lt;Person&gt;(entity =>
    /// {
    ///     entity.Property(e => e.FullName)
    ///         .IsAlias("concat(FirstName, ' ', LastName)");
    ///
    ///     entity.Property(e => e.DoubleValue)
    ///         .IsAlias("Value * 2");
    /// });
    /// </code>
    /// </example>
    public static PropertyBuilder<TProperty> IsAlias<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        string expression)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);

        propertyBuilder.HasAnnotation(ClickHouseAnnotationNames.AliasExpression, expression);
        propertyBuilder.ValueGeneratedOnAddOrUpdate();
        return propertyBuilder;
    }

    /// <summary>
    /// Configures the property as an ALIAS column.
    /// </summary>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="expression">ClickHouse SQL expression.</param>
    /// <returns>The property builder for chaining.</returns>
    public static PropertyBuilder IsAlias(
        this PropertyBuilder propertyBuilder,
        string expression)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);

        propertyBuilder.HasAnnotation(ClickHouseAnnotationNames.AliasExpression, expression);
        propertyBuilder.ValueGeneratedOnAddOrUpdate();
        return propertyBuilder;
    }

    /// <summary>
    /// Configures a DEFAULT expression for the column.
    /// The expression is computed if no value is provided on INSERT.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is different from EF Core's <c>DefaultValueSql</c> in that it uses
    /// ClickHouse-specific SQL syntax and functions.
    /// </para>
    /// <para>
    /// Unlike MATERIALIZED columns, DEFAULT columns can be explicitly set during INSERT.
    /// The expression is only evaluated when no value is provided.
    /// </para>
    /// </remarks>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="expression">ClickHouse SQL expression (e.g., "now()", "generateUUIDv4()")</param>
    /// <returns>The property builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="expression"/> is null or whitespace.</exception>
    /// <example>
    /// <code>
    /// modelBuilder.Entity&lt;Event&gt;(entity =>
    /// {
    ///     entity.Property(e => e.CreatedAt)
    ///         .HasDefaultExpression("now()");
    ///
    ///     entity.Property(e => e.TraceId)
    ///         .HasDefaultExpression("generateUUIDv4()");
    /// });
    /// </code>
    /// </example>
    public static PropertyBuilder<TProperty> HasDefaultExpression<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        string expression)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);

        propertyBuilder.HasAnnotation(ClickHouseAnnotationNames.DefaultExpression, expression);
        return propertyBuilder;
    }

    /// <summary>
    /// Configures a DEFAULT expression for the column.
    /// </summary>
    /// <param name="propertyBuilder">The property builder.</param>
    /// <param name="expression">ClickHouse SQL expression.</param>
    /// <returns>The property builder for chaining.</returns>
    public static PropertyBuilder HasDefaultExpression(
        this PropertyBuilder propertyBuilder,
        string expression)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);

        propertyBuilder.HasAnnotation(ClickHouseAnnotationNames.DefaultExpression, expression);
        return propertyBuilder;
    }

    #endregion
}
