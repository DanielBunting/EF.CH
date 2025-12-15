using System.Linq.Expressions;
using EF.CH.Dictionaries;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for defining ClickHouse dictionaries at the ModelBuilder level.
/// </summary>
public static class DictionaryExtensions
{
    /// <summary>
    /// Defines a ClickHouse dictionary with strongly-typed lambda configuration.
    /// This is an alternative to entity.AsDictionary&lt;TDict, TSource&gt;().
    /// </summary>
    /// <typeparam name="TDictionary">The dictionary entity type.</typeparam>
    /// <typeparam name="TSource">The source entity type.</typeparam>
    /// <param name="modelBuilder">The model builder.</param>
    /// <param name="dictionaryName">The dictionary name in ClickHouse.</param>
    /// <param name="configure">Action to configure the dictionary.</param>
    /// <returns>The model builder for chaining.</returns>
    /// <example>
    /// <code>
    /// modelBuilder.Dictionary&lt;CountryLookup, Country&gt;("country_dict", cfg => cfg
    ///     .PrimaryKey(c => c.Id)
    ///     .Attribute(c => c.Name)
    ///     .Attribute(c => c.IsoCode)
    ///     .Layout(DictionaryLayout.Hashed)
    ///     .Lifetime(300));
    /// </code>
    /// </example>
    public static ModelBuilder Dictionary<TDictionary, TSource>(
        this ModelBuilder modelBuilder,
        string dictionaryName,
        Action<StandaloneDictionaryBuilder<TDictionary, TSource>> configure)
        where TDictionary : class
        where TSource : class
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(dictionaryName);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new StandaloneDictionaryBuilder<TDictionary, TSource>(modelBuilder, dictionaryName);
        configure(builder);
        builder.Build();
        return modelBuilder;
    }
}

/// <summary>
/// Builder for configuring dictionaries using the standalone Dictionary&lt;T&gt;() API.
/// </summary>
/// <typeparam name="TDictionary">The dictionary entity type.</typeparam>
/// <typeparam name="TSource">The source entity type.</typeparam>
public sealed class StandaloneDictionaryBuilder<TDictionary, TSource>
    where TDictionary : class
    where TSource : class
{
    private readonly ModelBuilder _modelBuilder;
    private readonly string _dictionaryName;
    private readonly EntityTypeBuilder<TDictionary> _entityBuilder;
    private string[]? _keyColumns;
    private DictionaryLayout _layout = DictionaryLayout.Hashed;
    private Dictionary<string, object>? _layoutOptions;
    private int _lifetimeMinSeconds;
    private int _lifetimeMaxSeconds = 300;
    private Dictionary<string, object>? _defaults;

    internal StandaloneDictionaryBuilder(ModelBuilder modelBuilder, string dictionaryName)
    {
        _modelBuilder = modelBuilder;
        _dictionaryName = dictionaryName;
        _entityBuilder = modelBuilder.Entity<TDictionary>();
    }

    /// <summary>
    /// Configures a single-column primary key for the dictionary.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="keySelector">Expression selecting the key property.</param>
    /// <returns>The builder for chaining.</returns>
    public StandaloneDictionaryBuilder<TDictionary, TSource> PrimaryKey<TKey>(
        Expression<Func<TDictionary, TKey>> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        _keyColumns = [ExpressionExtensions.GetPropertyName(keySelector)];
        return this;
    }

    /// <summary>
    /// Configures a composite primary key for the dictionary.
    /// </summary>
    /// <param name="keySelector">Expression selecting the key properties.</param>
    /// <returns>The builder for chaining.</returns>
    public StandaloneDictionaryBuilder<TDictionary, TSource> CompositePrimaryKey(
        Expression<Func<TDictionary, object>> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        _keyColumns = ExpressionExtensions.GetPropertyNames(keySelector);
        return this;
    }

    /// <summary>
    /// Configures an attribute column in the dictionary.
    /// This method doesn't modify the entity - attributes are derived from entity properties.
    /// </summary>
    /// <typeparam name="TValue">The attribute type.</typeparam>
    /// <param name="attributeSelector">Expression selecting the attribute property.</param>
    /// <returns>The builder for chaining.</returns>
    public StandaloneDictionaryBuilder<TDictionary, TSource> Attribute<TValue>(
        Expression<Func<TDictionary, TValue>> attributeSelector)
    {
        // No-op for now - attributes are derived from entity properties
        // Could be extended to support column renames or transformations
        return this;
    }

    /// <summary>
    /// Configures the dictionary layout.
    /// </summary>
    /// <param name="layout">The layout type.</param>
    /// <returns>The builder for chaining.</returns>
    public StandaloneDictionaryBuilder<TDictionary, TSource> Layout(DictionaryLayout layout)
    {
        _layout = layout;
        return this;
    }

    /// <summary>
    /// Configures the dictionary to use Hashed layout.
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public StandaloneDictionaryBuilder<TDictionary, TSource> UseHashedLayout()
    {
        _layout = DictionaryLayout.Hashed;
        return this;
    }

    /// <summary>
    /// Configures the dictionary to use Flat layout.
    /// </summary>
    /// <param name="maxArraySize">Maximum array size for flat layout.</param>
    /// <returns>The builder for chaining.</returns>
    public StandaloneDictionaryBuilder<TDictionary, TSource> UseFlatLayout(int? maxArraySize = null)
    {
        _layout = DictionaryLayout.Flat;
        if (maxArraySize.HasValue)
        {
            _layoutOptions ??= new Dictionary<string, object>();
            _layoutOptions["max_array_size"] = maxArraySize.Value;
        }
        return this;
    }

    /// <summary>
    /// Configures the dictionary to use Cache layout.
    /// </summary>
    /// <param name="sizeInCells">The cache size in cells.</param>
    /// <returns>The builder for chaining.</returns>
    public StandaloneDictionaryBuilder<TDictionary, TSource> UseCacheLayout(int sizeInCells)
    {
        _layout = DictionaryLayout.Cache;
        _layoutOptions ??= new Dictionary<string, object>();
        _layoutOptions["size_in_cells"] = sizeInCells;
        return this;
    }

    /// <summary>
    /// Configures the dictionary lifetime (auto-refresh interval).
    /// </summary>
    /// <param name="seconds">The lifetime in seconds.</param>
    /// <returns>The builder for chaining.</returns>
    public StandaloneDictionaryBuilder<TDictionary, TSource> Lifetime(int seconds)
    {
        _lifetimeMinSeconds = 0;
        _lifetimeMaxSeconds = seconds;
        return this;
    }

    /// <summary>
    /// Configures the dictionary lifetime with min and max values.
    /// </summary>
    /// <param name="minSeconds">The minimum lifetime in seconds.</param>
    /// <param name="maxSeconds">The maximum lifetime in seconds.</param>
    /// <returns>The builder for chaining.</returns>
    public StandaloneDictionaryBuilder<TDictionary, TSource> Lifetime(int minSeconds, int maxSeconds)
    {
        _lifetimeMinSeconds = minSeconds;
        _lifetimeMaxSeconds = maxSeconds;
        return this;
    }

    /// <summary>
    /// Configures the dictionary for manual reload only (LIFETIME 0).
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public StandaloneDictionaryBuilder<TDictionary, TSource> ManualReloadOnly()
    {
        _lifetimeMinSeconds = 0;
        _lifetimeMaxSeconds = 0;
        return this;
    }

    /// <summary>
    /// Configures a default value for a dictionary attribute.
    /// </summary>
    /// <typeparam name="TValue">The attribute type.</typeparam>
    /// <param name="attributeSelector">Expression selecting the attribute.</param>
    /// <param name="defaultValue">The default value.</param>
    /// <returns>The builder for chaining.</returns>
    public StandaloneDictionaryBuilder<TDictionary, TSource> HasDefault<TValue>(
        Expression<Func<TDictionary, TValue>> attributeSelector,
        TValue defaultValue)
    {
        ArgumentNullException.ThrowIfNull(attributeSelector);
        var propertyName = ExpressionExtensions.GetPropertyName(attributeSelector);
        _defaults ??= new Dictionary<string, object>();
        _defaults[propertyName] = defaultValue!;
        return this;
    }

    /// <summary>
    /// Applies the configuration to the entity type.
    /// </summary>
    internal void Build()
    {
        if (_keyColumns == null || _keyColumns.Length == 0)
        {
            throw new InvalidOperationException(
                $"Dictionary '{_dictionaryName}' must have a primary key defined. " +
                "Call PrimaryKey() or CompositePrimaryKey().");
        }

        // Set the table name to match the dictionary name
        _entityBuilder.ToTable(_dictionaryName);

        // Dictionaries are keyless in EF Core (no change tracking)
        _entityBuilder.HasNoKey();

        // Mark as dictionary
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.Dictionary, true);

        // Source table is the source entity type's table
        var sourceTableName = typeof(TSource).Name.ToSnakeCase();
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.DictionarySource, sourceTableName);
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.DictionarySourceType, typeof(TSource));

        // Key columns
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns, _keyColumns);

        // Layout
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.DictionaryLayout, _layout);
        if (_layoutOptions != null)
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.DictionaryLayoutOptions, _layoutOptions);
        }

        // Lifetime
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMin, _lifetimeMinSeconds);
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMax, _lifetimeMaxSeconds);

        // Defaults
        if (_defaults != null)
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.DictionaryDefaults, _defaults);
        }
    }
}

internal static class StringExtensions
{
    internal static string ToSnakeCase(this string str)
    {
        return string.Concat(str.Select((c, i) =>
            i > 0 && char.IsUpper(c) ? "_" + char.ToLower(c) : char.ToLower(c).ToString()));
    }
}
