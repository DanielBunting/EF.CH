using System.Linq.Expressions;
using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Dictionaries;

/// <summary>
/// Fluent configuration builder for ClickHouse dictionaries.
/// </summary>
/// <typeparam name="TDictionary">The dictionary entity type.</typeparam>
/// <typeparam name="TSource">The source entity type.</typeparam>
public sealed class DictionaryConfiguration<TDictionary, TSource>
    where TDictionary : class
    where TSource : class
{
    private readonly EntityTypeBuilder<TDictionary> _builder;
    private string? _name;
    private string[]? _keyColumns;
    private DictionaryLayout _layout = DictionaryLayout.Hashed;
    private Dictionary<string, object>? _layoutOptions;
    private int _lifetimeMinSeconds;
    private int _lifetimeMaxSeconds = 300;
    private Dictionary<string, object>? _defaults;
    private LambdaExpression? _projectionExpression;
    private LambdaExpression? _filterExpression;

    internal DictionaryConfiguration(EntityTypeBuilder<TDictionary> builder)
    {
        _builder = builder;
    }

    /// <summary>
    /// Specifies a custom name for the dictionary.
    /// If not called, the name is derived from the entity type name.
    /// </summary>
    /// <param name="dictionaryName">The dictionary name.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public DictionaryConfiguration<TDictionary, TSource> HasName(string dictionaryName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dictionaryName);
        _name = dictionaryName;
        return this;
    }

    /// <summary>
    /// Configures a single-column key for the dictionary.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="keySelector">Expression selecting the key property.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public DictionaryConfiguration<TDictionary, TSource> HasKey<TKey>(
        Expression<Func<TDictionary, TKey>> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        _keyColumns = [ExpressionExtensions.GetPropertyName(keySelector)];
        return this;
    }

    /// <summary>
    /// Configures a composite key for the dictionary.
    /// </summary>
    /// <param name="keySelector">Expression selecting the key properties.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public DictionaryConfiguration<TDictionary, TSource> HasCompositeKey(
        Expression<Func<TDictionary, object>> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        _keyColumns = ExpressionExtensions.GetPropertyNames(keySelector);
        return this;
    }

    /// <summary>
    /// Configures the dictionary to source from the table with all matching columns.
    /// </summary>
    /// <returns>The configuration builder for chaining.</returns>
    public DictionaryConfiguration<TDictionary, TSource> FromTable()
    {
        // No projection or filter - use all columns from source
        return this;
    }

    /// <summary>
    /// Configures the dictionary to source from the table with a projection.
    /// </summary>
    /// <param name="projection">Expression projecting source columns to dictionary columns.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public DictionaryConfiguration<TDictionary, TSource> FromTable(
        Expression<Func<TSource, TDictionary>> projection)
    {
        ArgumentNullException.ThrowIfNull(projection);
        _projectionExpression = projection;
        return this;
    }

    /// <summary>
    /// Configures the dictionary to source from the table with a filter.
    /// </summary>
    /// <param name="filter">Expression filtering the source query.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public DictionaryConfiguration<TDictionary, TSource> FromTable(
        Expression<Func<IQueryable<TSource>, IQueryable<TSource>>> filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        _filterExpression = filter;
        return this;
    }

    /// <summary>
    /// Configures the dictionary to source from the table with projection and filter.
    /// </summary>
    /// <param name="projection">Expression projecting source columns to dictionary columns.</param>
    /// <param name="filter">Expression filtering the source query.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public DictionaryConfiguration<TDictionary, TSource> FromTable(
        Expression<Func<TSource, TDictionary>> projection,
        Expression<Func<IQueryable<TSource>, IQueryable<TSource>>> filter)
    {
        ArgumentNullException.ThrowIfNull(projection);
        ArgumentNullException.ThrowIfNull(filter);
        _projectionExpression = projection;
        _filterExpression = filter;
        return this;
    }

    /// <summary>
    /// Configures the dictionary layout.
    /// </summary>
    /// <param name="layout">The layout type.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public DictionaryConfiguration<TDictionary, TSource> UseLayout(DictionaryLayout layout)
    {
        _layout = layout;
        return this;
    }

    /// <summary>
    /// Configures the dictionary to use FLAT layout.
    /// </summary>
    /// <param name="configure">Optional configuration for layout options.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public DictionaryConfiguration<TDictionary, TSource> UseFlatLayout(
        Action<FlatLayoutOptions>? configure = null)
    {
        _layout = DictionaryLayout.Flat;
        if (configure != null)
        {
            var options = new FlatLayoutOptions();
            configure(options);
            _layoutOptions = [];
            if (options.MaxArraySize.HasValue)
                _layoutOptions["max_array_size"] = options.MaxArraySize.Value;
        }
        return this;
    }

    /// <summary>
    /// Configures the dictionary to use HASHED layout.
    /// </summary>
    /// <param name="configure">Optional configuration for layout options.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public DictionaryConfiguration<TDictionary, TSource> UseHashedLayout(
        Action<HashedLayoutOptions>? configure = null)
    {
        _layout = DictionaryLayout.Hashed;
        if (configure != null)
        {
            var options = new HashedLayoutOptions();
            configure(options);
            _layoutOptions = [];
            if (options.ShardCount.HasValue)
                _layoutOptions["shards"] = options.ShardCount.Value;
            if (options.Sparse.HasValue)
                _layoutOptions["sparse"] = options.Sparse.Value;
        }
        return this;
    }

    /// <summary>
    /// Configures the dictionary to use CACHE layout.
    /// </summary>
    /// <param name="configure">Optional configuration for cache options. If not provided, uses default size.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public DictionaryConfiguration<TDictionary, TSource> UseCacheLayout(
        Action<CacheLayoutOptions>? configure = null)
    {
        _layout = DictionaryLayout.Cache;
        if (configure != null)
        {
            var options = new CacheLayoutOptions();
            configure(options);
            _layoutOptions = new Dictionary<string, object>
            {
                ["size_in_cells"] = options.SizeInCells
            };
        }
        return this;
    }

    /// <summary>
    /// Configures the dictionary lifetime (refresh interval).
    /// </summary>
    /// <param name="seconds">The lifetime in seconds.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public DictionaryConfiguration<TDictionary, TSource> HasLifetime(int seconds)
    {
        _lifetimeMinSeconds = 0;
        _lifetimeMaxSeconds = seconds;
        return this;
    }

    /// <summary>
    /// Configures the dictionary lifetime with min/max range.
    /// </summary>
    /// <param name="minSeconds">Minimum lifetime in seconds.</param>
    /// <param name="maxSeconds">Maximum lifetime in seconds.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public DictionaryConfiguration<TDictionary, TSource> HasLifetime(int minSeconds, int maxSeconds)
    {
        _lifetimeMinSeconds = minSeconds;
        _lifetimeMaxSeconds = maxSeconds;
        return this;
    }

    /// <summary>
    /// Disables automatic refresh (LIFETIME(0)).
    /// </summary>
    /// <returns>The configuration builder for chaining.</returns>
    public DictionaryConfiguration<TDictionary, TSource> HasNoAutoRefresh()
    {
        _lifetimeMinSeconds = 0;
        _lifetimeMaxSeconds = 0;
        return this;
    }

    /// <summary>
    /// Configures a default value for a dictionary attribute.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="property">Expression selecting the property.</param>
    /// <param name="defaultValue">The default value.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public DictionaryConfiguration<TDictionary, TSource> HasDefault<TProperty>(
        Expression<Func<TDictionary, TProperty>> property,
        TProperty defaultValue)
    {
        ArgumentNullException.ThrowIfNull(property);
        ArgumentNullException.ThrowIfNull(defaultValue);

        var propertyName = ExpressionExtensions.GetPropertyName(property);
        _defaults ??= [];
        _defaults[propertyName] = defaultValue;
        return this;
    }

    /// <summary>
    /// Applies the configuration to the entity type builder.
    /// Called internally when the configuration action completes.
    /// </summary>
    internal void Apply()
    {
        if (_keyColumns == null || _keyColumns.Length == 0)
        {
            throw new InvalidOperationException(
                $"Dictionary '{typeof(TDictionary).Name}' must have HasKey() or HasCompositeKey() configured.");
        }

        // Derive name from entity type if not specified
        var name = _name ?? ConvertToSnakeCase(typeof(TDictionary).Name);

        // Store all configuration as annotations
        _builder.HasAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        _builder.HasAnnotation(ClickHouseAnnotationNames.DictionarySource, GetSourceTableName());
        _builder.HasAnnotation(ClickHouseAnnotationNames.DictionarySourceType, typeof(TSource));
        _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns, _keyColumns);
        _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryLayout, _layout);
        _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMin, _lifetimeMinSeconds);
        _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMax, _lifetimeMaxSeconds);

        // Set the table name to the dictionary name
        _builder.ToTable(name);

        if (_layoutOptions != null)
        {
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryLayoutOptions, _layoutOptions);
        }

        if (_defaults != null)
        {
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryDefaults, _defaults);
        }

        if (_projectionExpression != null)
        {
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryProjectionExpression, _projectionExpression);
        }

        if (_filterExpression != null)
        {
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryFilterExpression, _filterExpression);
        }
    }

    private static string GetSourceTableName()
    {
        // Convert source type name to snake_case for table name
        return ConvertToSnakeCase(typeof(TSource).Name);
    }

    private static string ConvertToSnakeCase(string name)
    {
        if (string.IsNullOrEmpty(name))
            return name;

        var result = new System.Text.StringBuilder();
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                if (i > 0)
                    result.Append('_');
                result.Append(char.ToLowerInvariant(c));
            }
            else
            {
                result.Append(c);
            }
        }
        return result.ToString();
    }
}
