using System.Linq.Expressions;
using EF.CH.Dictionaries.Sources;
using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Dictionaries;

/// <summary>
/// Fluent configuration builder for ClickHouse dictionaries with external sources (PostgreSQL, MySQL, HTTP).
/// Unlike DictionaryConfiguration&lt;TDictionary, TSource&gt;, this does not require a source entity type
/// since the source is an external database/endpoint.
/// </summary>
/// <typeparam name="TDictionary">The dictionary entity type.</typeparam>
public sealed class ExternalDictionaryConfiguration<TDictionary>
    where TDictionary : class
{
    private readonly EntityTypeBuilder<TDictionary> _builder;
    private string? _name;
    private string[]? _keyColumns;
    private DictionaryLayout _layout = DictionaryLayout.Hashed;
    private Dictionary<string, object>? _layoutOptions;
    private int _lifetimeMinSeconds;
    private int _lifetimeMaxSeconds = 300;
    private Dictionary<string, object>? _defaults;

    // External source configuration (only one can be set)
    private string? _sourceProvider;
    private PostgresDictionarySourceConfig? _postgresConfig;
    private MySqlDictionarySourceConfig? _mysqlConfig;
    private HttpDictionarySourceConfig? _httpConfig;

    internal ExternalDictionaryConfiguration(EntityTypeBuilder<TDictionary> builder)
    {
        _builder = builder;
    }

    /// <summary>
    /// Specifies a custom name for the dictionary.
    /// If not called, the name is derived from the entity type name.
    /// </summary>
    /// <param name="dictionaryName">The dictionary name.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public ExternalDictionaryConfiguration<TDictionary> HasName(string dictionaryName)
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
    public ExternalDictionaryConfiguration<TDictionary> HasKey<TKey>(
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
    public ExternalDictionaryConfiguration<TDictionary> HasCompositeKey(
        Expression<Func<TDictionary, object>> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        _keyColumns = ExpressionExtensions.GetPropertyNames(keySelector);
        return this;
    }

    #region External Source Configuration

    /// <summary>
    /// Configures PostgreSQL as the dictionary source.
    /// </summary>
    /// <param name="configure">Action to configure the PostgreSQL source.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public ExternalDictionaryConfiguration<TDictionary> FromPostgreSql(
        Action<PostgresDictionarySourceBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        EnsureNoSourceConfigured();

        _sourceProvider = "postgresql";
        var builder = new PostgresDictionarySourceBuilder();
        configure(builder);
        _postgresConfig = builder.Build();
        return this;
    }

    /// <summary>
    /// Configures MySQL as the dictionary source.
    /// </summary>
    /// <param name="configure">Action to configure the MySQL source.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public ExternalDictionaryConfiguration<TDictionary> FromMySql(
        Action<MySqlDictionarySourceBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        EnsureNoSourceConfigured();

        _sourceProvider = "mysql";
        var builder = new MySqlDictionarySourceBuilder();
        configure(builder);
        _mysqlConfig = builder.Build();
        return this;
    }

    /// <summary>
    /// Configures HTTP as the dictionary source.
    /// </summary>
    /// <param name="configure">Action to configure the HTTP source.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public ExternalDictionaryConfiguration<TDictionary> FromHttp(
        Action<HttpDictionarySourceBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        EnsureNoSourceConfigured();

        _sourceProvider = "http";
        var builder = new HttpDictionarySourceBuilder();
        configure(builder);
        _httpConfig = builder.Build();
        return this;
    }

    private void EnsureNoSourceConfigured()
    {
        if (_sourceProvider != null)
        {
            throw new InvalidOperationException(
                $"Dictionary '{typeof(TDictionary).Name}' already has a source configured ({_sourceProvider}). " +
                "Only one source type can be specified per dictionary.");
        }
    }

    #endregion

    #region Layout Configuration

    /// <summary>
    /// Configures the dictionary layout.
    /// </summary>
    /// <param name="layout">The layout type.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public ExternalDictionaryConfiguration<TDictionary> UseLayout(DictionaryLayout layout)
    {
        _layout = layout;
        return this;
    }

    /// <summary>
    /// Configures the dictionary to use FLAT layout.
    /// </summary>
    /// <param name="configure">Optional configuration for layout options.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public ExternalDictionaryConfiguration<TDictionary> UseFlatLayout(
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
    public ExternalDictionaryConfiguration<TDictionary> UseHashedLayout(
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
    public ExternalDictionaryConfiguration<TDictionary> UseCacheLayout(
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

    #endregion

    #region Lifetime Configuration

    /// <summary>
    /// Configures the dictionary lifetime (refresh interval).
    /// </summary>
    /// <param name="seconds">The lifetime in seconds.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public ExternalDictionaryConfiguration<TDictionary> HasLifetime(int seconds)
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
    public ExternalDictionaryConfiguration<TDictionary> HasLifetime(int minSeconds, int maxSeconds)
    {
        _lifetimeMinSeconds = minSeconds;
        _lifetimeMaxSeconds = maxSeconds;
        return this;
    }

    /// <summary>
    /// Disables automatic refresh (LIFETIME(0)).
    /// </summary>
    /// <returns>The configuration builder for chaining.</returns>
    public ExternalDictionaryConfiguration<TDictionary> HasNoAutoRefresh()
    {
        _lifetimeMinSeconds = 0;
        _lifetimeMaxSeconds = 0;
        return this;
    }

    #endregion

    #region Default Values

    /// <summary>
    /// Configures a default value for a dictionary attribute.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="property">Expression selecting the property.</param>
    /// <param name="defaultValue">The default value.</param>
    /// <returns>The configuration builder for chaining.</returns>
    public ExternalDictionaryConfiguration<TDictionary> HasDefault<TProperty>(
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

    #endregion

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

        if (_sourceProvider == null)
        {
            throw new InvalidOperationException(
                $"Dictionary '{typeof(TDictionary).Name}' must have a source configured. " +
                "Use FromPostgreSql(), FromMySql(), or FromHttp().");
        }

        // Derive name from entity type if not specified
        var name = _name ?? ConvertToSnakeCase(typeof(TDictionary).Name);

        // Store common dictionary configuration as annotations
        _builder.HasAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        _builder.HasAnnotation(ClickHouseAnnotationNames.DictionarySourceProvider, _sourceProvider);
        _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns, _keyColumns);
        _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryLayout, _layout);
        _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMin, _lifetimeMinSeconds);
        _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMax, _lifetimeMaxSeconds);

        // Set the table name to the dictionary name
        _builder.ToTable(name);

        // Ensure all CLR properties are added to the entity type
        // This is necessary when using ModelBuilder without a database provider
        foreach (var clrProperty in typeof(TDictionary).GetProperties())
        {
            if (clrProperty.CanRead && clrProperty.CanWrite)
            {
                _builder.Property(clrProperty.PropertyType, clrProperty.Name);
            }
        }

        // Dictionaries are keyless in EF Core - they use ClickHouse dictionary keys, not EF primary keys
        _builder.HasNoKey();

        if (_layoutOptions != null)
        {
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryLayoutOptions, _layoutOptions);
        }

        if (_defaults != null)
        {
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryDefaults, _defaults);
        }

        // Apply source-specific configuration
        ApplySourceConfiguration();
    }

    private void ApplySourceConfiguration()
    {
        switch (_sourceProvider)
        {
            case "postgresql":
                ApplyPostgresConfiguration();
                break;
            case "mysql":
                ApplyMySqlConfiguration();
                break;
            case "http":
                ApplyHttpConfiguration();
                break;
        }
    }

    private void ApplyPostgresConfiguration()
    {
        if (_postgresConfig == null) return;

        if (!string.IsNullOrEmpty(_postgresConfig.Table))
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryExternalTable, _postgresConfig.Table);
        if (!string.IsNullOrEmpty(_postgresConfig.Schema))
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryExternalSchema, _postgresConfig.Schema);
        if (!string.IsNullOrEmpty(_postgresConfig.WhereClause))
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryExternalWhere, _postgresConfig.WhereClause);
        if (!string.IsNullOrEmpty(_postgresConfig.InvalidateQuery))
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryInvalidateQuery, _postgresConfig.InvalidateQuery);

        ApplyConnectionConfiguration(_postgresConfig.Connection);
    }

    private void ApplyMySqlConfiguration()
    {
        if (_mysqlConfig == null) return;

        if (!string.IsNullOrEmpty(_mysqlConfig.Table))
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryExternalTable, _mysqlConfig.Table);
        if (!string.IsNullOrEmpty(_mysqlConfig.WhereClause))
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryExternalWhere, _mysqlConfig.WhereClause);
        if (!string.IsNullOrEmpty(_mysqlConfig.InvalidateQuery))
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryInvalidateQuery, _mysqlConfig.InvalidateQuery);
        if (_mysqlConfig.FailOnConnectionLoss.HasValue)
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryMySqlFailOnConnectionLoss, _mysqlConfig.FailOnConnectionLoss.Value);

        ApplyConnectionConfiguration(_mysqlConfig.Connection);
    }

    private void ApplyHttpConfiguration()
    {
        if (_httpConfig == null) return;

        if (!string.IsNullOrEmpty(_httpConfig.UrlEnv))
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryHttpUrl + "Env", _httpConfig.UrlEnv);
        if (!string.IsNullOrEmpty(_httpConfig.UrlValue))
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryHttpUrl, _httpConfig.UrlValue);
        if (!string.IsNullOrEmpty(_httpConfig.Format))
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryHttpFormat, _httpConfig.Format);

        // Store headers
        if (_httpConfig.Headers.Count > 0)
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryHttpHeaders, _httpConfig.Headers);
        if (_httpConfig.HeadersEnv.Count > 0)
            _builder.HasAnnotation(ClickHouseAnnotationNames.DictionaryHttpHeaders + "Env", _httpConfig.HeadersEnv);

        // HTTP credentials
        if (!string.IsNullOrEmpty(_httpConfig.UserEnv))
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalUserEnv, _httpConfig.UserEnv);
        if (!string.IsNullOrEmpty(_httpConfig.UserValue))
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalUserValue, _httpConfig.UserValue);
        if (!string.IsNullOrEmpty(_httpConfig.PasswordEnv))
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalPasswordEnv, _httpConfig.PasswordEnv);
        if (!string.IsNullOrEmpty(_httpConfig.PasswordValue))
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalPasswordValue, _httpConfig.PasswordValue);

        // Profile
        if (!string.IsNullOrEmpty(_httpConfig.ProfileName))
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalConnectionProfile, _httpConfig.ProfileName);
    }

    private void ApplyConnectionConfiguration(DictionaryConnectionConfig config)
    {
        // Profile takes precedence
        if (!string.IsNullOrEmpty(config.ProfileName))
        {
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalConnectionProfile, config.ProfileName);
            return;
        }

        // Host/port configuration
        if (!string.IsNullOrEmpty(config.HostPortEnv))
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalHostPortEnv, config.HostPortEnv);
        if (!string.IsNullOrEmpty(config.HostPortValue))
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalHostPortValue, config.HostPortValue);

        // Separate host/port (stored with same annotation names but will be resolved differently)
        if (!string.IsNullOrEmpty(config.HostEnv))
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalHostPortEnv + ":Host", config.HostEnv);
        if (!string.IsNullOrEmpty(config.HostValue))
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalHostPortValue + ":Host", config.HostValue);
        if (!string.IsNullOrEmpty(config.PortEnv))
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalHostPortEnv + ":Port", config.PortEnv);
        if (config.PortValue.HasValue)
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalHostPortValue + ":Port", config.PortValue.Value);

        // Database
        if (!string.IsNullOrEmpty(config.DatabaseEnv))
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalDatabaseEnv, config.DatabaseEnv);
        if (!string.IsNullOrEmpty(config.DatabaseValue))
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalDatabaseValue, config.DatabaseValue);

        // Credentials
        if (!string.IsNullOrEmpty(config.UserEnv))
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalUserEnv, config.UserEnv);
        if (!string.IsNullOrEmpty(config.UserValue))
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalUserValue, config.UserValue);
        if (!string.IsNullOrEmpty(config.PasswordEnv))
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalPasswordEnv, config.PasswordEnv);
        if (!string.IsNullOrEmpty(config.PasswordValue))
            _builder.HasAnnotation(ClickHouseAnnotationNames.ExternalPasswordValue, config.PasswordValue);
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
