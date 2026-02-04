using System.Linq.Expressions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.External;

/// <summary>
/// Builder for configuring external URL entities that use the url() table function.
/// External entities do not create ClickHouse tables - they query HTTP/HTTPS URLs directly via table functions.
/// </summary>
/// <typeparam name="TEntity">The entity type.</typeparam>
public class ExternalUrlEntityBuilder<TEntity> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _entityBuilder;
    private readonly UrlConfig _config = new();

    internal ExternalUrlEntityBuilder(ModelBuilder modelBuilder)
    {
        _entityBuilder = modelBuilder.Entity<TEntity>();
    }

    /// <summary>
    /// Specifies the URL to fetch data from.
    /// </summary>
    /// <param name="url">The HTTP/HTTPS URL.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalUrlEntityBuilder<TEntity> FromUrl(string url)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(url);
        _config.Url = url;
        return this;
    }

    /// <summary>
    /// Specifies the URL from an environment variable.
    /// </summary>
    /// <param name="envVar">The environment variable name containing the URL.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalUrlEntityBuilder<TEntity> FromUrlEnv(string envVar)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(envVar);
        _config.UrlEnv = envVar;
        return this;
    }

    /// <summary>
    /// Specifies the data format (e.g., "CSV", "JSONEachRow", "TSV").
    /// </summary>
    /// <param name="format">The ClickHouse format name.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalUrlEntityBuilder<TEntity> WithFormat(string format)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(format);
        _config.Format = format;
        return this;
    }

    /// <summary>
    /// Specifies the data structure explicitly.
    /// If not specified, structure is inferred from entity properties.
    /// </summary>
    /// <param name="structure">The ClickHouse structure definition (e.g., "id UInt64, name String").</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalUrlEntityBuilder<TEntity> WithStructure(string structure)
    {
        _config.Structure = structure;
        return this;
    }

    /// <summary>
    /// Specifies the compression type (e.g., "gzip", "none").
    /// </summary>
    /// <param name="compression">The compression type.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalUrlEntityBuilder<TEntity> WithCompression(string compression)
    {
        _config.Compression = compression;
        return this;
    }

    /// <summary>
    /// Adds HTTP headers to the request.
    /// </summary>
    /// <param name="headers">Dictionary of header name-value pairs.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalUrlEntityBuilder<TEntity> WithHeaders(IDictionary<string, string> headers)
    {
        ArgumentNullException.ThrowIfNull(headers);
        _config.Headers = new Dictionary<string, string>(headers);
        return this;
    }

    /// <summary>
    /// Adds a single HTTP header to the request.
    /// </summary>
    /// <param name="name">The header name.</param>
    /// <param name="value">The header value.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalUrlEntityBuilder<TEntity> WithHeader(string name, string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        _config.Headers ??= new Dictionary<string, string>();
        _config.Headers[name] = value;
        return this;
    }

    /// <summary>
    /// Configures a property mapping with optional customization.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyExpression">Expression selecting the property.</param>
    /// <param name="configure">Optional action to configure the property.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalUrlEntityBuilder<TEntity> Property<TProperty>(
        Expression<Func<TEntity, TProperty>> propertyExpression,
        Action<PropertyBuilder<TProperty>>? configure = null)
    {
        var propBuilder = _entityBuilder.Property(propertyExpression);
        configure?.Invoke(propBuilder);
        return this;
    }

    /// <summary>
    /// Ignores a property (won't be included in structure).
    /// </summary>
    /// <param name="propertyName">The name of the property to ignore.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalUrlEntityBuilder<TEntity> Ignore(string propertyName)
    {
        _entityBuilder.Ignore(propertyName);
        return this;
    }

    /// <summary>
    /// Applies the configuration to the entity type.
    /// Called automatically by ExternalUrlEntity extension method.
    /// </summary>
    internal void Build()
    {
        _entityBuilder.HasNoKey();

        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction, true);
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalProvider, "url");
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalReadOnly, true);

        if (!string.IsNullOrEmpty(_config.Url))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalUrl, _config.Url);
        }

        if (!string.IsNullOrEmpty(_config.UrlEnv))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalUrlEnv, _config.UrlEnv);
        }

        if (!string.IsNullOrEmpty(_config.Format))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalUrlFormat, _config.Format);
        }

        if (!string.IsNullOrEmpty(_config.Structure))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalUrlStructure, _config.Structure);
        }

        if (!string.IsNullOrEmpty(_config.Compression))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalUrlCompression, _config.Compression);
        }

        if (_config.Headers is { Count: > 0 })
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalUrlHeaders, _config.Headers);
        }
    }

    private class UrlConfig
    {
        public string? Url { get; set; }
        public string? UrlEnv { get; set; }
        public string? Format { get; set; }
        public string? Structure { get; set; }
        public string? Compression { get; set; }
        public Dictionary<string, string>? Headers { get; set; }
    }
}
