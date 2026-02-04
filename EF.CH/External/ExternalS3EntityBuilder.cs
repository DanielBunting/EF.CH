using System.Linq.Expressions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.External;

/// <summary>
/// Builder for configuring external S3 entities that use the s3() table function.
/// External entities do not create ClickHouse tables - they query S3 directly via table functions.
/// </summary>
/// <typeparam name="TEntity">The entity type.</typeparam>
public class ExternalS3EntityBuilder<TEntity> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _entityBuilder;
    private readonly S3Config _config = new();

    internal ExternalS3EntityBuilder(ModelBuilder modelBuilder)
    {
        _entityBuilder = modelBuilder.Entity<TEntity>();
    }

    /// <summary>
    /// Specifies the S3 path pattern (e.g., "s3://bucket/path/*.parquet").
    /// Supports glob patterns for reading multiple files.
    /// </summary>
    /// <param name="path">The S3 path or glob pattern.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalS3EntityBuilder<TEntity> FromPath(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        _config.Path = path;
        return this;
    }

    /// <summary>
    /// Specifies the data format (e.g., "Parquet", "CSV", "JSONEachRow").
    /// </summary>
    /// <param name="format">The ClickHouse format name.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalS3EntityBuilder<TEntity> WithFormat(string format)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(format);
        _config.Format = format;
        return this;
    }

    /// <summary>
    /// Configures S3 credentials.
    /// </summary>
    /// <param name="configure">Action to configure credentials.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalS3EntityBuilder<TEntity> Connection(Action<S3ConnectionBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new S3ConnectionBuilder(_config);
        configure(builder);
        return this;
    }

    /// <summary>
    /// Specifies the data structure explicitly.
    /// If not specified, structure is inferred from entity properties.
    /// </summary>
    /// <param name="structure">The ClickHouse structure definition (e.g., "id UInt64, name String").</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalS3EntityBuilder<TEntity> WithStructure(string structure)
    {
        _config.Structure = structure;
        return this;
    }

    /// <summary>
    /// Specifies the compression type (e.g., "gzip", "zstd", "none").
    /// </summary>
    /// <param name="compression">The compression type.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalS3EntityBuilder<TEntity> WithCompression(string compression)
    {
        _config.Compression = compression;
        return this;
    }

    /// <summary>
    /// Configures a property mapping with optional customization.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyExpression">Expression selecting the property.</param>
    /// <param name="configure">Optional action to configure the property.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalS3EntityBuilder<TEntity> Property<TProperty>(
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
    public ExternalS3EntityBuilder<TEntity> Ignore(string propertyName)
    {
        _entityBuilder.Ignore(propertyName);
        return this;
    }

    /// <summary>
    /// Applies the configuration to the entity type.
    /// Called automatically by ExternalS3Entity extension method.
    /// </summary>
    internal void Build()
    {
        // External entities must be keyless - they use table functions, not tables
        _entityBuilder.HasNoKey();

        // Set core annotations
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction, true);
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalProvider, "s3");
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalReadOnly, true);

        // Set S3-specific annotations
        if (!string.IsNullOrEmpty(_config.Path))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalS3Path, _config.Path);
        }

        if (!string.IsNullOrEmpty(_config.Format))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalS3Format, _config.Format);
        }

        if (!string.IsNullOrEmpty(_config.AccessKeyEnv))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalS3AccessKeyEnv, _config.AccessKeyEnv);
        }

        if (!string.IsNullOrEmpty(_config.AccessKeyValue))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalS3AccessKeyValue, _config.AccessKeyValue);
        }

        if (!string.IsNullOrEmpty(_config.SecretKeyEnv))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalS3SecretKeyEnv, _config.SecretKeyEnv);
        }

        if (!string.IsNullOrEmpty(_config.SecretKeyValue))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalS3SecretKeyValue, _config.SecretKeyValue);
        }

        if (!string.IsNullOrEmpty(_config.Structure))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalS3Structure, _config.Structure);
        }

        if (!string.IsNullOrEmpty(_config.Compression))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalS3Compression, _config.Compression);
        }
    }

    internal class S3Config
    {
        public string? Path { get; set; }
        public string? Format { get; set; }
        public string? AccessKeyEnv { get; set; }
        public string? AccessKeyValue { get; set; }
        public string? SecretKeyEnv { get; set; }
        public string? SecretKeyValue { get; set; }
        public string? Structure { get; set; }
        public string? Compression { get; set; }
    }
}

/// <summary>
/// Builder for configuring S3 connection credentials.
/// </summary>
public class S3ConnectionBuilder
{
    private readonly ExternalS3EntityBuilder<object>.S3Config _config;

    internal S3ConnectionBuilder(object config)
    {
        _config = (ExternalS3EntityBuilder<object>.S3Config)config;
    }

    /// <summary>
    /// Sets the access key from an environment variable.
    /// </summary>
    /// <param name="envVar">The environment variable name.</param>
    /// <returns>The builder for chaining.</returns>
    public S3ConnectionBuilder AccessKey(string envVar)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(envVar);
        _config.AccessKeyEnv = envVar;
        return this;
    }

    /// <summary>
    /// Sets the access key from a literal value or environment variable.
    /// </summary>
    /// <param name="env">The environment variable name (preferred).</param>
    /// <param name="value">The literal value (not recommended for production).</param>
    /// <returns>The builder for chaining.</returns>
    public S3ConnectionBuilder AccessKey(string? env = null, string? value = null)
    {
        if (!string.IsNullOrEmpty(env))
        {
            _config.AccessKeyEnv = env;
        }

        if (!string.IsNullOrEmpty(value))
        {
            _config.AccessKeyValue = value;
        }

        return this;
    }

    /// <summary>
    /// Sets the secret key from an environment variable.
    /// </summary>
    /// <param name="envVar">The environment variable name.</param>
    /// <returns>The builder for chaining.</returns>
    public S3ConnectionBuilder SecretKey(string envVar)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(envVar);
        _config.SecretKeyEnv = envVar;
        return this;
    }

    /// <summary>
    /// Sets the secret key from a literal value or environment variable.
    /// </summary>
    /// <param name="env">The environment variable name (preferred).</param>
    /// <param name="value">The literal value (not recommended for production).</param>
    /// <returns>The builder for chaining.</returns>
    public S3ConnectionBuilder SecretKey(string? env = null, string? value = null)
    {
        if (!string.IsNullOrEmpty(env))
        {
            _config.SecretKeyEnv = env;
        }

        if (!string.IsNullOrEmpty(value))
        {
            _config.SecretKeyValue = value;
        }

        return this;
    }
}
