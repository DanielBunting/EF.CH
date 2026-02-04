using System.Linq.Expressions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.External;

/// <summary>
/// Builder for configuring external file entities that use the file() table function.
/// External entities do not create ClickHouse tables - they query local files directly via table functions.
/// </summary>
/// <typeparam name="TEntity">The entity type.</typeparam>
public class ExternalFileEntityBuilder<TEntity> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _entityBuilder;
    private readonly FileConfig _config = new();

    internal ExternalFileEntityBuilder(ModelBuilder modelBuilder)
    {
        _entityBuilder = modelBuilder.Entity<TEntity>();
    }

    /// <summary>
    /// Specifies the file path pattern (e.g., "/data/*.csv", "file*.parquet").
    /// Supports glob patterns for reading multiple files.
    /// </summary>
    /// <param name="path">The file path or glob pattern.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalFileEntityBuilder<TEntity> FromPath(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        _config.Path = path;
        return this;
    }

    /// <summary>
    /// Specifies the data format (e.g., "CSV", "Parquet", "JSONEachRow").
    /// </summary>
    /// <param name="format">The ClickHouse format name.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalFileEntityBuilder<TEntity> WithFormat(string format)
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
    public ExternalFileEntityBuilder<TEntity> WithStructure(string structure)
    {
        _config.Structure = structure;
        return this;
    }

    /// <summary>
    /// Specifies the compression type (e.g., "gzip", "zstd", "none").
    /// </summary>
    /// <param name="compression">The compression type.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalFileEntityBuilder<TEntity> WithCompression(string compression)
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
    public ExternalFileEntityBuilder<TEntity> Property<TProperty>(
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
    public ExternalFileEntityBuilder<TEntity> Ignore(string propertyName)
    {
        _entityBuilder.Ignore(propertyName);
        return this;
    }

    /// <summary>
    /// Applies the configuration to the entity type.
    /// Called automatically by ExternalFileEntity extension method.
    /// </summary>
    internal void Build()
    {
        _entityBuilder.HasNoKey();

        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction, true);
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalProvider, "file");
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalReadOnly, true);

        if (!string.IsNullOrEmpty(_config.Path))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalFilePath, _config.Path);
        }

        if (!string.IsNullOrEmpty(_config.Format))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalFileFormat, _config.Format);
        }

        if (!string.IsNullOrEmpty(_config.Structure))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalFileStructure, _config.Structure);
        }

        if (!string.IsNullOrEmpty(_config.Compression))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalFileCompression, _config.Compression);
        }
    }

    private class FileConfig
    {
        public string? Path { get; set; }
        public string? Format { get; set; }
        public string? Structure { get; set; }
        public string? Compression { get; set; }
    }
}
