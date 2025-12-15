using System.Linq.Expressions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.External;

/// <summary>
/// Builder for configuring external Redis entities that use the redis() table function.
/// Redis entities require a key column and a structure definition (auto-generated or explicit).
/// </summary>
/// <typeparam name="TEntity">The entity type.</typeparam>
public class ExternalRedisEntityBuilder<TEntity> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _entityBuilder;
    private readonly RedisConnectionConfig _connection = new();

    private string? _keyColumn;
    private string? _explicitStructure;
    private bool _allowInserts;

    internal ExternalRedisEntityBuilder(ModelBuilder modelBuilder)
    {
        _entityBuilder = modelBuilder.Entity<TEntity>();
    }

    /// <summary>
    /// Specifies the key column using a lambda expression.
    /// This column becomes the Redis key.
    /// </summary>
    /// <typeparam name="TKey">The key property type.</typeparam>
    /// <param name="keySelector">Expression selecting the key property.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalRedisEntityBuilder<TEntity> KeyColumn<TKey>(Expression<Func<TEntity, TKey>> keySelector)
    {
        if (keySelector.Body is MemberExpression memberExpr)
        {
            _keyColumn = memberExpr.Member.Name;
        }
        else
        {
            throw new ArgumentException("Key selector must be a simple property access expression.", nameof(keySelector));
        }

        return this;
    }

    /// <summary>
    /// Specifies the key column by name.
    /// This column becomes the Redis key.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalRedisEntityBuilder<TEntity> KeyColumn(string columnName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(columnName);
        _keyColumn = columnName;
        return this;
    }

    /// <summary>
    /// Sets an explicit structure definition for the Redis table function.
    /// If not specified, the structure will be auto-generated from entity properties.
    /// </summary>
    /// <param name="structure">The structure definition (e.g., "id UInt64, name String, count UInt32").</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalRedisEntityBuilder<TEntity> Structure(string structure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(structure);
        _explicitStructure = structure;
        return this;
    }

    /// <summary>
    /// Configures the Redis connection parameters.
    /// </summary>
    /// <param name="configure">Action to configure the connection.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalRedisEntityBuilder<TEntity> Connection(Action<RedisConnectionBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new RedisConnectionBuilder(_connection);
        configure(builder);
        return this;
    }

    /// <summary>
    /// Enables INSERT operations via INSERT INTO FUNCTION redis(...).
    /// By default, external entities are read-only.
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public ExternalRedisEntityBuilder<TEntity> AllowInserts()
    {
        _allowInserts = true;
        return this;
    }

    /// <summary>
    /// Explicitly marks as read-only (this is the default).
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public ExternalRedisEntityBuilder<TEntity> ReadOnly()
    {
        _allowInserts = false;
        return this;
    }

    /// <summary>
    /// Configures a property mapping with optional customization.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyExpression">Expression selecting the property.</param>
    /// <param name="configure">Optional action to configure the property.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalRedisEntityBuilder<TEntity> Property<TProperty>(
        Expression<Func<TEntity, TProperty>> propertyExpression,
        Action<PropertyBuilder<TProperty>>? configure = null)
    {
        var propBuilder = _entityBuilder.Property(propertyExpression);
        configure?.Invoke(propBuilder);
        return this;
    }

    /// <summary>
    /// Ignores a property (won't be included in queries or structure).
    /// </summary>
    /// <param name="propertyName">The name of the property to ignore.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalRedisEntityBuilder<TEntity> Ignore(string propertyName)
    {
        _entityBuilder.Ignore(propertyName);
        return this;
    }

    /// <summary>
    /// Applies the configuration to the entity type.
    /// Called automatically by ExternalRedisEntity extension method.
    /// </summary>
    internal void Build()
    {
        // Validate required configuration
        if (string.IsNullOrEmpty(_keyColumn))
        {
            throw new InvalidOperationException(
                $"Redis external entity '{typeof(TEntity).Name}' requires a key column. " +
                "Call KeyColumn(x => x.PropertyName) or KeyColumn(\"columnName\").");
        }

        // External entities must be keyless - they use table functions, not tables
        _entityBuilder.HasNoKey();

        // Set core annotations
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction, true);
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalProvider, "redis");
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalReadOnly, !_allowInserts);

        // Set Redis-specific annotations
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalRedisKeyColumn, _keyColumn);

        if (!string.IsNullOrEmpty(_explicitStructure))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalRedisStructure, _explicitStructure);
        }

        // Apply connection configuration
        _connection.ApplyTo(_entityBuilder);
    }
}
