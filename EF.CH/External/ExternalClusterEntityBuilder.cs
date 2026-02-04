using System.Linq.Expressions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.External;

/// <summary>
/// Builder for configuring external cluster entities that use the cluster() table function.
/// External entities do not create ClickHouse tables - they query tables across a ClickHouse cluster via table functions.
/// </summary>
/// <typeparam name="TEntity">The entity type.</typeparam>
public class ExternalClusterEntityBuilder<TEntity> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _entityBuilder;
    private readonly ClusterConfig _config = new();

    internal ExternalClusterEntityBuilder(ModelBuilder modelBuilder)
    {
        _entityBuilder = modelBuilder.Entity<TEntity>();
    }

    /// <summary>
    /// Specifies the cluster name.
    /// </summary>
    /// <param name="clusterName">The ClickHouse cluster name.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalClusterEntityBuilder<TEntity> FromCluster(string clusterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterName);
        _config.ClusterName = clusterName;
        return this;
    }

    /// <summary>
    /// Specifies the database and table within the cluster.
    /// </summary>
    /// <param name="database">The database name (or "currentDatabase()" for current database).</param>
    /// <param name="table">The table name.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalClusterEntityBuilder<TEntity> FromTable(string database, string table)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        _config.Database = database;
        _config.Table = table;
        return this;
    }

    /// <summary>
    /// Specifies to use the current database.
    /// </summary>
    /// <param name="table">The table name.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalClusterEntityBuilder<TEntity> FromCurrentDatabase(string table)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        _config.Database = "currentDatabase()";
        _config.Table = table;
        return this;
    }

    /// <summary>
    /// Sets the sharding key for distributed queries.
    /// </summary>
    /// <param name="shardingKey">The sharding key expression.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalClusterEntityBuilder<TEntity> WithShardingKey(string shardingKey)
    {
        _config.ShardingKey = shardingKey;
        return this;
    }

    /// <summary>
    /// Sets the sharding key using a property selector.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyExpression">Expression selecting the sharding key property.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalClusterEntityBuilder<TEntity> WithShardingKey<TProperty>(
        Expression<Func<TEntity, TProperty>> propertyExpression)
    {
        ArgumentNullException.ThrowIfNull(propertyExpression);
        var memberExpression = propertyExpression.Body as MemberExpression
            ?? throw new ArgumentException("Expression must be a member access", nameof(propertyExpression));
        _config.ShardingKey = memberExpression.Member.Name;
        return this;
    }

    /// <summary>
    /// Configures a property mapping with optional customization.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyExpression">Expression selecting the property.</param>
    /// <param name="configure">Optional action to configure the property.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalClusterEntityBuilder<TEntity> Property<TProperty>(
        Expression<Func<TEntity, TProperty>> propertyExpression,
        Action<PropertyBuilder<TProperty>>? configure = null)
    {
        var propBuilder = _entityBuilder.Property(propertyExpression);
        configure?.Invoke(propBuilder);
        return this;
    }

    /// <summary>
    /// Ignores a property (won't be included in queries).
    /// </summary>
    /// <param name="propertyName">The name of the property to ignore.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalClusterEntityBuilder<TEntity> Ignore(string propertyName)
    {
        _entityBuilder.Ignore(propertyName);
        return this;
    }

    /// <summary>
    /// Applies the configuration to the entity type.
    /// Called automatically by ExternalClusterEntity extension method.
    /// </summary>
    internal void Build()
    {
        _entityBuilder.HasNoKey();

        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction, true);
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalProvider, "cluster");
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalReadOnly, true);

        if (!string.IsNullOrEmpty(_config.ClusterName))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalClusterName, _config.ClusterName);
        }

        if (!string.IsNullOrEmpty(_config.Database))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalClusterDatabase, _config.Database);
        }

        if (!string.IsNullOrEmpty(_config.Table))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalClusterTable, _config.Table);
        }

        if (!string.IsNullOrEmpty(_config.ShardingKey))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalClusterShardingKey, _config.ShardingKey);
        }
    }

    private class ClusterConfig
    {
        public string? ClusterName { get; set; }
        public string? Database { get; set; }
        public string? Table { get; set; }
        public string? ShardingKey { get; set; }
    }
}
