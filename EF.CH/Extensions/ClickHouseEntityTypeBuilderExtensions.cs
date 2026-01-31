using System.Linq.Expressions;
using EF.CH.Dictionaries;
using EF.CH.Metadata;
using EF.CH.Query.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions;

/// <summary>
/// A builder that provides fluent chaining for configuring replicated engine options.
/// </summary>
/// <remarks>
/// <para>
/// This builder is returned from replicated engine methods (e.g., <c>UseReplicatedMergeTree</c>)
/// and allows chaining cluster and replication configuration in a fluent manner.
/// </para>
/// <para>
/// The builder supports implicit conversion back to <see cref="EntityTypeBuilder{TEntity}"/>,
/// ensuring backward compatibility with existing code that chains other entity configuration methods.
/// </para>
/// </remarks>
/// <typeparam name="TEntity">The entity type being configured.</typeparam>
/// <example>
/// <code>
/// // Fluent chain pattern
/// entity.UseReplicatedMergeTree(x => x.Id)
///       .WithCluster("geo_cluster")
///       .WithReplication("/clickhouse/geo/{database}/{table}");
///
/// // Implicit conversion allows continued chaining
/// entity.UseReplicatedMergeTree(x => x.Id)
///       .WithCluster("geo_cluster")
///       .HasPartitionByMonth(x => x.OrderDate);  // Continues with EntityTypeBuilder
/// </code>
/// </example>
public class ReplicatedEngineBuilder<TEntity> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _builder;

    /// <summary>
    /// Initializes a new instance of the <see cref="ReplicatedEngineBuilder{TEntity}"/> class.
    /// </summary>
    /// <param name="builder">The underlying entity type builder.</param>
    internal ReplicatedEngineBuilder(EntityTypeBuilder<TEntity> builder)
    {
        _builder = builder;
    }

    /// <summary>
    /// Configures the cluster name for this entity's DDL operations (ON CLUSTER clause).
    /// </summary>
    /// <param name="clusterName">The cluster name as defined in ClickHouse server configuration.</param>
    /// <returns>This builder for continued chaining.</returns>
    public ReplicatedEngineBuilder<TEntity> WithCluster(string clusterName)
    {
        _builder.UseCluster(clusterName);
        return this;
    }

    /// <summary>
    /// Configures replication settings for this entity.
    /// </summary>
    /// <param name="zooKeeperPath">The ZooKeeper/Keeper path for replication metadata. Supports placeholders: {database}, {table}, {uuid}</param>
    /// <param name="replicaName">The replica name, usually "{replica}" for macro expansion.</param>
    /// <returns>This builder for continued chaining.</returns>
    public ReplicatedEngineBuilder<TEntity> WithReplication(
        string zooKeeperPath,
        string replicaName = "{replica}")
    {
        _builder.HasReplication(zooKeeperPath, replicaName);
        return this;
    }

    /// <summary>
    /// Assigns this entity to a table group.
    /// </summary>
    /// <param name="tableGroupName">The table group name from configuration.</param>
    /// <returns>This builder for continued chaining.</returns>
    public ReplicatedEngineBuilder<TEntity> WithTableGroup(string tableGroupName)
    {
        _builder.UseTableGroup(tableGroupName);
        return this;
    }

    /// <summary>
    /// Returns the underlying entity type builder for continued configuration.
    /// </summary>
    /// <returns>The underlying <see cref="EntityTypeBuilder{TEntity}"/>.</returns>
    public EntityTypeBuilder<TEntity> And() => _builder;

    /// <summary>
    /// Implicitly converts the builder back to <see cref="EntityTypeBuilder{TEntity}"/>
    /// for seamless integration with existing configuration patterns.
    /// </summary>
    /// <param name="builder">The replicated engine builder to convert.</param>
    public static implicit operator EntityTypeBuilder<TEntity>(
        ReplicatedEngineBuilder<TEntity> builder) => builder._builder;
}

/// <summary>
/// A builder that provides fluent chaining for configuring Distributed engine options.
/// </summary>
/// <remarks>
/// <para>
/// This builder is returned from <c>UseDistributed</c> methods
/// and allows chaining sharding key and policy configuration in a fluent manner.
/// </para>
/// <para>
/// The builder supports implicit conversion back to <see cref="EntityTypeBuilder{TEntity}"/>,
/// ensuring backward compatibility with existing code that chains other entity configuration methods.
/// </para>
/// </remarks>
/// <typeparam name="TEntity">The entity type being configured.</typeparam>
/// <example>
/// <code>
/// // Fluent chain pattern
/// entity.UseDistributed("my_cluster", "events_local")
///       .WithShardingKey(x => x.UserId);
///
/// // With expression-based sharding
/// entity.UseDistributed("my_cluster", "events_local")
///       .WithShardingKey("cityHash64(UserId)")
///       .WithPolicy("ssd_policy");
/// </code>
/// </example>
public class DistributedEngineBuilder<TEntity> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _builder;

    /// <summary>
    /// Initializes a new instance of the <see cref="DistributedEngineBuilder{TEntity}"/> class.
    /// </summary>
    /// <param name="builder">The underlying entity type builder.</param>
    internal DistributedEngineBuilder(EntityTypeBuilder<TEntity> builder)
    {
        _builder = builder;
    }

    /// <summary>
    /// Configures the sharding key using a property selector expression.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="shardingKeySelector">Expression selecting the property to use as sharding key.</param>
    /// <returns>This builder for continued chaining.</returns>
    public DistributedEngineBuilder<TEntity> WithShardingKey<TProperty>(
        Expression<Func<TEntity, TProperty>> shardingKeySelector)
    {
        ArgumentNullException.ThrowIfNull(shardingKeySelector);
        var propertyName = ExpressionExtensions.GetPropertyName(shardingKeySelector);
        _builder.HasAnnotation(ClickHouseAnnotationNames.DistributedShardingKey, propertyName);
        return this;
    }

    /// <summary>
    /// Configures the sharding key using a raw SQL expression.
    /// </summary>
    /// <param name="shardingKeyExpression">The sharding key expression (e.g., "cityHash64(UserId)").</param>
    /// <returns>This builder for continued chaining.</returns>
    public DistributedEngineBuilder<TEntity> WithShardingKey(string shardingKeyExpression)
    {
        ArgumentNullException.ThrowIfNull(shardingKeyExpression);
        _builder.HasAnnotation(ClickHouseAnnotationNames.DistributedShardingKey, shardingKeyExpression);
        return this;
    }

    /// <summary>
    /// Configures the storage policy for the Distributed engine.
    /// </summary>
    /// <param name="policyName">The storage policy name as defined in ClickHouse server configuration.</param>
    /// <returns>This builder for continued chaining.</returns>
    public DistributedEngineBuilder<TEntity> WithPolicy(string policyName)
    {
        ArgumentNullException.ThrowIfNull(policyName);
        _builder.HasAnnotation(ClickHouseAnnotationNames.DistributedPolicyName, policyName);
        return this;
    }

    /// <summary>
    /// Returns the underlying entity type builder for continued configuration.
    /// </summary>
    /// <returns>The underlying <see cref="EntityTypeBuilder{TEntity}"/>.</returns>
    public EntityTypeBuilder<TEntity> And() => _builder;

    /// <summary>
    /// Implicitly converts the builder back to <see cref="EntityTypeBuilder{TEntity}"/>
    /// for seamless integration with existing configuration patterns.
    /// </summary>
    /// <param name="builder">The distributed engine builder to convert.</param>
    public static implicit operator EntityTypeBuilder<TEntity>(
        DistributedEngineBuilder<TEntity> builder) => builder._builder;
}

/// <summary>
/// Extension methods for configuring ClickHouse-specific entity type options.
/// </summary>
public static class ClickHouseEntityTypeBuilderExtensions
{
    /// <summary>
    /// Configures the entity to use a MergeTree engine with the specified ORDER BY columns.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseMergeTree(
        this EntityTypeBuilder builder,
        params string[] orderByColumns)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(orderByColumns);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for MergeTree.", nameof(orderByColumns));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a MergeTree engine with the specified ORDER BY columns.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        params string[] orderByColumns)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseMergeTree(orderByColumns);
        return builder;
    }

    /// <summary>
    /// Configures the entity to use a MergeTree engine with the specified ORDER BY columns.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.UseMergeTree(x => new { x.OrderDate, x.Id });
    /// // or
    /// entity.UseMergeTree(x => x.Id);
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> UseMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        return builder.UseMergeTree(columns);
    }

    /// <summary>
    /// Configures the entity to use a ReplacingMergeTree engine.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseReplacingMergeTree(
        this EntityTypeBuilder builder,
        params string[] orderByColumns)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(orderByColumns);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for ReplacingMergeTree.", nameof(orderByColumns));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "ReplacingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a ReplacingMergeTree engine.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseReplacingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        params string[] orderByColumns)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseReplacingMergeTree(orderByColumns);
        return builder;
    }

    /// <summary>
    /// Configures the entity to use a ReplacingMergeTree engine with a version column.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="versionColumn">The version column for deduplication.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseReplacingMergeTree(
        this EntityTypeBuilder builder,
        string versionColumn,
        params string[] orderByColumns)
    {
        builder.UseReplacingMergeTree(orderByColumns);
        builder.HasAnnotation(ClickHouseAnnotationNames.VersionColumn, versionColumn);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a ReplacingMergeTree engine with a version column.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="versionColumn">The version column for deduplication.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseReplacingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string versionColumn,
        params string[] orderByColumns)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseReplacingMergeTree(versionColumn, orderByColumns);
        return builder;
    }

    /// <summary>
    /// Configures the entity to use a ReplacingMergeTree engine with the specified ORDER BY columns.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.UseReplacingMergeTree(x => new { x.UserId, x.Timestamp });
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> UseReplacingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        return builder.UseReplacingMergeTree(columns);
    }

    /// <summary>
    /// Configures the entity to use a ReplacingMergeTree engine with a version column.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="versionColumnExpression">Expression selecting the version column for deduplication.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.UseReplacingMergeTree(x => x.UpdatedAt, x => new { x.Id });
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> UseReplacingMergeTree<TEntity, TVersion>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, TVersion>> versionColumnExpression,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(versionColumnExpression);
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var versionColumn = ExpressionExtensions.GetPropertyName(versionColumnExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        return builder.UseReplacingMergeTree(versionColumn, columns);
    }

    /// <summary>
    /// Configures the entity to use a ReplacingMergeTree engine with version and is_deleted columns.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TVersion">The version column type.</typeparam>
    /// <typeparam name="TIsDeleted">The is_deleted column type (should be byte/UInt8).</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="versionColumnExpression">Expression selecting the version column.</param>
    /// <param name="isDeletedColumnExpression">Expression selecting the is_deleted column (UInt8).</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <remarks>
    /// <para>
    /// Requires ClickHouse 23.2+. When is_deleted is specified, rows where the winning version
    /// has is_deleted=1 are physically removed during background merges.
    /// </para>
    /// <para>
    /// With FINAL, deleted rows are automatically excluded from query results.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// entity.UseReplacingMergeTree(
    ///     x => x.Version,      // Version column
    ///     x => x.IsDeleted,    // IsDeleted column (UInt8)
    ///     x => new { x.Id });  // ORDER BY
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> UseReplacingMergeTree<TEntity, TVersion, TIsDeleted>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, TVersion>> versionColumnExpression,
        Expression<Func<TEntity, TIsDeleted>> isDeletedColumnExpression,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(versionColumnExpression);
        ArgumentNullException.ThrowIfNull(isDeletedColumnExpression);
        ArgumentNullException.ThrowIfNull(orderByExpression);

        var versionColumn = ExpressionExtensions.GetPropertyName(versionColumnExpression);
        var isDeletedColumn = ExpressionExtensions.GetPropertyName(isDeletedColumnExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);

        builder.UseReplacingMergeTree(versionColumn, columns);
        builder.HasAnnotation(ClickHouseAnnotationNames.IsDeletedColumn, isDeletedColumn);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a SummingMergeTree engine.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseSummingMergeTree(
        this EntityTypeBuilder builder,
        params string[] orderByColumns)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(orderByColumns);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for SummingMergeTree.", nameof(orderByColumns));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "SummingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a SummingMergeTree engine.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseSummingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        params string[] orderByColumns)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseSummingMergeTree(orderByColumns);
        return builder;
    }

    /// <summary>
    /// Configures the entity to use an AggregatingMergeTree engine.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseAggregatingMergeTree(
        this EntityTypeBuilder builder,
        params string[] orderByColumns)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(orderByColumns);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for AggregatingMergeTree.", nameof(orderByColumns));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "AggregatingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use an AggregatingMergeTree engine.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseAggregatingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        params string[] orderByColumns)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseAggregatingMergeTree(orderByColumns);
        return builder;
    }

    /// <summary>
    /// Configures the entity to use a SummingMergeTree engine with the specified ORDER BY columns.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseSummingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        return builder.UseSummingMergeTree(columns);
    }

    /// <summary>
    /// Configures the entity to use an AggregatingMergeTree engine with the specified ORDER BY columns.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseAggregatingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        return builder.UseAggregatingMergeTree(columns);
    }

    #region CollapsingMergeTree

    /// <summary>
    /// Configures the entity to use a CollapsingMergeTree engine.
    /// </summary>
    /// <remarks>
    /// CollapsingMergeTree is designed to track state changes where rows can be "cancelled"
    /// by inserting a row with the opposite sign. During background merges, ClickHouse
    /// removes pairs of rows with opposite signs and matching ORDER BY keys.
    ///
    /// The sign column must be Int8 with values +1 (state row) or -1 (cancel row).
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="signColumnExpression">Expression selecting the sign column (Int8/sbyte with +1 or -1).</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.UseCollapsingMergeTree(
    ///     signColumn: x => x.Sign,
    ///     orderBy: x => new { x.UserId, x.EventTime });
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> UseCollapsingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, sbyte>> signColumnExpression,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(signColumnExpression);
        ArgumentNullException.ThrowIfNull(orderByExpression);

        var signColumn = ExpressionExtensions.GetPropertyName(signColumnExpression);
        var orderByColumns = ExpressionExtensions.GetPropertyNames(orderByExpression);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for CollapsingMergeTree.", nameof(orderByExpression));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "CollapsingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.SignColumn, signColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a CollapsingMergeTree engine with string column names.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="signColumn">The name of the sign column.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseCollapsingMergeTree(
        this EntityTypeBuilder builder,
        string signColumn,
        params string[] orderByColumns)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(signColumn);
        ArgumentNullException.ThrowIfNull(orderByColumns);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for CollapsingMergeTree.", nameof(orderByColumns));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "CollapsingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.SignColumn, signColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a VersionedCollapsingMergeTree engine.
    /// </summary>
    /// <remarks>
    /// VersionedCollapsingMergeTree extends CollapsingMergeTree to handle out-of-order inserts.
    /// It uses a version column to correctly collapse rows even when they arrive in different order.
    ///
    /// The sign column must be Int8 with values +1 (state row) or -1 (cancel row).
    /// The version column should be an incrementing value (typically UInt8, UInt16, UInt32, UInt64).
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TVersion">The version column type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="signColumnExpression">Expression selecting the sign column (Int8/sbyte with +1 or -1).</param>
    /// <param name="versionColumnExpression">Expression selecting the version column.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.UseVersionedCollapsingMergeTree(
    ///     signColumn: x => x.Sign,
    ///     versionColumn: x => x.Version,
    ///     orderBy: x => new { x.UserId, x.EventTime });
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> UseVersionedCollapsingMergeTree<TEntity, TVersion>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, sbyte>> signColumnExpression,
        Expression<Func<TEntity, TVersion>> versionColumnExpression,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(signColumnExpression);
        ArgumentNullException.ThrowIfNull(versionColumnExpression);
        ArgumentNullException.ThrowIfNull(orderByExpression);

        var signColumn = ExpressionExtensions.GetPropertyName(signColumnExpression);
        var versionColumn = ExpressionExtensions.GetPropertyName(versionColumnExpression);
        var orderByColumns = ExpressionExtensions.GetPropertyNames(orderByExpression);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for VersionedCollapsingMergeTree.", nameof(orderByExpression));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "VersionedCollapsingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.SignColumn, signColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.VersionColumn, versionColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a VersionedCollapsingMergeTree engine with string column names.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="signColumn">The name of the sign column.</param>
    /// <param name="versionColumn">The name of the version column.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseVersionedCollapsingMergeTree(
        this EntityTypeBuilder builder,
        string signColumn,
        string versionColumn,
        params string[] orderByColumns)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(signColumn);
        ArgumentException.ThrowIfNullOrWhiteSpace(versionColumn);
        ArgumentNullException.ThrowIfNull(orderByColumns);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for VersionedCollapsingMergeTree.", nameof(orderByColumns));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "VersionedCollapsingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.SignColumn, signColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.VersionColumn, versionColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
    }

    #endregion

    #region Replicated Engines

    /// <summary>
    /// Configures the entity to use a ReplicatedMergeTree engine.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ReplicatedMergeTree requires ZooKeeper/Keeper coordination for replication.
    /// The ZooKeeper path and replica name can be configured via <see cref="HasReplication"/>,
    /// or will use defaults from the table group or cluster configuration.
    /// </para>
    /// <para>
    /// Tables using replicated engines should also have a cluster configured via <see cref="UseCluster"/>
    /// or inherit one from their table group.
    /// </para>
    /// </remarks>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseReplicatedMergeTree(
        this EntityTypeBuilder builder,
        params string[] orderByColumns)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(orderByColumns);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for ReplicatedMergeTree.", nameof(orderByColumns));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "ReplicatedMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);
        builder.HasAnnotation(ClickHouseAnnotationNames.IsReplicated, true);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a ReplicatedMergeTree engine.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>A builder for configuring replicated engine options with fluent chaining.</returns>
    /// <example>
    /// <code>
    /// entity.UseReplicatedMergeTree(x => x.Id)
    ///       .WithCluster("geo_cluster")
    ///       .WithReplication("/clickhouse/geo/{database}/{table}");
    /// </code>
    /// </example>
    public static ReplicatedEngineBuilder<TEntity> UseReplicatedMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        ((EntityTypeBuilder)builder).UseReplicatedMergeTree(columns);
        return new ReplicatedEngineBuilder<TEntity>(builder);
    }

    /// <summary>
    /// Configures the entity to use a ReplicatedMergeTree engine.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>A builder for configuring replicated engine options with fluent chaining.</returns>
    public static ReplicatedEngineBuilder<TEntity> UseReplicatedMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        params string[] orderByColumns)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseReplicatedMergeTree(orderByColumns);
        return new ReplicatedEngineBuilder<TEntity>(builder);
    }

    /// <summary>
    /// Configures the entity to use a ReplicatedReplacingMergeTree engine.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseReplicatedReplacingMergeTree(
        this EntityTypeBuilder builder,
        params string[] orderByColumns)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(orderByColumns);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for ReplicatedReplacingMergeTree.", nameof(orderByColumns));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "ReplicatedReplacingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);
        builder.HasAnnotation(ClickHouseAnnotationNames.IsReplicated, true);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a ReplicatedReplacingMergeTree engine with a version column.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TVersion">The version column type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="versionColumnExpression">Expression selecting the version column for deduplication.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>A builder for configuring replicated engine options with fluent chaining.</returns>
    /// <example>
    /// <code>
    /// entity.UseReplicatedReplacingMergeTree(x => x.Version, x => x.Id)
    ///       .WithCluster("geo_cluster")
    ///       .WithReplication("/clickhouse/geo/{database}/{table}");
    /// </code>
    /// </example>
    public static ReplicatedEngineBuilder<TEntity> UseReplicatedReplacingMergeTree<TEntity, TVersion>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, TVersion>> versionColumnExpression,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(versionColumnExpression);
        ArgumentNullException.ThrowIfNull(orderByExpression);

        var versionColumn = ExpressionExtensions.GetPropertyName(versionColumnExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);

        ((EntityTypeBuilder)builder).UseReplicatedReplacingMergeTree(columns);
        builder.HasAnnotation(ClickHouseAnnotationNames.VersionColumn, versionColumn);

        return new ReplicatedEngineBuilder<TEntity>(builder);
    }

    /// <summary>
    /// Configures the entity to use a ReplicatedReplacingMergeTree engine.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>A builder for configuring replicated engine options with fluent chaining.</returns>
    public static ReplicatedEngineBuilder<TEntity> UseReplicatedReplacingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        ((EntityTypeBuilder)builder).UseReplicatedReplacingMergeTree(columns);
        return new ReplicatedEngineBuilder<TEntity>(builder);
    }

    /// <summary>
    /// Configures the entity to use a ReplicatedSummingMergeTree engine.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseReplicatedSummingMergeTree(
        this EntityTypeBuilder builder,
        params string[] orderByColumns)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(orderByColumns);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for ReplicatedSummingMergeTree.", nameof(orderByColumns));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "ReplicatedSummingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);
        builder.HasAnnotation(ClickHouseAnnotationNames.IsReplicated, true);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a ReplicatedSummingMergeTree engine.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>A builder for configuring replicated engine options with fluent chaining.</returns>
    public static ReplicatedEngineBuilder<TEntity> UseReplicatedSummingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        ((EntityTypeBuilder)builder).UseReplicatedSummingMergeTree(columns);
        return new ReplicatedEngineBuilder<TEntity>(builder);
    }

    /// <summary>
    /// Configures the entity to use a ReplicatedAggregatingMergeTree engine.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByColumns">The columns for ORDER BY clause.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseReplicatedAggregatingMergeTree(
        this EntityTypeBuilder builder,
        params string[] orderByColumns)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(orderByColumns);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for ReplicatedAggregatingMergeTree.", nameof(orderByColumns));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "ReplicatedAggregatingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);
        builder.HasAnnotation(ClickHouseAnnotationNames.IsReplicated, true);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a ReplicatedAggregatingMergeTree engine.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>A builder for configuring replicated engine options with fluent chaining.</returns>
    public static ReplicatedEngineBuilder<TEntity> UseReplicatedAggregatingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        ((EntityTypeBuilder)builder).UseReplicatedAggregatingMergeTree(columns);
        return new ReplicatedEngineBuilder<TEntity>(builder);
    }

    /// <summary>
    /// Configures the entity to use a ReplicatedCollapsingMergeTree engine.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="signColumnExpression">Expression selecting the sign column (Int8/sbyte with +1 or -1).</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>A builder for configuring replicated engine options with fluent chaining.</returns>
    public static ReplicatedEngineBuilder<TEntity> UseReplicatedCollapsingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, sbyte>> signColumnExpression,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(signColumnExpression);
        ArgumentNullException.ThrowIfNull(orderByExpression);

        var signColumn = ExpressionExtensions.GetPropertyName(signColumnExpression);
        var orderByColumns = ExpressionExtensions.GetPropertyNames(orderByExpression);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for ReplicatedCollapsingMergeTree.", nameof(orderByExpression));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "ReplicatedCollapsingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.SignColumn, signColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);
        builder.HasAnnotation(ClickHouseAnnotationNames.IsReplicated, true);

        return new ReplicatedEngineBuilder<TEntity>(builder);
    }

    /// <summary>
    /// Configures the entity to use a ReplicatedVersionedCollapsingMergeTree engine.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TVersion">The version column type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="signColumnExpression">Expression selecting the sign column (Int8/sbyte with +1 or -1).</param>
    /// <param name="versionColumnExpression">Expression selecting the version column.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>A builder for configuring replicated engine options with fluent chaining.</returns>
    public static ReplicatedEngineBuilder<TEntity> UseReplicatedVersionedCollapsingMergeTree<TEntity, TVersion>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, sbyte>> signColumnExpression,
        Expression<Func<TEntity, TVersion>> versionColumnExpression,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(signColumnExpression);
        ArgumentNullException.ThrowIfNull(versionColumnExpression);
        ArgumentNullException.ThrowIfNull(orderByExpression);

        var signColumn = ExpressionExtensions.GetPropertyName(signColumnExpression);
        var versionColumn = ExpressionExtensions.GetPropertyName(versionColumnExpression);
        var orderByColumns = ExpressionExtensions.GetPropertyNames(orderByExpression);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for ReplicatedVersionedCollapsingMergeTree.", nameof(orderByExpression));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "ReplicatedVersionedCollapsingMergeTree");
        builder.HasAnnotation(ClickHouseAnnotationNames.SignColumn, signColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.VersionColumn, versionColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);
        builder.HasAnnotation(ClickHouseAnnotationNames.IsReplicated, true);

        return new ReplicatedEngineBuilder<TEntity>(builder);
    }

    #endregion

    #region Cluster and Table Group Configuration

    /// <summary>
    /// Configures the cluster name for this entity's DDL operations (ON CLUSTER clause).
    /// </summary>
    /// <remarks>
    /// <para>
    /// When a cluster is specified, DDL statements (CREATE TABLE, ALTER TABLE, DROP TABLE)
    /// will include an ON CLUSTER clause, causing them to execute on all cluster nodes.
    /// </para>
    /// <para>
    /// This is typically used with replicated engines. If using table groups,
    /// prefer <see cref="UseTableGroup"/> which handles cluster assignment automatically.
    /// </para>
    /// </remarks>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="clusterName">The cluster name as defined in ClickHouse server configuration.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseCluster(
        this EntityTypeBuilder builder,
        string clusterName)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterName);

        builder.HasAnnotation(ClickHouseAnnotationNames.EntityClusterName, clusterName);

        return builder;
    }

    /// <summary>
    /// Configures the cluster name for this entity's DDL operations (ON CLUSTER clause).
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="clusterName">The cluster name as defined in ClickHouse server configuration.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseCluster<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string clusterName)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseCluster(clusterName);
        return builder;
    }

    /// <summary>
    /// Assigns this entity to a table group.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Table groups provide a way to organize entities with shared cluster, connection,
    /// and replication settings. The group configuration is defined in <c>appsettings.json</c>
    /// or via the fluent configuration API.
    /// </para>
    /// <para>
    /// When an entity is assigned to a table group, it inherits:
    /// - The cluster for ON CLUSTER DDL operations
    /// - Connection routing (which endpoints to use for reads/writes)
    /// - Replication settings (ZooKeeper path, replica name)
    /// </para>
    /// </remarks>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="tableGroupName">The table group name from configuration.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// // In OnModelCreating:
    /// modelBuilder.Entity&lt;SessionCache&gt;(e =>
    /// {
    ///     e.UseMergeTree(x => x.SessionId);
    ///     e.UseTableGroup("LocalCache");  // No cluster, no replication
    /// });
    ///
    /// modelBuilder.Entity&lt;Order&gt;(e =>
    /// {
    ///     e.UseReplicatedReplacingMergeTree(x => x.Version, x => x.Id);
    ///     e.UseTableGroup("Core");  // geo_cluster, replicated
    /// });
    /// </code>
    /// </example>
    public static EntityTypeBuilder UseTableGroup(
        this EntityTypeBuilder builder,
        string tableGroupName)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableGroupName);

        builder.HasAnnotation(ClickHouseAnnotationNames.TableGroup, tableGroupName);

        return builder;
    }

    /// <summary>
    /// Assigns this entity to a table group.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="tableGroupName">The table group name from configuration.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseTableGroup<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string tableGroupName)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseTableGroup(tableGroupName);
        return builder;
    }

    /// <summary>
    /// Marks this entity as local-only, preventing ON CLUSTER DDL and replication.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use this for tables that should only exist on the local node, such as:
    /// - Session caches
    /// - Temporary working tables
    /// - Node-specific configuration
    /// </para>
    /// <para>
    /// This takes precedence over any cluster or table group settings.
    /// The entity will not have ON CLUSTER in its DDL and should not use
    /// replicated engine variants.
    /// </para>
    /// </remarks>
    /// <param name="builder">The entity type builder.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder IsLocalOnly(this EntityTypeBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.HasAnnotation(ClickHouseAnnotationNames.IsLocalOnly, true);
        return builder;
    }

    /// <summary>
    /// Marks this entity as local-only, preventing ON CLUSTER DDL and replication.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> IsLocalOnly<TEntity>(
        this EntityTypeBuilder<TEntity> builder)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).IsLocalOnly();
        return builder;
    }

    /// <summary>
    /// Configures replication settings for this entity.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is typically used in conjunction with replicated engines.
    /// If using table groups, replication settings are usually inherited from
    /// the cluster configuration.
    /// </para>
    /// <para>
    /// The path supports placeholders: {database}, {table}, {uuid}
    /// </para>
    /// </remarks>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="zooKeeperPath">The ZooKeeper/Keeper path for replication metadata.</param>
    /// <param name="replicaName">The replica name, usually "{replica}" for macro expansion.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder HasReplication(
        this EntityTypeBuilder builder,
        string zooKeeperPath,
        string replicaName = "{replica}")
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(zooKeeperPath);

        builder.HasAnnotation(ClickHouseAnnotationNames.ReplicatedPath, zooKeeperPath);
        builder.HasAnnotation(ClickHouseAnnotationNames.ReplicaName, replicaName);

        return builder;
    }

    /// <summary>
    /// Configures replication settings for this entity.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="zooKeeperPath">The ZooKeeper/Keeper path for replication metadata.</param>
    /// <param name="replicaName">The replica name, usually "{replica}" for macro expansion.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> HasReplication<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string zooKeeperPath,
        string replicaName = "{replica}")
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).HasReplication(zooKeeperPath, replicaName);
        return builder;
    }

    #endregion

    #region Null Engine

    /// <summary>
    /// Configures the entity to use the Null engine.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The Null engine discards all inserted data and returns empty results for SELECT queries.
    /// It functions like /dev/null - data goes in but is never stored.
    /// </para>
    /// <para>
    /// This is commonly used as a source table for materialized views when you don't need
    /// to store the raw data, only the aggregated results in the views.
    /// </para>
    /// </remarks>
    /// <param name="builder">The entity type builder.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// // Raw events go into Null table, triggering materialized views
    /// modelBuilder.Entity&lt;RawEvent&gt;(entity =>
    /// {
    ///     entity.UseNullEngine();
    /// });
    ///
    /// // Materialized view stores only aggregated data
    /// modelBuilder.Entity&lt;HourlySummary&gt;(entity =>
    /// {
    ///     entity.UseSummingMergeTree(x => new { x.Hour, x.Category });
    ///     entity.AsMaterializedView&lt;HourlySummary, RawEvent&gt;(...);
    /// });
    /// </code>
    /// </example>
    public static EntityTypeBuilder UseNullEngine(this EntityTypeBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "Null");
        return builder;
    }

    /// <summary>
    /// Configures the entity to use the Null engine.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The Null engine discards all inserted data and returns empty results for SELECT queries.
    /// It functions like /dev/null - data goes in but is never stored.
    /// </para>
    /// <para>
    /// This is commonly used as a source table for materialized views when you don't need
    /// to store the raw data, only the aggregated results in the views.
    /// </para>
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseNullEngine<TEntity>(
        this EntityTypeBuilder<TEntity> builder)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseNullEngine();
        return builder;
    }

    #endregion

    #region Distributed Engine

    /// <summary>
    /// Configures the entity to use a Distributed engine that queries across a cluster.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The Distributed engine provides a unified view over sharded data spread across multiple servers.
    /// It does not store data itself but acts as a proxy to the underlying local tables.
    /// </para>
    /// <para>
    /// The underlying local table must exist on each node of the cluster before creating the distributed table.
    /// </para>
    /// </remarks>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="cluster">The cluster name as defined in ClickHouse server configuration.</param>
    /// <param name="database">The database name on the cluster nodes (use "currentDatabase()" for current database).</param>
    /// <param name="table">The underlying local table name on each node.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseDistributed(
        this EntityTypeBuilder builder,
        string cluster,
        string database,
        string table)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(cluster);
        ArgumentException.ThrowIfNullOrWhiteSpace(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, "Distributed");
        builder.HasAnnotation(ClickHouseAnnotationNames.DistributedCluster, cluster);
        builder.HasAnnotation(ClickHouseAnnotationNames.DistributedDatabase, database);
        builder.HasAnnotation(ClickHouseAnnotationNames.DistributedTable, table);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use a Distributed engine that queries across a cluster.
    /// Uses currentDatabase() for the database parameter.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The Distributed engine provides a unified view over sharded data spread across multiple servers.
    /// It does not store data itself but acts as a proxy to the underlying local tables.
    /// </para>
    /// <para>
    /// The underlying local table must exist on each node of the cluster before creating the distributed table.
    /// </para>
    /// </remarks>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="cluster">The cluster name as defined in ClickHouse server configuration.</param>
    /// <param name="table">The underlying local table name on each node.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseDistributed(
        this EntityTypeBuilder builder,
        string cluster,
        string table)
    {
        return builder.UseDistributed(cluster, "currentDatabase()", table);
    }

    /// <summary>
    /// Configures the entity to use a Distributed engine that queries across a cluster.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The Distributed engine provides a unified view over sharded data spread across multiple servers.
    /// It does not store data itself but acts as a proxy to the underlying local tables.
    /// </para>
    /// <para>
    /// The underlying local table must exist on each node of the cluster before creating the distributed table.
    /// </para>
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="cluster">The cluster name as defined in ClickHouse server configuration.</param>
    /// <param name="database">The database name on the cluster nodes (use "currentDatabase()" for current database).</param>
    /// <param name="table">The underlying local table name on each node.</param>
    /// <returns>A builder for configuring distributed engine options with fluent chaining.</returns>
    /// <example>
    /// <code>
    /// entity.UseDistributed("my_cluster", "default", "events_local")
    ///       .WithShardingKey(x => x.UserId);
    /// </code>
    /// </example>
    public static DistributedEngineBuilder<TEntity> UseDistributed<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string cluster,
        string database,
        string table)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseDistributed(cluster, database, table);
        return new DistributedEngineBuilder<TEntity>(builder);
    }

    /// <summary>
    /// Configures the entity to use a Distributed engine that queries across a cluster.
    /// Uses currentDatabase() for the database parameter.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The Distributed engine provides a unified view over sharded data spread across multiple servers.
    /// It does not store data itself but acts as a proxy to the underlying local tables.
    /// </para>
    /// <para>
    /// The underlying local table must exist on each node of the cluster before creating the distributed table.
    /// </para>
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="cluster">The cluster name as defined in ClickHouse server configuration.</param>
    /// <param name="table">The underlying local table name on each node.</param>
    /// <returns>A builder for configuring distributed engine options with fluent chaining.</returns>
    /// <example>
    /// <code>
    /// entity.UseDistributed("my_cluster", "events_local")
    ///       .WithShardingKey("cityHash64(UserId)")
    ///       .WithPolicy("ssd_policy");
    /// </code>
    /// </example>
    public static DistributedEngineBuilder<TEntity> UseDistributed<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string cluster,
        string table)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseDistributed(cluster, table);
        return new DistributedEngineBuilder<TEntity>(builder);
    }

    #endregion

    #region Partition By

    /// <summary>
    /// Configures the PARTITION BY expression for the table.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="partitionExpression">The partition expression (e.g., "toYYYYMM(created_at)").</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder HasPartitionBy(
        this EntityTypeBuilder builder,
        string partitionExpression)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(partitionExpression);

        builder.HasAnnotation(ClickHouseAnnotationNames.PartitionBy, partitionExpression);

        return builder;
    }

    /// <summary>
    /// Configures the PARTITION BY expression for the table.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="partitionExpression">The partition expression (e.g., "toYYYYMM(created_at)").</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> HasPartitionBy<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string partitionExpression)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).HasPartitionBy(partitionExpression);
        return builder;
    }

    /// <summary>
    /// Configures PARTITION BY using a monthly granularity on the specified column.
    /// Generates: PARTITION BY toYYYYMM(column)
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="columnExpression">Expression selecting the date/datetime column to partition by.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.HasPartitionByMonth(x => x.CreatedAt);
    /// // Generates: PARTITION BY toYYYYMM("CreatedAt")
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> HasPartitionByMonth<TEntity, TProperty>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, TProperty>> columnExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(columnExpression);
        var columnName = ExpressionExtensions.GetPropertyName(columnExpression);
        return builder.HasPartitionBy($"toYYYYMM(\"{columnName}\")");
    }

    /// <summary>
    /// Configures PARTITION BY using a daily granularity on the specified column.
    /// Generates: PARTITION BY toYYYYMMDD(column)
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="columnExpression">Expression selecting the date/datetime column to partition by.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.HasPartitionByDay(x => x.EventDate);
    /// // Generates: PARTITION BY toYYYYMMDD("EventDate")
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> HasPartitionByDay<TEntity, TProperty>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, TProperty>> columnExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(columnExpression);
        var columnName = ExpressionExtensions.GetPropertyName(columnExpression);
        return builder.HasPartitionBy($"toYYYYMMDD(\"{columnName}\")");
    }

    /// <summary>
    /// Configures PARTITION BY using a yearly granularity on the specified column.
    /// Generates: PARTITION BY toYear(column)
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="columnExpression">Expression selecting the date/datetime column to partition by.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.HasPartitionByYear(x => x.OrderDate);
    /// // Generates: PARTITION BY toYear("OrderDate")
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> HasPartitionByYear<TEntity, TProperty>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, TProperty>> columnExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(columnExpression);
        var columnName = ExpressionExtensions.GetPropertyName(columnExpression);
        return builder.HasPartitionBy($"toYear(\"{columnName}\")");
    }

    /// <summary>
    /// Configures PARTITION BY using an expression-based column selector.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="partitionExpression">Expression selecting the column to partition by.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.HasPartitionBy(x => x.Region);
    /// // Generates: PARTITION BY "Region"
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> HasPartitionBy<TEntity, TProperty>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, TProperty>> partitionExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(partitionExpression);
        var columnName = ExpressionExtensions.GetPropertyName(partitionExpression);
        return builder.HasPartitionBy($"\"{columnName}\"");
    }

    #endregion

    /// <summary>
    /// Configures the SAMPLE BY expression for the table.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="sampleExpression">The sample expression.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder HasSampleBy(
        this EntityTypeBuilder builder,
        string sampleExpression)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(sampleExpression);

        builder.HasAnnotation(ClickHouseAnnotationNames.SampleBy, sampleExpression);

        return builder;
    }

    /// <summary>
    /// Configures the SAMPLE BY expression for the table.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="sampleExpression">The sample expression.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> HasSampleBy<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string sampleExpression)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).HasSampleBy(sampleExpression);
        return builder;
    }

    /// <summary>
    /// Configures the SAMPLE BY expression using an expression-based column selector.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="sampleExpression">Expression selecting the column for SAMPLE BY.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.HasSampleBy(x => x.UserId);
    /// // Generates: SAMPLE BY "UserId"
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> HasSampleBy<TEntity, TProperty>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, TProperty>> sampleExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(sampleExpression);
        var columnName = ExpressionExtensions.GetPropertyName(sampleExpression);
        return builder.HasSampleBy($"\"{columnName}\"");
    }

    /// <summary>
    /// Configures the TTL expression for the table.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="ttlExpression">The TTL expression (e.g., "created_at + INTERVAL 1 MONTH").</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder HasTtl(
        this EntityTypeBuilder builder,
        string ttlExpression)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(ttlExpression);

        builder.HasAnnotation(ClickHouseAnnotationNames.Ttl, ttlExpression);

        return builder;
    }

    /// <summary>
    /// Configures the TTL expression for the table.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="ttlExpression">The TTL expression (e.g., "created_at + INTERVAL 1 MONTH").</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> HasTtl<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string ttlExpression)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).HasTtl(ttlExpression);
        return builder;
    }

    /// <summary>
    /// Configures TTL using a TimeSpan interval.
    /// Best for days, hours, minutes, and seconds.
    /// </summary>
    /// <remarks>
    /// <para>
    /// TimeSpan cannot accurately represent calendar months or years (which vary in length),
    /// so for those intervals use the <see cref="ClickHouseInterval"/> overload instead.
    /// </para>
    /// <para>
    /// The TimeSpan is converted to the largest whole unit that fits:
    /// days, hours, minutes, or seconds.
    /// </para>
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TProperty">The property type (should be DateTime or DateTimeOffset).</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="columnExpression">Expression selecting the datetime column for TTL.</param>
    /// <param name="expireAfter">How long after the column value before the row expires.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.HasTtl(x => x.CreatedAt, TimeSpan.FromDays(30));
    /// entity.HasTtl(x => x.CreatedAt, TimeSpan.FromHours(24));
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> HasTtl<TEntity, TProperty>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, TProperty>> columnExpression,
        TimeSpan expireAfter)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(columnExpression);

        var columnName = ExpressionExtensions.GetPropertyName(columnExpression);
        var interval = ConvertTimeSpanToClickHouseInterval(expireAfter);
        return builder.HasTtl($"\"{columnName}\" + {interval}");
    }

    /// <summary>
    /// Configures TTL using a ClickHouseInterval.
    /// Supports all interval units including months, quarters, and years.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use this overload when you need calendar-based intervals like months or years
    /// that cannot be accurately represented by <see cref="TimeSpan"/>.
    /// </para>
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TProperty">The property type (should be DateTime or DateTimeOffset).</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="columnExpression">Expression selecting the datetime column for TTL.</param>
    /// <param name="expireAfter">How long after the column value before the row expires.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.HasTtl(x => x.CreatedAt, ClickHouseInterval.Months(1));
    /// entity.HasTtl(x => x.CreatedAt, ClickHouseInterval.Years(1));
    /// entity.HasTtl(x => x.CreatedAt, ClickHouseInterval.Quarters(1));
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> HasTtl<TEntity, TProperty>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, TProperty>> columnExpression,
        ClickHouseInterval expireAfter)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(columnExpression);

        var columnName = ExpressionExtensions.GetPropertyName(columnExpression);
        return builder.HasTtl($"\"{columnName}\" + {expireAfter.ToSql()}");
    }

    /// <summary>
    /// Converts a TimeSpan to a ClickHouse INTERVAL expression string.
    /// Uses the largest whole unit that fits.
    /// </summary>
    private static string ConvertTimeSpanToClickHouseInterval(TimeSpan timeSpan)
    {
        if (timeSpan <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeSpan), "TTL interval must be positive.");
        }

        // Use largest whole unit that fits
        if (timeSpan.TotalDays >= 1 && timeSpan.TotalDays == Math.Floor(timeSpan.TotalDays))
        {
            return $"INTERVAL {(int)timeSpan.TotalDays} DAY";
        }
        if (timeSpan.TotalHours >= 1 && timeSpan.TotalHours == Math.Floor(timeSpan.TotalHours))
        {
            return $"INTERVAL {(int)timeSpan.TotalHours} HOUR";
        }
        if (timeSpan.TotalMinutes >= 1 && timeSpan.TotalMinutes == Math.Floor(timeSpan.TotalMinutes))
        {
            return $"INTERVAL {(int)timeSpan.TotalMinutes} MINUTE";
        }
        return $"INTERVAL {(int)timeSpan.TotalSeconds} SECOND";
    }

    /// <summary>
    /// Configures additional engine settings.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="settings">The settings dictionary.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> HasEngineSettings<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        IDictionary<string, string> settings)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(settings);

        builder.HasAnnotation(ClickHouseAnnotationNames.Settings, settings);

        return builder;
    }

    #region Materialized Views

    /// <summary>
    /// Configures the entity as a materialized view with a raw SQL SELECT query.
    /// This is the escape hatch for complex ClickHouse-specific queries that cannot be expressed in LINQ.
    /// </summary>
    /// <typeparam name="TEntity">The entity type representing the view's output schema.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="sourceTable">The name of the source table the view reads from.</param>
    /// <param name="selectSql">The raw SQL SELECT query for the view (without CREATE MATERIALIZED VIEW prefix).</param>
    /// <param name="populate">Whether to backfill existing data when creating the view.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <remarks>
    /// <para>
    /// ClickHouse materialized views are INSERT triggers, not cached query results.
    /// When data is inserted into the source table, the view's SELECT query transforms it
    /// and inserts the result into the view's storage.
    /// </para>
    /// <para>
    /// The entity should be configured as keyless using <c>HasNoKey()</c> since materialized views
    /// are typically append-only. Engine configuration (UseMergeTree, etc.) still applies.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// modelBuilder.Entity&lt;DailySummary&gt;(entity =>
    /// {
    ///     entity.ToTable("DailySummary_MV");
    ///     entity.HasNoKey();
    ///     entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });
    ///     entity.AsMaterializedViewRaw(
    ///         sourceTable: "Orders",
    ///         selectSql: @"
    ///             SELECT
    ///                 toDate(OrderDate) AS Date,
    ///                 ProductId,
    ///                 sum(Quantity) AS TotalQuantity,
    ///                 sum(Revenue) AS TotalRevenue
    ///             FROM Orders
    ///             GROUP BY Date, ProductId
    ///         ",
    ///         populate: false
    ///     );
    /// });
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> AsMaterializedViewRaw<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string sourceTable,
        string selectSql,
        bool populate = false)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(sourceTable);
        ArgumentException.ThrowIfNullOrEmpty(selectSql);

        // Mark as keyless - materialized views are typically append-only
        builder.HasNoKey();

        // Store view configuration as annotations
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, sourceTable);
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, selectSql.Trim());
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewPopulate, populate);

        return builder;
    }

    /// <summary>
    /// Configures the entity as a materialized view with a raw SQL SELECT query.
    /// Non-generic overload.
    /// </summary>
    public static EntityTypeBuilder AsMaterializedViewRaw(
        this EntityTypeBuilder builder,
        string sourceTable,
        string selectSql,
        bool populate = false)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(sourceTable);
        ArgumentException.ThrowIfNullOrEmpty(selectSql);

        builder.HasNoKey();
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, sourceTable);
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, selectSql.Trim());
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewPopulate, populate);

        return builder;
    }

    /// <summary>
    /// Configures the entity as a materialized view using a type-safe LINQ query expression.
    /// The LINQ expression is translated to ClickHouse SQL at configuration time.
    /// </summary>
    /// <typeparam name="TEntity">The entity type representing the view's output schema.</typeparam>
    /// <typeparam name="TSource">The entity type representing the source table.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="query">
    /// A LINQ expression that defines the view's transformation.
    /// Supports GroupBy, Select, and aggregate functions (Sum, Count, Min, Max, Average).
    /// </param>
    /// <param name="populate">Whether to backfill existing data when creating the view.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <remarks>
    /// <para>
    /// ClickHouse materialized views are INSERT triggers. When data is inserted into the source table,
    /// the view's query transforms it and inserts the result into the view's storage.
    /// </para>
    /// <para>
    /// The query expression is translated to ClickHouse SQL at configuration time, not at runtime.
    /// This provides compile-time type checking while generating optimized ClickHouse SQL.
    /// </para>
    /// <para>
    /// Engine configuration (UseMergeTree, UseSummingMergeTree, etc.) should be applied before
    /// calling this method.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// modelBuilder.Entity&lt;OrderDailySummary&gt;(entity =&gt;
    /// {
    ///     entity.ToTable("OrderDailySummary_MV");
    ///     entity.UseSummingMergeTree(x =&gt; new { x.Date, x.ProductId });
    ///     entity.AsMaterializedView&lt;OrderDailySummary, Order&gt;(
    ///         query: orders =&gt; orders
    ///             .GroupBy(o =&gt; new { Date = o.OrderDate.Date, o.ProductId })
    ///             .Select(g =&gt; new OrderDailySummary
    ///             {
    ///                 Date = g.Key.Date,
    ///                 ProductId = g.Key.ProductId,
    ///                 TotalQuantity = g.Sum(o =&gt; o.Quantity),
    ///                 TotalRevenue = g.Sum(o =&gt; o.Revenue)
    ///             }),
    ///         populate: false);
    /// });
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> AsMaterializedView<TEntity, TSource>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<IQueryable<TSource>, IQueryable<TEntity>>> query,
        bool populate = false)
        where TEntity : class
        where TSource : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(query);

        // Get source table name from model or entity type name
        var sourceEntityType = builder.Metadata.Model.FindEntityType(typeof(TSource));
        var sourceTableName = sourceEntityType?.GetTableName() ?? typeof(TSource).Name;

        // Translate LINQ expression to SQL immediately
        // This allows the SQL to be serialized in model snapshots
        var translator = new Query.Internal.MaterializedViewSqlTranslator(
            (IModel)builder.Metadata.Model,
            sourceTableName);
        var selectSql = translator.Translate<TSource, TEntity>(query);

        builder.HasNoKey();
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, sourceTableName);
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, selectSql);
        builder.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewPopulate, populate);

        return builder;
    }

    #endregion

    #region Dictionaries

    /// <summary>
    /// Configures this entity as a ClickHouse dictionary sourced from another table.
    /// </summary>
    /// <typeparam name="TDictionary">The dictionary entity type.</typeparam>
    /// <typeparam name="TSource">The source entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="configure">Action to configure the dictionary.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <remarks>
    /// <para>
    /// This method configures an entity to be created as a ClickHouse dictionary rather than a table.
    /// Dictionaries are in-memory key-value stores that provide fast lookups and are ideal for
    /// reference data like countries, currencies, or product categories.
    /// </para>
    /// <para>
    /// The dictionary entity should implement <see cref="Dictionaries.IClickHouseDictionary"/> as a marker interface.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// // Define dictionary entity
    /// public class CountryLookup : IClickHouseDictionary
    /// {
    ///     public ulong Id { get; set; }
    ///     public string Name { get; set; } = string.Empty;
    ///     public string IsoCode { get; set; } = string.Empty;
    /// }
    ///
    /// // Configure in OnModelCreating
    /// modelBuilder.Entity&lt;CountryLookup&gt;(entity =>
    /// {
    ///     entity.AsDictionary&lt;CountryLookup, Country&gt;(cfg => cfg
    ///         .HasKey(x => x.Id)
    ///         .FromTable(
    ///             projection: c => new CountryLookup
    ///             {
    ///                 Id = c.Id,
    ///                 Name = c.Name,
    ///                 IsoCode = c.IsoCode
    ///             },
    ///             filter: q => q.Where(c => c.IsActive))
    ///         .UseHashedLayout()
    ///         .HasLifetime(minSeconds: 60, maxSeconds: 300)
    ///         .HasDefault(x => x.Name, "Unknown"));
    /// });
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TDictionary> AsDictionary<TDictionary, TSource>(
        this EntityTypeBuilder<TDictionary> builder,
        Action<Dictionaries.DictionaryConfiguration<TDictionary, TSource>> configure)
        where TDictionary : class
        where TSource : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var configuration = new Dictionaries.DictionaryConfiguration<TDictionary, TSource>(builder);
        configure(configuration);
        configuration.Apply();

        return builder;
    }

    /// <summary>
    /// Configures this entity as a ClickHouse dictionary sourced from an external database (PostgreSQL, MySQL) or HTTP endpoint.
    /// </summary>
    /// <typeparam name="TDictionary">The dictionary entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="configure">Action to configure the dictionary.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <remarks>
    /// <para>
    /// This method configures an entity to be created as a ClickHouse dictionary with an external data source.
    /// Unlike <see cref="AsDictionary{TDictionary, TSource}"/> which sources from a ClickHouse table,
    /// this overload sources data from external databases (PostgreSQL, MySQL) or HTTP endpoints.
    /// </para>
    /// <para>
    /// External dictionaries are NOT created during EF Core migrations because they contain credentials.
    /// Instead, use <c>context.EnsureDictionariesAsync()</c> at application startup to create them with
    /// runtime-resolved credentials from environment variables or IConfiguration.
    /// </para>
    /// <para>
    /// The dictionary entity should implement <see cref="Dictionaries.IClickHouseDictionary"/> as a marker interface.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// // Define dictionary entity
    /// public class CountryLookup : IClickHouseDictionary
    /// {
    ///     public ulong Id { get; set; }
    ///     public string Name { get; set; } = string.Empty;
    ///     public string IsoCode { get; set; } = string.Empty;
    /// }
    ///
    /// // Configure in OnModelCreating
    /// modelBuilder.Entity&lt;CountryLookup&gt;(entity =>
    /// {
    ///     entity.AsDictionary&lt;CountryLookup&gt;(cfg => cfg
    ///         .HasKey(x => x.Id)
    ///         .FromPostgreSql(pg => pg
    ///             .FromTable("countries", schema: "public")
    ///             .Connection(c => c
    ///                 .HostPort(env: "PG_HOST")
    ///                 .Database(env: "PG_DATABASE")
    ///                 .Credentials("PG_USER", "PG_PASSWORD"))
    ///             .Where("is_active = true")
    ///             .InvalidateQuery("SELECT max(updated_at) FROM countries"))
    ///         .UseHashedLayout()
    ///         .HasLifetime(minSeconds: 60, maxSeconds: 300)
    ///         .HasDefault(x => x.Name, "Unknown"));
    /// });
    ///
    /// // Create dictionaries at startup (credentials resolved from config)
    /// await context.EnsureDictionariesAsync();
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TDictionary> AsDictionary<TDictionary>(
        this EntityTypeBuilder<TDictionary> builder,
        Action<Dictionaries.ExternalDictionaryConfiguration<TDictionary>> configure)
        where TDictionary : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var configuration = new Dictionaries.ExternalDictionaryConfiguration<TDictionary>(builder);
        configure(configuration);
        configuration.Apply();

        return builder;
    }

    #endregion

    #region Projections

    /// <summary>
    /// Starts configuring a projection with an auto-generated name.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <returns>A projection builder for configuring sort-order or aggregation projections.</returns>
    /// <remarks>
    /// <para>
    /// Projections are pre-computed aggregations or alternative sort orders stored alongside
    /// the main table. ClickHouse automatically maintains projections during INSERT operations
    /// and the query optimizer selects the best projection for each query.
    /// </para>
    /// <para>
    /// The projection name is auto-generated based on the table name and columns:
    /// - Sort-order: {TableName}_ord_{Column1}_{Column2}
    /// - Aggregation: {TableName}_agg_{Field1}_{Field2}
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// // Sort-order projection - auto-named: Orders_ord_CustomerId_OrderDate
    /// entity.HasProjection()
    ///     .OrderBy(x => x.CustomerId)
    ///     .ThenBy(x => x.OrderDate)
    ///     .Build();
    ///
    /// // Aggregation projection - auto-named: Orders_agg_Date_TotalAmount_OrderCount
    /// entity.HasProjection()
    ///     .GroupBy(x => x.OrderDate.Date)
    ///     .Select(g => new {
    ///         Date = g.Key,
    ///         TotalAmount = g.Sum(o => o.Amount),
    ///         OrderCount = g.Count()
    ///     })
    ///     .Build();
    /// </code>
    /// </example>
    public static Projections.ProjectionBuilder<TEntity> HasProjection<TEntity>(
        this EntityTypeBuilder<TEntity> builder)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        return new Projections.ProjectionBuilder<TEntity>(builder);
    }

    /// <summary>
    /// Starts configuring a projection with an explicit name.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="name">The projection name (unique within the table).</param>
    /// <returns>A projection builder for configuring sort-order or aggregation projections.</returns>
    /// <remarks>
    /// Projections are pre-computed aggregations or alternative sort orders stored alongside
    /// the main table. ClickHouse automatically maintains projections during INSERT operations
    /// and the query optimizer selects the best projection for each query.
    /// </remarks>
    /// <example>
    /// <code>
    /// // Sort-order projection with explicit name
    /// entity.HasProjection("prj_by_customer")
    ///     .OrderBy(x => x.CustomerId)
    ///     .ThenBy(x => x.OrderDate)
    ///     .Build();
    ///
    /// // Aggregation projection with explicit name
    /// entity.HasProjection("daily_stats")
    ///     .GroupBy(x => x.OrderDate.Date)
    ///     .Select(g => new {
    ///         Date = g.Key,
    ///         TotalAmount = g.Sum(o => o.Amount),
    ///         OrderCount = g.Count()
    ///     })
    ///     .Build();
    /// </code>
    /// </example>
    public static Projections.ProjectionBuilder<TEntity> HasProjection<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string name)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        return new Projections.ProjectionBuilder<TEntity>(builder, name);
    }

    /// <summary>
    /// Adds a raw SQL projection to the entity's table.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="name">The projection name (unique within the table).</param>
    /// <param name="selectSql">The raw SQL SELECT query for the projection.</param>
    /// <param name="materialize">Whether to materialize existing data (default: true).</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <remarks>
    /// Use this overload for complex projections that cannot be expressed using the fluent API.
    /// </remarks>
    /// <example>
    /// <code>
    /// entity.HasProjection(
    ///     "prj_by_region",
    ///     "SELECT * ORDER BY (\"Region\", \"OrderDate\")");
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> HasProjection<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string name,
        string selectSql,
        bool materialize = true)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(selectSql);

        AddProjectionWithValidation(builder, Projections.ProjectionDefinition.Raw(name, selectSql.Trim(), materialize));

        return builder;
    }

    /// <summary>
    /// Removes a projection from the entity's table configuration.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="name">The projection name to remove.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> RemoveProjection<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string name)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var annotation = builder.Metadata.FindAnnotation(ClickHouseAnnotationNames.Projections);
        if (annotation?.Value is List<Projections.ProjectionDefinition> projections)
        {
            projections.RemoveAll(p => p.Name == name);
            builder.HasAnnotation(ClickHouseAnnotationNames.Projections, projections);
        }

        return builder;
    }

    private static void AddProjectionWithValidation<TEntity>(
        EntityTypeBuilder<TEntity> builder,
        Projections.ProjectionDefinition projection) where TEntity : class
    {
        var annotation = builder.Metadata.FindAnnotation(ClickHouseAnnotationNames.Projections);
        var projections = annotation?.Value as List<Projections.ProjectionDefinition>
            ?? [];

        if (projections.Any(p => p.Name == projection.Name))
        {
            var tableName = builder.Metadata.GetTableName() ?? typeof(TEntity).Name;
            throw new InvalidOperationException(
                $"A projection named '{projection.Name}' already exists on table '{tableName}'. " +
                "Projection names must be unique within a table.");
        }

        projections.Add(projection);
        builder.HasAnnotation(ClickHouseAnnotationNames.Projections, projections);
    }

    #endregion
}
