using System.Linq.Expressions;
using EF.CH.Dictionaries;
using EF.CH.Metadata;
using EF.CH.Metadata.Internal;
using EF.CH.Query.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions;

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
/// // With expression-based sharding (raw SQL)
/// entity.UseDistributed("my_cluster", "events_local")
///       .WithShardingKeyExpression("cityHash64(UserId)")
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
    /// Configures the sharding key using a direct property-access expression.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Only direct member access is accepted (e.g. <c>x =&gt; x.UserId</c>). Anything
    /// else — function calls, casts, binary expressions — throws an
    /// <see cref="ArgumentException"/> at configuration time. For computed sharding
    /// keys such as <c>cityHash64(UserId)</c>, use
    /// <see cref="WithShardingKeyExpression(string)"/> instead.
    /// </para>
    /// </remarks>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="shardingKeySelector">Expression selecting the property to use as sharding key.</param>
    /// <returns>This builder for continued chaining.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="shardingKeySelector"/> is anything other than a
    /// direct property-access expression.
    /// </exception>
    public DistributedEngineBuilder<TEntity> WithShardingKey<TProperty>(
        Expression<Func<TEntity, TProperty>> shardingKeySelector)
    {
        ArgumentNullException.ThrowIfNull(shardingKeySelector);

        // Unwrap a single Convert/ConvertChecked node — Expression<Func<T, object>>
        // overloads always wrap value-typed members in a Convert.
        var body = shardingKeySelector.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
        {
            body = convert.Operand;
        }

        if (body is not MemberExpression member)
        {
            throw new ArgumentException(
                "WithShardingKey<TProperty> accepts only direct property access " +
                "(e.g. x => x.UserId). For computed sharding keys " +
                "(e.g. cityHash64(x.UserId)), use WithShardingKeyExpression(string).",
                nameof(shardingKeySelector));
        }

        // Member must be on the lambda parameter, not a constant capture or nested member.
        if (member.Expression is not ParameterExpression)
        {
            throw new ArgumentException(
                "WithShardingKey<TProperty> accepts only direct property access " +
                "on the lambda parameter (e.g. x => x.UserId). For computed sharding keys, " +
                "use WithShardingKeyExpression(string).",
                nameof(shardingKeySelector));
        }

        _builder.HasAnnotation(ClickHouseAnnotationNames.DistributedShardingKey, member.Member.Name);
        return this;
    }

    /// <summary>
    /// Configures the sharding key using a raw SQL expression.
    /// </summary>
    /// <remarks>
    /// Use this overload for hash-based or otherwise computed sharding keys, e.g.
    /// <c>"cityHash64(UserId)"</c>. The expression is emitted verbatim into the
    /// distributed engine's <c>ENGINE = Distributed(..., sharding_key)</c> clause.
    /// </remarks>
    /// <param name="shardingKeyExpression">The sharding key expression (e.g., "cityHash64(UserId)").</param>
    /// <returns>This builder for continued chaining.</returns>
    public DistributedEngineBuilder<TEntity> WithShardingKeyExpression(string shardingKeyExpression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(shardingKeyExpression);
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

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.MergeTree);
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
    /// // Replication is a property of the engine — chain WithReplication on any MergeTree builder:
    /// entity.UseMergeTree(x => x.Id).WithReplication("/clickhouse/tables/{uuid}");
    /// </code>
    /// </example>
    public static Engines.MergeTreeBuilder<TEntity> UseMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        builder.UseMergeTree(columns);
        return new Engines.MergeTreeBuilder<TEntity>(builder);
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

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.ReplacingMergeTree);
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
    public static Engines.ReplacingMergeTreeBuilder<TEntity> UseReplacingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        builder.UseReplacingMergeTree(columns);
        return new Engines.ReplacingMergeTreeBuilder<TEntity>(builder);
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

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.SummingMergeTree);
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

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.AggregatingMergeTree);
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
    public static Engines.SummingMergeTreeBuilder<TEntity> UseSummingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        builder.UseSummingMergeTree(columns);
        return new Engines.SummingMergeTreeBuilder<TEntity>(builder);
    }

    /// <summary>
    /// Configures the entity to use an AggregatingMergeTree engine with the specified ORDER BY columns.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="orderByExpression">Expression selecting the ORDER BY columns.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static Engines.AggregatingMergeTreeBuilder<TEntity> UseAggregatingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(orderByExpression);
        var columns = ExpressionExtensions.GetPropertyNames(orderByExpression);
        builder.UseAggregatingMergeTree(columns);
        return new Engines.AggregatingMergeTreeBuilder<TEntity>(builder);
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
    /// entity.UseCollapsingMergeTree(x => new { x.UserId, x.EventTime })
    ///       .WithSign(x => x.Sign);
    /// </code>
    /// </example>
    public static Engines.CollapsingMergeTreeBuilder<TEntity> UseCollapsingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(orderByExpression);

        var orderByColumns = ExpressionExtensions.GetPropertyNames(orderByExpression);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for CollapsingMergeTree.", nameof(orderByExpression));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.CollapsingMergeTree);
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return new Engines.CollapsingMergeTreeBuilder<TEntity>(builder);
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

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.CollapsingMergeTree);
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
    /// entity.UseVersionedCollapsingMergeTree(x => new { x.UserId, x.EventTime })
    ///       .WithSign(x => x.Sign)
    ///       .WithVersion(x => x.Version);
    /// </code>
    /// </example>
    public static Engines.VersionedCollapsingMergeTreeBuilder<TEntity> UseVersionedCollapsingMergeTree<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, object>> orderByExpression)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(orderByExpression);

        var orderByColumns = ExpressionExtensions.GetPropertyNames(orderByExpression);

        if (orderByColumns.Length == 0)
        {
            throw new ArgumentException("At least one ORDER BY column is required for VersionedCollapsingMergeTree.", nameof(orderByExpression));
        }

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.VersionedCollapsingMergeTree);
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return new Engines.VersionedCollapsingMergeTreeBuilder<TEntity>(builder);
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

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.VersionedCollapsingMergeTree);
        builder.HasAnnotation(ClickHouseAnnotationNames.SignColumn, signColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.VersionColumn, versionColumn);
        builder.HasAnnotation(ClickHouseAnnotationNames.OrderBy, orderByColumns);

        return builder;
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
    /// Configures this entity to emit <c>ON CLUSTER '{cluster}'</c>, deferring
    /// cluster resolution to the server's <c>&lt;macros&gt;</c> config.
    /// </summary>
    public static EntityTypeBuilder UseCluster(this EntityTypeBuilder builder)
        => builder.UseCluster(ClickHouseClusterMacros.Cluster);

    /// <inheritdoc cref="UseCluster(EntityTypeBuilder)"/>
    public static EntityTypeBuilder<TEntity> UseCluster<TEntity>(this EntityTypeBuilder<TEntity> builder)
        where TEntity : class
        => builder.UseCluster(ClickHouseClusterMacros.Cluster);

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
    ///     e.UseReplacingMergeTree(x => x.Id)
    ///         .WithVersion(x => x.Version)
    ///         .WithReplication("/clickhouse/tables/{uuid}");
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
    ///     modelBuilder.MaterializedView&lt;HourlySummary&gt;().From&lt;RawEvent&gt;().DefinedAs(...);
    /// });
    /// </code>
    /// </example>
    public static EntityTypeBuilder UseNullEngine(this EntityTypeBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.Null);
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

    #region Memory Engine

    /// <summary>
    /// Configures the entity to use the Memory engine.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The Memory engine stores data in RAM. Data is lost when the server restarts.
    /// </para>
    /// <para>
    /// Good for temporary lookup tables, small reference data, and testing scenarios
    /// where persistence is not required.
    /// </para>
    /// </remarks>
    /// <param name="builder">The entity type builder.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseMemoryEngine(this EntityTypeBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.Memory);
        return builder;
    }

    /// <summary>
    /// Configures the entity to use the Memory engine.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The Memory engine stores data in RAM. Data is lost when the server restarts.
    /// </para>
    /// <para>
    /// Good for temporary lookup tables, small reference data, and testing scenarios
    /// where persistence is not required.
    /// </para>
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseMemoryEngine<TEntity>(
        this EntityTypeBuilder<TEntity> builder)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseMemoryEngine();
        return builder;
    }

    #endregion

    #region Log Engine

    /// <summary>
    /// Configures the entity to use the Log engine.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The Log engine provides simple sequential storage for small tables (up to ~1 million rows).
    /// It supports concurrent reads but only sequential writes.
    /// </para>
    /// </remarks>
    /// <param name="builder">The entity type builder.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseLogEngine(this EntityTypeBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.Log);
        return builder;
    }

    /// <summary>
    /// Configures the entity to use the Log engine.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The Log engine provides simple sequential storage for small tables (up to ~1 million rows).
    /// It supports concurrent reads but only sequential writes.
    /// </para>
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseLogEngine<TEntity>(
        this EntityTypeBuilder<TEntity> builder)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseLogEngine();
        return builder;
    }

    #endregion

    #region TinyLog Engine

    /// <summary>
    /// Configures the entity to use the TinyLog engine.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The TinyLog engine is the simplest storage engine, storing each column in a separate file.
    /// It does not support concurrent data access.
    /// </para>
    /// </remarks>
    /// <param name="builder">The entity type builder.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseTinyLogEngine(this EntityTypeBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.TinyLog);
        return builder;
    }

    /// <summary>
    /// Configures the entity to use the TinyLog engine.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The TinyLog engine is the simplest storage engine, storing each column in a separate file.
    /// It does not support concurrent data access.
    /// </para>
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseTinyLogEngine<TEntity>(
        this EntityTypeBuilder<TEntity> builder)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseTinyLogEngine();
        return builder;
    }

    #endregion

    #region StripeLog Engine

    /// <summary>
    /// Configures the entity to use the StripeLog engine.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The StripeLog engine stores all columns in one file. Good for small tables with many columns.
    /// </para>
    /// </remarks>
    /// <param name="builder">The entity type builder.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseStripeLogEngine(this EntityTypeBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.StripeLog);
        return builder;
    }

    /// <summary>
    /// Configures the entity to use the StripeLog engine.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The StripeLog engine stores all columns in one file. Good for small tables with many columns.
    /// </para>
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseStripeLogEngine<TEntity>(
        this EntityTypeBuilder<TEntity> builder)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseStripeLogEngine();
        return builder;
    }

    #endregion

    #region KeeperMap Engine

    /// <summary>
    /// Configures the entity to use the KeeperMap engine, a linearly-consistent key-value store
    /// backed by ClickHouse Keeper / ZooKeeper.
    /// </summary>
    /// <remarks>
    /// <para>
    /// KeeperMap requires exactly one PRIMARY KEY column and does not support ORDER BY,
    /// PARTITION BY, TTL, or SAMPLE BY clauses.
    /// </para>
    /// <para>
    /// The <paramref name="rootPath"/> is a path in Keeper/ZooKeeper under which the table's
    /// keys are stored; all replicas sharing the same root path form a single logical table.
    /// </para>
    /// </remarks>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="rootPath">The Keeper/ZooKeeper root path for this table.</param>
    /// <param name="primaryKeyColumn">The single PRIMARY KEY column name.</param>
    /// <param name="keysLimit">Optional per-shard maximum key count.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder UseKeeperMapEngine(
        this EntityTypeBuilder builder,
        string rootPath,
        string primaryKeyColumn,
        ulong? keysLimit = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(rootPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(primaryKeyColumn);

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.KeeperMap);
        builder.HasAnnotation(ClickHouseAnnotationNames.KeeperMapRootPath, rootPath);
        builder.HasAnnotation(ClickHouseAnnotationNames.PrimaryKey, new[] { primaryKeyColumn });
        if (keysLimit.HasValue)
            builder.HasAnnotation(ClickHouseAnnotationNames.KeeperMapKeysLimit, keysLimit.Value);

        return builder;
    }

    /// <summary>
    /// Configures the entity to use the KeeperMap engine.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="rootPath">The Keeper/ZooKeeper root path for this table.</param>
    /// <param name="primaryKeyColumn">The single PRIMARY KEY column name.</param>
    /// <param name="keysLimit">Optional per-shard maximum key count.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder<TEntity> UseKeeperMapEngine<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string rootPath,
        string primaryKeyColumn,
        ulong? keysLimit = null)
        where TEntity : class
    {
        ((EntityTypeBuilder)builder).UseKeeperMapEngine(rootPath, primaryKeyColumn, keysLimit);
        return builder;
    }

    /// <summary>
    /// Configures the entity to use the KeeperMap engine, selecting the PRIMARY KEY column via expression.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TKey">The PRIMARY KEY column type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="rootPath">The Keeper/ZooKeeper root path for this table.</param>
    /// <param name="primaryKey">Expression selecting the single PRIMARY KEY column.</param>
    /// <param name="keysLimit">Optional per-shard maximum key count.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.UseKeeperMapEngine("/clickhouse/kv/users", x => x.Id);
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> UseKeeperMapEngine<TEntity, TKey>(
        this EntityTypeBuilder<TEntity> builder,
        string rootPath,
        Expression<Func<TEntity, TKey>> primaryKey,
        ulong? keysLimit = null)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(primaryKey);
        var column = ExpressionExtensions.GetPropertyName(primaryKey);
        return builder.UseKeeperMapEngine(rootPath, column, keysLimit);
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

        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.Distributed);
        builder.HasAnnotation(ClickHouseAnnotationNames.DistributedCluster, cluster);
        builder.HasAnnotation(ClickHouseAnnotationNames.DistributedDatabase, database);
        builder.HasAnnotation(ClickHouseAnnotationNames.DistributedTable, table);

        return builder;
    }

    #region Parameterised views

    /// <summary>
    /// Marks this entity as a ClickHouse parameterised view. The type's DbSet
    /// can be queried via <see cref="ClickHouseQueryableParameterExtensions.WithParameter"/>.
    /// </summary>
    public static EntityTypeBuilder<TEntity> ToParameterizedView<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string viewName) where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);
        builder.HasNoKey();
        builder.ToView(viewName);
        builder.HasAnnotation(EF.CH.Metadata.ClickHouseAnnotationNames.ParameterizedView, true);
        return builder;
    }

    #endregion

    #region Nested columns

    /// <summary>
    /// Declares that the given collection property maps to a ClickHouse
    /// <c>Nested(...)</c> column — parallel arrays under a shared prefix
    /// (e.g. <c>Participants.name</c>, <c>Participants.age</c>). Call
    /// <see cref="NestedColumnBuilder{TNested}.WithParallelAccess"/> to tag
    /// the property for parallel-array codegen (arrayMap / arrayZip / indexed
    /// access target the sub-columns directly).
    /// </summary>
    public static NestedColumnBuilder<TNested> HasNested<TEntity, TNested>(
        this EntityTypeBuilder<TEntity> builder,
        System.Linq.Expressions.Expression<Func<TEntity, IEnumerable<TNested>>> navigation)
        where TEntity : class
        where TNested : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(navigation);
        return new NestedColumnBuilder<TNested>(builder.Metadata, navigation);
    }

    #endregion

    #region External integration engines

    /// <summary>
    /// <c>ENGINE = PostgreSQL(host:port, database, table, user, password [, schema [, on_conflict]])</c>.
    /// </summary>
    public static EntityTypeBuilder<TEntity> UsePostgreSqlEngine<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string hostAndPort, string database, string table, string user, string password,
        string? schema = null) where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(hostAndPort);
        ArgumentException.ThrowIfNullOrWhiteSpace(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        ArgumentException.ThrowIfNullOrWhiteSpace(user);
        var parts = new List<string> { SqlLiteral(hostAndPort), SqlLiteral(database), SqlLiteral(table), SqlLiteral(user), SqlLiteral(password ?? "") };
        if (!string.IsNullOrEmpty(schema)) parts.Add(SqlLiteral(schema));
        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.PostgreSQL);
        builder.HasAnnotation(ClickHouseAnnotationNames.ExternalEngineArguments, string.Join(", ", parts));
        return builder;
    }

    /// <summary>
    /// <c>ENGINE = MySQL(host:port, database, table, user, password)</c>.
    /// </summary>
    public static EntityTypeBuilder<TEntity> UseMySqlEngine<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string hostAndPort, string database, string table, string user, string password) where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(hostAndPort);
        ArgumentException.ThrowIfNullOrWhiteSpace(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        ArgumentException.ThrowIfNullOrWhiteSpace(user);
        var args = string.Join(", ", new[] { hostAndPort, database, table, user, password ?? "" }.Select(SqlLiteral));
        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.MySQL);
        builder.HasAnnotation(ClickHouseAnnotationNames.ExternalEngineArguments, args);
        return builder;
    }

    /// <summary>
    /// <c>ENGINE = Redis(host:port [, db_index [, password [, pool_size]]])</c>.
    /// Primary key is required — pass via <see cref="EntityTypeBuilder.HasKey"/>.
    /// </summary>
    public static EntityTypeBuilder<TEntity> UseRedisEngine<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string hostAndPort, int dbIndex = 0, string? password = null, int? poolSize = null) where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(hostAndPort);
        var parts = new List<string> { SqlLiteral(hostAndPort), dbIndex.ToString(System.Globalization.CultureInfo.InvariantCulture) };
        if (password is not null || poolSize is not null)
        {
            parts.Add(SqlLiteral(password ?? ""));
            if (poolSize is not null)
                parts.Add(poolSize.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.Redis);
        builder.HasAnnotation(ClickHouseAnnotationNames.ExternalEngineArguments, string.Join(", ", parts));
        return builder;
    }

    /// <summary>
    /// <c>ENGINE = ODBC(connection_string, database, table)</c>.
    /// </summary>
    public static EntityTypeBuilder<TEntity> UseOdbcEngine<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string connectionString, string externalDatabase, string externalTable) where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(externalDatabase);
        ArgumentException.ThrowIfNullOrWhiteSpace(externalTable);
        builder.HasAnnotation(ClickHouseAnnotationNames.Engine, ClickHouseEngineNames.ODBC);
        builder.HasAnnotation(ClickHouseAnnotationNames.ExternalEngineArguments,
            string.Join(", ", new[] { connectionString, externalDatabase, externalTable }.Select(SqlLiteral)));
        return builder;
    }

    private static string SqlLiteral(string value) => "'" + value.Replace("'", "''") + "'";

    #endregion

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
    ///       .WithShardingKeyExpression("cityHash64(UserId)")
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
    /// Configures PARTITION BY using an expression-based column selector.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="partitionExpression">Expression selecting the column to partition by.</param>
    /// <param name="granularity">Optional date-bucket wrapper applied to the column.
    /// <see cref="PartitionGranularity.None"/> emits the raw column;
    /// other values wrap it in the matching ClickHouse function.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <example>
    /// <code>
    /// entity.HasPartitionBy(x => x.Region);
    /// // Generates: PARTITION BY "Region"
    ///
    /// entity.HasPartitionBy(x => x.CreatedAt, PartitionGranularity.Month);
    /// // Generates: PARTITION BY toYYYYMM("CreatedAt")
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> HasPartitionBy<TEntity, TProperty>(
        this EntityTypeBuilder<TEntity> builder,
        Expression<Func<TEntity, TProperty>> partitionExpression,
        PartitionGranularity granularity = PartitionGranularity.None)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(partitionExpression);
        var columnName = ExpressionExtensions.GetPropertyName(partitionExpression);
        var quoted = $"\"{columnName}\"";
        var expr = granularity switch
        {
            PartitionGranularity.None    => quoted,
            PartitionGranularity.Hour    => $"toStartOfHour({quoted})",
            PartitionGranularity.Day     => $"toYYYYMMDD({quoted})",
            PartitionGranularity.Week    => $"toStartOfWeek({quoted})",
            PartitionGranularity.Month   => $"toYYYYMM({quoted})",
            PartitionGranularity.Quarter => $"toStartOfQuarter({quoted})",
            PartitionGranularity.Year    => $"toYear({quoted})",
            _ => throw new ArgumentOutOfRangeException(nameof(granularity)),
        };
        return builder.HasPartitionBy(expr);
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

    #region Parameterized Views

    /// <summary>
    /// Configures the entity as a result type for a ClickHouse parameterized view.
    /// </summary>
    /// <typeparam name="TEntity">The entity type representing the view's output schema.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="viewName">The name of the parameterized view.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <remarks>
    /// <para>
    /// This method configures the entity as keyless and marks it as a parameterized view result type.
    /// The entity can then be queried using <see cref="ClickHouseParameterizedViewExtensions.FromParameterizedView{TResult}"/>.
    /// </para>
    /// <para>
    /// Parameterized views in ClickHouse use the syntax <c>{name:Type}</c> in the view definition
    /// and are queried with <c>SELECT * FROM view_name(param=value, ...)</c>.
    /// </para>
    /// <para>
    /// Note: This method does not create the view itself. Use <see cref="ClickHouseMigrationBuilderExtensions.CreateParameterizedView"/>
    /// in a migration or raw SQL to create the view.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// // Configure the result entity
    /// modelBuilder.Entity&lt;UserEventView&gt;(entity =>
    /// {
    ///     entity.HasParameterizedView("user_events_view");
    /// });
    ///
    /// // Query the view
    /// var events = context.FromParameterizedView&lt;UserEventView&gt;(
    ///     "user_events_view",
    ///     new { user_id = 123UL, start_date = DateTime.Today })
    ///     .Where(e => e.EventType == "click")
    ///     .ToListAsync();
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> HasParameterizedView<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string viewName)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);

        // Mark as keyless since views don't have keys
        builder.HasNoKey();

        // Store view configuration as annotations
        builder.HasAnnotation(ClickHouseAnnotationNames.ParameterizedView, true);
        builder.HasAnnotation(ClickHouseAnnotationNames.ParameterizedViewName, viewName);

        return builder;
    }

    /// <summary>
    /// Configures the entity as a result type for a ClickHouse parameterized view.
    /// Non-generic overload.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="viewName">The name of the parameterized view.</param>
    /// <returns>The entity type builder for chaining.</returns>
    public static EntityTypeBuilder HasParameterizedView(
        this EntityTypeBuilder builder,
        string viewName)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);

        builder.HasNoKey();
        builder.HasAnnotation(ClickHouseAnnotationNames.ParameterizedView, true);
        builder.HasAnnotation(ClickHouseAnnotationNames.ParameterizedViewName, viewName);

        return builder;
    }

    /// <summary>
    /// Configures the entity as a parameterized view using a fluent builder pattern.
    /// </summary>
    /// <typeparam name="TView">The view result entity type.</typeparam>
    /// <typeparam name="TSource">The source entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="configure">Action to configure the parameterized view.</param>
    /// <returns>The entity type builder for chaining.</returns>
    /// <remarks>
    /// <para>
    /// This method provides a type-safe way to configure parameterized views
    /// instead of writing raw SQL. The configuration is stored as annotations
    /// and can be used to generate CREATE VIEW DDL.
    /// </para>
    /// <para>
    /// Use <see cref="ClickHouseDatabaseExtensions.EnsureParameterizedViewsAsync"/> to create
    /// all configured views at runtime.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// modelBuilder.Entity&lt;UserEventView&gt;(entity =>
    /// {
    ///     entity.AsParameterizedView&lt;UserEventView, Event&gt;(cfg => cfg
    ///         .FromTable()
    ///         .Select(e => new UserEventView
    ///         {
    ///             EventId = e.EventId,
    ///             EventType = e.EventType,
    ///             UserId = e.UserId,
    ///             Timestamp = e.Timestamp
    ///         })
    ///         .Parameter&lt;ulong&gt;("user_id")
    ///         .Parameter&lt;DateTime&gt;("start_date")
    ///         .Where((e, p) => e.UserId == p.Get&lt;ulong&gt;("user_id"))
    ///         .Where((e, p) => e.Timestamp >= p.Get&lt;DateTime&gt;("start_date")));
    /// });
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TView> AsParameterizedView<TView, TSource>(
        this EntityTypeBuilder<TView> builder,
        Action<ParameterizedViews.ParameterizedViewConfiguration<TView, TSource>> configure)
        where TView : class
        where TSource : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var configuration = new ParameterizedViews.ParameterizedViewConfiguration<TView, TSource>();
        configure(configuration);

        // Get or generate view name
        var viewName = configuration.ViewName ?? ConvertToSnakeCase(typeof(TView).Name);

        // Mark as keyless (views don't have keys)
        builder.HasNoKey();

        // Store basic annotations
        builder.HasAnnotation(ClickHouseAnnotationNames.ParameterizedView, true);
        builder.HasAnnotation(ClickHouseAnnotationNames.ParameterizedViewName, viewName);

        // Build and store the full metadata
        var metadata = configuration.BuildMetadata(viewName);
        builder.HasAnnotation(ClickHouseAnnotationNames.ParameterizedViewMetadata, metadata);

        return builder;
    }

    /// <summary>
    /// Converts PascalCase to snake_case.
    /// </summary>
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

    #endregion

    #region Plain Views

    /// <summary>
    /// Configures the entity as a result type for a plain (non-parameterized, non-materialized)
    /// ClickHouse view. Marks the entity keyless and stores the view name as an annotation.
    /// </summary>
    /// <typeparam name="TEntity">The entity type representing the view's output schema.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="viewName">The view name in ClickHouse.</param>
    /// <param name="schema">Optional schema (database) qualifier.</param>
    /// <example>
    /// <code>
    /// modelBuilder.Entity&lt;ActiveUserView&gt;(entity =>
    /// {
    ///     entity.HasView("active_users");
    /// });
    ///
    /// var rows = await context.FromView&lt;ActiveUserView&gt;("active_users").ToListAsync();
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TEntity> HasView<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string viewName,
        string? schema = null)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);

        builder.HasNoKey();
        builder.ToView(viewName, schema);

        builder.HasAnnotation(ClickHouseAnnotationNames.View, true);
        builder.HasAnnotation(ClickHouseAnnotationNames.ViewName, viewName);
        if (!string.IsNullOrEmpty(schema))
        {
            builder.HasAnnotation(ClickHouseAnnotationNames.ViewSchema, schema);
        }

        return builder;
    }

    /// <summary>
    /// Non-generic overload of <see cref="HasView{TEntity}(EntityTypeBuilder{TEntity}, string, string?)"/>.
    /// </summary>
    public static EntityTypeBuilder HasView(
        this EntityTypeBuilder builder,
        string viewName,
        string? schema = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);

        builder.HasNoKey();
        builder.ToView(viewName, schema);

        builder.HasAnnotation(ClickHouseAnnotationNames.View, true);
        builder.HasAnnotation(ClickHouseAnnotationNames.ViewName, viewName);
        if (!string.IsNullOrEmpty(schema))
        {
            builder.HasAnnotation(ClickHouseAnnotationNames.ViewSchema, schema);
        }

        return builder;
    }

    /// <summary>
    /// Configures the entity as a plain ClickHouse view from a raw SELECT SQL string.
    /// </summary>
    /// <typeparam name="TEntity">The view result entity type.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="viewName">The view name in ClickHouse.</param>
    /// <param name="selectSql">The SELECT SQL body for the view (without CREATE VIEW prefix).</param>
    /// <param name="ifNotExists">Emit IF NOT EXISTS in CREATE VIEW DDL.</param>
    /// <param name="orReplace">Emit OR REPLACE in CREATE VIEW DDL. Mutually exclusive with <paramref name="ifNotExists"/>.</param>
    /// <param name="onCluster">Optional ON CLUSTER cluster name.</param>
    /// <param name="schema">Optional schema (database) qualifier.</param>
    public static EntityTypeBuilder<TEntity> AsViewRaw<TEntity>(
        this EntityTypeBuilder<TEntity> builder,
        string viewName,
        string selectSql,
        bool ifNotExists = false,
        bool orReplace = false,
        string? onCluster = null,
        string? schema = null)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);
        ArgumentException.ThrowIfNullOrWhiteSpace(selectSql);

        if (ifNotExists && orReplace)
        {
            throw new ArgumentException(
                "ClickHouse CREATE VIEW does not allow combining IF NOT EXISTS with OR REPLACE.",
                nameof(orReplace));
        }

        builder.HasNoKey();
        builder.ToView(viewName, schema);

        var metadata = new Views.ViewMetadataBase
        {
            ViewName = viewName,
            ResultType = typeof(TEntity),
            RawSelectSql = selectSql,
            IfNotExists = ifNotExists,
            OrReplace = orReplace,
            OnCluster = onCluster,
            Schema = schema
        };

        builder.HasAnnotation(ClickHouseAnnotationNames.View, true);
        builder.HasAnnotation(ClickHouseAnnotationNames.ViewName, viewName);
        builder.HasAnnotation(ClickHouseAnnotationNames.ViewMetadata, metadata);
        if (ifNotExists)
            builder.HasAnnotation(ClickHouseAnnotationNames.ViewIfNotExists, true);
        if (orReplace)
            builder.HasAnnotation(ClickHouseAnnotationNames.ViewOrReplace, true);
        if (!string.IsNullOrEmpty(onCluster))
            builder.HasAnnotation(ClickHouseAnnotationNames.ViewCluster, onCluster);
        if (!string.IsNullOrEmpty(schema))
            builder.HasAnnotation(ClickHouseAnnotationNames.ViewSchema, schema);

        return builder;
    }

    /// <summary>
    /// Configures the entity as a plain ClickHouse view using a fluent builder.
    /// </summary>
    /// <typeparam name="TView">The view result entity type.</typeparam>
    /// <typeparam name="TSource">The source entity type the view selects from.</typeparam>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="configure">Action to configure the view.</param>
    /// <example>
    /// <code>
    /// modelBuilder.Entity&lt;ActiveUserView&gt;(entity =>
    /// {
    ///     entity.AsView&lt;ActiveUserView, User&gt;(cfg => cfg
    ///         .FromTable()
    ///         .Select(u => new ActiveUserView { UserId = u.UserId, Name = u.Name })
    ///         .Where(u => u.IsActive)
    ///         .OrReplace());
    /// });
    /// </code>
    /// </example>
    public static EntityTypeBuilder<TView> AsView<TView, TSource>(
        this EntityTypeBuilder<TView> builder,
        Action<Views.ViewConfiguration<TView, TSource>> configure)
        where TView : class
        where TSource : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var configuration = new Views.ViewConfiguration<TView, TSource>();
        configure(configuration);

        var viewName = configuration.ViewName ?? ConvertToSnakeCase(typeof(TView).Name);

        builder.HasNoKey();
        builder.ToView(viewName, configuration.Schema);

        var metadata = configuration.BuildMetadata(viewName);

        builder.HasAnnotation(ClickHouseAnnotationNames.View, true);
        builder.HasAnnotation(ClickHouseAnnotationNames.ViewName, viewName);
        builder.HasAnnotation(ClickHouseAnnotationNames.ViewMetadata, metadata);
        if (metadata.IfNotExists)
            builder.HasAnnotation(ClickHouseAnnotationNames.ViewIfNotExists, true);
        if (metadata.OrReplace)
            builder.HasAnnotation(ClickHouseAnnotationNames.ViewOrReplace, true);
        if (!string.IsNullOrEmpty(metadata.OnCluster))
            builder.HasAnnotation(ClickHouseAnnotationNames.ViewCluster, metadata.OnCluster);
        if (!string.IsNullOrEmpty(metadata.Schema))
            builder.HasAnnotation(ClickHouseAnnotationNames.ViewSchema, metadata.Schema);

        return builder;
    }

    /// <summary>
    /// Marks a view-mapped entity as deferred so that <c>EnsureViewsAsync</c> skips it
    /// and the caller can deploy the view manually (e.g. after the source is seeded).
    /// </summary>
    public static EntityTypeBuilder<TEntity> AsViewDeferred<TEntity>(
        this EntityTypeBuilder<TEntity> builder)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.HasAnnotation(ClickHouseAnnotationNames.ViewDeferred, true);
        return builder;
    }

    #endregion
}

/// <summary>
/// Builder returned by <c>EntityTypeBuilder&lt;T&gt;.HasNested(...)</c>. Marks the
/// navigation as a ClickHouse <c>Nested</c> column with parallel-array access
/// semantics. The actual column layout is already produced by the Nested type
/// mapping; this builder is the declarative hook users call when they intend
/// to reach the sub-columns via <c>Participants.name</c> / <c>.age</c>.
/// </summary>
public sealed class NestedColumnBuilder<TNested> where TNested : class
{
    private readonly Microsoft.EntityFrameworkCore.Metadata.IMutableEntityType _entityType;
    private readonly System.Linq.Expressions.LambdaExpression _navigation;

    internal NestedColumnBuilder(
        Microsoft.EntityFrameworkCore.Metadata.IMutableEntityType entityType,
        System.Linq.Expressions.LambdaExpression navigation)
    {
        _entityType = entityType;
        _navigation = navigation;
    }

    /// <summary>
    /// Tags the Nested column as parallel-array-accessible. The annotation is
    /// read by downstream LINQ translators when generating SQL that addresses
    /// individual sub-columns.
    /// </summary>
    public NestedColumnBuilder<TNested> WithParallelAccess()
    {
        var memberName = (_navigation.Body as System.Linq.Expressions.MemberExpression)?.Member.Name
            ?? "Nested";
        _entityType.AddAnnotation(
            EF.CH.Metadata.ClickHouseAnnotationNames.NestedParallelAccess + ":" + memberName,
            true);
        return this;
    }
}
