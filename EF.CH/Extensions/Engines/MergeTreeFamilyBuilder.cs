using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions.Engines;

/// <summary>
/// Shared base for fluent builders returned by every <c>Use*MergeTree</c>
/// extension. Provides cluster, table-group, and replication knobs that apply
/// to any MergeTree-family engine — replication is a property of the engine,
/// not a separate kind of engine. The CRTP <typeparamref name="TBuilder"/>
/// parameter lets each chained call return the concrete leaf builder so
/// engine-specific knobs (<c>WithVersion</c>, <c>WithSign</c>, etc.) remain
/// reachable in any order.
/// </summary>
/// <typeparam name="TBuilder">The concrete leaf builder type.</typeparam>
/// <typeparam name="TEntity">The entity type being configured.</typeparam>
public abstract class MergeTreeFamilyBuilder<TBuilder, TEntity>
    where TBuilder : MergeTreeFamilyBuilder<TBuilder, TEntity>
    where TEntity : class
{
    /// <summary>The underlying entity-type builder this fluent wrapper writes annotations on.</summary>
    protected EntityTypeBuilder<TEntity> Builder { get; }

    private TBuilder Self => (TBuilder)this;

    /// <summary>Initializes the builder with the underlying entity-type builder.</summary>
    /// <param name="builder">The wrapped <see cref="EntityTypeBuilder{TEntity}"/>.</param>
    protected MergeTreeFamilyBuilder(EntityTypeBuilder<TEntity> builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        Builder = builder;
    }

    /// <summary>
    /// Marks the engine as replicated. The configured <paramref name="zooKeeperPath"/>
    /// and <paramref name="replicaName"/> are emitted as the first two engine
    /// arguments at SQL-generation time, e.g.
    /// <c>ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}', '{replica}', "Version")</c>.
    /// </summary>
    /// <param name="zooKeeperPath">ZooKeeper / Keeper path. Supports placeholders <c>{database}</c>, <c>{table}</c>, <c>{uuid}</c>.</param>
    /// <param name="replicaName">Replica name, usually <c>{replica}</c> for server-side macro expansion.</param>
    public TBuilder WithReplication(string zooKeeperPath, string replicaName = "{replica}")
    {
        Builder.HasReplication(zooKeeperPath, replicaName);
        Builder.HasAnnotation(ClickHouseAnnotationNames.IsReplicated, true);
        return Self;
    }

    /// <summary>
    /// Configures the cluster name for this entity's DDL operations
    /// (<c>ON CLUSTER '&lt;name&gt;'</c>).
    /// </summary>
    /// <param name="clusterName">Cluster name as defined in ClickHouse server config.</param>
    public TBuilder WithCluster(string clusterName)
    {
        Builder.UseCluster(clusterName);
        return Self;
    }

    /// <summary>
    /// Emits <c>ON CLUSTER '{cluster}'</c>, deferring cluster resolution to the
    /// server's <c>&lt;macros&gt;&lt;cluster&gt;</c> entry. Use when DDL should
    /// run against whichever cluster the target node belongs to without
    /// hardcoding a cluster name.
    /// </summary>
    public TBuilder WithCluster()
    {
        Builder.UseCluster();
        return Self;
    }

    /// <summary>Assigns this entity to a table group from configuration.</summary>
    /// <param name="tableGroupName">Table group name.</param>
    public TBuilder WithTableGroup(string tableGroupName)
    {
        Builder.UseTableGroup(tableGroupName);
        return Self;
    }

    /// <summary>Returns the underlying entity-type builder for continued configuration.</summary>
    public EntityTypeBuilder<TEntity> And() => Builder;

    /// <summary>Implicit conversion back to <see cref="EntityTypeBuilder{TEntity}"/>.</summary>
    public static implicit operator EntityTypeBuilder<TEntity>(
        MergeTreeFamilyBuilder<TBuilder, TEntity> b) => b.Builder;
}
