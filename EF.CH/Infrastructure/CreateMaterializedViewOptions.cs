namespace EF.CH.Infrastructure;

/// <summary>
/// Options for
/// <see cref="EF.CH.Extensions.ClickHouseDatabaseExtensions.CreateMaterializedViewAsync{TEntity}"/>.
/// </summary>
public sealed class CreateMaterializedViewOptions
{
    private bool _populate;
    private bool _ifNotExists = true;
    private string? _onCluster;

    /// <summary>
    /// Emit <c>POPULATE</c> so ClickHouse backfills the materialised view from
    /// the source table at attach time.
    /// </summary>
    public CreateMaterializedViewOptions WithPopulate()
    {
        _populate = true;
        return this;
    }

    /// <summary>
    /// Emit <c>ON CLUSTER '{cluster}'</c> so the view is created on every node
    /// in the named cluster. Match the <c>UseCluster</c> annotation when used
    /// in conjunction with replicated MVs.
    /// </summary>
    public CreateMaterializedViewOptions OnCluster(string cluster)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(cluster);
        _onCluster = cluster;
        return this;
    }

    /// <summary>
    /// Suppress the default <c>IF NOT EXISTS</c> clause; the call will throw if
    /// the view already exists.
    /// </summary>
    public CreateMaterializedViewOptions WithoutIfNotExists()
    {
        _ifNotExists = false;
        return this;
    }

    internal bool Populate => _populate;
    internal bool IfNotExists => _ifNotExists;
    internal string? OnClusterName => _onCluster;
}
