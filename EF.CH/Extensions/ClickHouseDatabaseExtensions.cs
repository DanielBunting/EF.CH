using System.Data;
using EF.CH.Diagnostics;
using EF.CH.Infrastructure;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for ClickHouse database operations on DatabaseFacade.
/// </summary>
public static class ClickHouseDatabaseExtensions
{
    #region OptimizeTable

    /// <summary>
    /// Optimizes the table for the specified entity type, triggering a merge of data parts.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="database">The database facade.</param>
    /// <param name="configure">Optional action to configure optimization options
    /// (FINAL, PARTITION, DEDUPLICATE).</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <example>
    /// <code>
    /// // No options — basic OPTIMIZE TABLE
    /// await context.Database.OptimizeTableAsync&lt;Event&gt;();
    ///
    /// // With FINAL, partition, deduplicate, etc.
    /// await context.Database.OptimizeTableAsync&lt;Event&gt;(o => o
    ///     .WithPartition("202401")
    ///     .WithFinal()
    ///     .WithDeduplicate());
    /// </code>
    /// </example>
    public static Task<int> OptimizeTableAsync<TEntity>(
        this DatabaseFacade database,
        Action<OptimizeTableOptions>? configure = null,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        var tableName = GetTableName<TEntity>(database);
        return OptimizeTableCoreAsync(database, tableName, configure, cancellationToken);
    }

    /// <summary>
    /// Synchronous counterpart of
    /// <see cref="OptimizeTableAsync{TEntity}(DatabaseFacade, Action{OptimizeTableOptions}?, CancellationToken)"/>.
    /// </summary>
    public static int OptimizeTable<TEntity>(
        this DatabaseFacade database,
        Action<OptimizeTableOptions>? configure = null) where TEntity : class
        => OptimizeTableAsync<TEntity>(database, configure).GetAwaiter().GetResult();

    /// <summary>
    /// Optimizes a table by name.
    /// </summary>
    /// <param name="database">The database facade.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="configure">Optional action to configure optimization options
    /// (FINAL, PARTITION, DEDUPLICATE).</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <example>
    /// <code>
    /// // No options
    /// await context.Database.OptimizeTableAsync("Events");
    ///
    /// // With FINAL on a specific partition
    /// await context.Database.OptimizeTableAsync("Events", o => o
    ///     .WithPartition("202401")
    ///     .WithFinal());
    /// </code>
    /// </example>
    public static Task<int> OptimizeTableAsync(
        this DatabaseFacade database,
        string tableName,
        Action<OptimizeTableOptions>? configure = null,
        CancellationToken cancellationToken = default)
    {
        return OptimizeTableCoreAsync(database, tableName, configure, cancellationToken);
    }

    /// <summary>
    /// Synchronous counterpart of
    /// <see cref="OptimizeTableAsync(DatabaseFacade, string, Action{OptimizeTableOptions}?, CancellationToken)"/>.
    /// </summary>
    public static int OptimizeTable(
        this DatabaseFacade database,
        string tableName,
        Action<OptimizeTableOptions>? configure = null)
        => OptimizeTableAsync(database, tableName, configure).GetAwaiter().GetResult();

    #endregion

    #region Private Helpers

    private static Task<int> OptimizeTableCoreAsync(
        DatabaseFacade database,
        string tableName,
        Action<OptimizeTableOptions>? configure,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var options = new OptimizeTableOptions();
        configure?.Invoke(options);

        var sql = BuildOptimizeSql(database, tableName, options);
        return database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    private static string BuildOptimizeSql(
        DatabaseFacade database,
        string tableName,
        OptimizeTableOptions options)
    {
        var sqlHelper = database.GetService<ISqlGenerationHelper>();
        var quotedTable = sqlHelper.DelimitIdentifier(tableName);

        var sql = $"OPTIMIZE TABLE {quotedTable}";

        if (!string.IsNullOrEmpty(options.Partition))
        {
            // Partition IDs are passed as string literals
            sql += $" PARTITION '{options.Partition}'";
        }

        if (options.Final)
        {
            sql += " FINAL";
        }

        if (options.Deduplicate)
        {
            sql += " DEDUPLICATE";
            if (options.DeduplicateBy?.Length > 0)
            {
                var columns = string.Join(", ",
                    options.DeduplicateBy.Select(c => sqlHelper.DelimitIdentifier(c)));
                sql += $" BY {columns}";
            }
        }

        return sql;
    }

    private static string GetTableName<TEntity>(DatabaseFacade database) where TEntity : class
    {
        var context = database.GetService<ICurrentDbContext>().Context;
        var entityType = context.Model.FindEntityType(typeof(TEntity))
            ?? throw new InvalidOperationException(
                $"Entity type '{typeof(TEntity).Name}' is not part of the model.");

        return entityType.GetTableName()
            ?? throw new InvalidOperationException(
                $"Entity type '{typeof(TEntity).Name}' does not have a table name.");
    }

    #endregion

    #region TRUNCATE

    /// <summary>
    /// Synchronous counterpart of <see cref="TruncateTableAsync{TEntity}(DatabaseFacade, string?, CancellationToken)"/>.
    /// </summary>
    public static int TruncateTable<TEntity>(
        this DatabaseFacade database,
        string? onCluster = null) where TEntity : class
        => TruncateTableAsync<TEntity>(database, onCluster).GetAwaiter().GetResult();

    /// <summary>
    /// Issues <c>TRUNCATE TABLE "..."</c> for the entity's mapped table.
    /// Unlike <c>ExecuteDelete</c> this is a synchronous, atomic drop of all
    /// parts and doesn't increment the mutation counter.
    /// </summary>
    public static Task<int> TruncateTableAsync<TEntity>(
        this DatabaseFacade database,
        string? onCluster = null,
        CancellationToken cancellationToken = default) where TEntity : class
        => TruncateTableAsync(database, GetTableName<TEntity>(database), onCluster, cancellationToken);

    /// <summary>
    /// Synchronous counterpart of <see cref="TruncateTableAsync(DatabaseFacade, string, string?, CancellationToken)"/>.
    /// </summary>
    public static int TruncateTable(
        this DatabaseFacade database,
        string tableName,
        string? onCluster = null)
        => TruncateTableAsync(database, tableName, onCluster).GetAwaiter().GetResult();

    /// <summary>
    /// Issues <c>TRUNCATE TABLE "..."</c> for an explicit table name.
    /// </summary>
    public static Task<int> TruncateTableAsync(
        this DatabaseFacade database,
        string tableName,
        string? onCluster = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        var sqlHelper = database.GetService<ISqlGenerationHelper>();
        var cluster = onCluster is null
            ? string.Empty
            : ClickHouseClusterMacros.ContainsMacro(onCluster)
                ? $" ON CLUSTER '{onCluster.Replace("'", "''")}'"
                : $" ON CLUSTER {sqlHelper.DelimitIdentifier(onCluster)}";
        return database.ExecuteSqlRawAsync(
            $"TRUNCATE TABLE {sqlHelper.DelimitIdentifier(tableName)}{cluster}",
            cancellationToken);
    }

    #endregion

    #region SYSTEM admin commands

    /// <summary>
    /// Synchronous counterpart of <see cref="ReloadDictionaryAsync"/>.
    /// </summary>
    public static int ReloadDictionary(this DatabaseFacade database, string name)
        => ReloadDictionaryAsync(database, name).GetAwaiter().GetResult();

    public static Task<int> ReloadDictionaryAsync(this DatabaseFacade database, string name, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var helper = database.GetService<ISqlGenerationHelper>();
        return database.ExecuteSqlRawAsync($"SYSTEM RELOAD DICTIONARY {helper.DelimitIdentifier(name)}", ct);
    }

    /// <summary>
    /// Synchronous counterpart of <see cref="FlushLogsAsync"/>.
    /// </summary>
    public static int FlushLogs(this DatabaseFacade database)
        => FlushLogsAsync(database).GetAwaiter().GetResult();

    public static Task<int> FlushLogsAsync(this DatabaseFacade database, CancellationToken ct = default)
        => database.ExecuteSqlRawAsync("SYSTEM FLUSH LOGS", ct);

    /// <summary>
    /// Synchronous counterpart of <see cref="SyncReplicaAsync"/>.
    /// </summary>
    public static int SyncReplica(this DatabaseFacade database, string tableName, bool strict = false)
        => SyncReplicaAsync(database, tableName, strict).GetAwaiter().GetResult();

    public static Task<int> SyncReplicaAsync(this DatabaseFacade database, string tableName, bool strict = false, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        var helper = database.GetService<ISqlGenerationHelper>();
        var sql = $"SYSTEM SYNC REPLICA {helper.DelimitIdentifier(tableName)}" + (strict ? " STRICT" : string.Empty);
        return database.ExecuteSqlRawAsync(sql, ct);
    }

    public static Task<int> DropMarkCacheAsync(this DatabaseFacade database, CancellationToken ct = default)
        => database.ExecuteSqlRawAsync("SYSTEM DROP MARK CACHE", ct);

    public static Task<int> RestartReplicasAsync(this DatabaseFacade database, CancellationToken ct = default)
        => database.ExecuteSqlRawAsync("SYSTEM RESTART REPLICAS", ct);

    #endregion

    #region Deferred materialised views

    /// <summary>
    /// Synchronous counterpart of <see cref="CreateMaterializedViewAsync{TEntity}"/>.
    /// </summary>
    public static int CreateMaterializedView<TEntity>(
        this DatabaseFacade database,
        bool populate = false) where TEntity : class
        => CreateMaterializedViewAsync<TEntity>(database, populate).GetAwaiter().GetResult();

    /// <summary>
    /// Creates a materialised view that was declared with
    /// <see cref="ClickHouseEntityTypeBuilderExtensions.AsMaterializedViewDeferred{TEntity}"/>.
    /// The view's target table must already exist
    /// (normally created via <c>EnsureCreatedAsync</c>). Use <paramref name="populate"/>
    /// to backfill from the source table at attach time.
    /// </summary>
    public static Task<int> CreateMaterializedViewAsync<TEntity>(
        this DatabaseFacade database,
        bool populate = false,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        var context = database.GetService<ICurrentDbContext>().Context;
        var entityType = context.Model.FindEntityType(typeof(TEntity))
            ?? throw new InvalidOperationException($"Entity type '{typeof(TEntity).Name}' is not in the model.");

        var viewName = entityType.GetTableName()
            ?? throw new InvalidOperationException($"Entity '{typeof(TEntity).Name}' has no table name.");
        var selectSql = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery)?.Value as string
            ?? throw new InvalidOperationException(
                $"Entity '{typeof(TEntity).Name}' is not configured as a materialised view. " +
                "Call AsMaterializedView(…) or AsMaterializedViewRaw(…) first.");

        var helper = database.GetService<ISqlGenerationHelper>();
        var engineSql = BuildEngineClauseForEntity(entityType);
        var sql = new System.Text.StringBuilder()
            .Append("CREATE MATERIALIZED VIEW IF NOT EXISTS ").Append(helper.DelimitIdentifier(viewName)).Append(' ')
            .Append(engineSql)
            .Append(populate ? " POPULATE AS " : " AS ")
            .Append(selectSql)
            .ToString();

        return database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    private static string BuildEngineClauseForEntity(IEntityType entityType)
    {
        var engine = entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value as string ?? "MergeTree";
        var orderBy = entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value as string[];
        var sb = new System.Text.StringBuilder("ENGINE = ").Append(engine);
        if (string.Equals(engine, "MergeTree", StringComparison.OrdinalIgnoreCase)
         || engine.EndsWith("MergeTree", StringComparison.OrdinalIgnoreCase))
            sb.Append("()");
        if (orderBy is { Length: > 0 })
            sb.Append(" ORDER BY (").Append(string.Join(", ", orderBy.Select(c => "\"" + c + "\""))).Append(')');
        return sb.ToString();
    }

    /// <summary>
    /// Synchronous counterpart of <see cref="CreateRefreshableMaterializedViewAsync{TEntity}"/>.
    /// </summary>
    public static int CreateRefreshableMaterializedView<TEntity>(
        this DatabaseFacade database) where TEntity : class
        => CreateRefreshableMaterializedViewAsync<TEntity>(database).GetAwaiter().GetResult();

    /// <summary>
    /// Issues <c>CREATE MATERIALIZED VIEW … REFRESH …</c> for an entity declared with
    /// <see cref="ClickHouseEntityTypeBuilderExtensions.AsRefreshableMaterializedView"/>.
    /// Intended to back the deferred-creation path the same way
    /// <see cref="CreateMaterializedViewAsync{TEntity}"/> does for non-refreshable MVs.
    /// </summary>
    public static Task<int> CreateRefreshableMaterializedViewAsync<TEntity>(
        this DatabaseFacade database,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        var (entityType, viewName, helper) = ResolveRefreshable<TEntity>(database);
        var selectSql = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery)?.Value as string
            ?? throw new InvalidOperationException(
                $"Entity '{typeof(TEntity).Name}' is not configured as a refreshable MV (missing query).");

        var kind = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshKind)?.Value as string ?? "EVERY";
        var interval = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshInterval)?.Value as string
            ?? throw new InvalidOperationException(
                $"Entity '{typeof(TEntity).Name}' has no refresh interval — was it declared with AsRefreshableMaterializedView?");
        var offset = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshOffset)?.Value as string;
        var randomize = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshRandomizeFor)?.Value as string;
        var append = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshAppend)?.Value as bool? ?? false;
        var empty = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshEmpty)?.Value as bool? ?? false;
        var target = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshTarget)?.Value as string;

        var sb = new System.Text.StringBuilder()
            .Append("CREATE MATERIALIZED VIEW IF NOT EXISTS ")
            .Append(helper.DelimitIdentifier(viewName)).Append(' ')
            .Append("REFRESH ").Append(kind).Append(' ').Append(interval);
        if (!string.IsNullOrEmpty(offset)) sb.Append(" OFFSET ").Append(offset);
        if (!string.IsNullOrEmpty(randomize)) sb.Append(" RANDOMIZE FOR ").Append(randomize);
        if (append) sb.Append(" APPEND");
        if (!string.IsNullOrEmpty(target))
            sb.Append(" TO ").Append(helper.DelimitIdentifier(target));
        else
            sb.Append(' ').Append(BuildEngineClauseForEntity(entityType));
        if (empty) sb.Append(" EMPTY");
        sb.Append(" AS ").Append(selectSql);

        return database.ExecuteSqlRawAsync(sb.ToString(), cancellationToken);
    }

    /// <summary>
    /// <c>SYSTEM REFRESH VIEW &lt;name&gt;</c> — manually trigger a refresh.
    /// </summary>
    public static Task<int> RefreshViewAsync<TEntity>(this DatabaseFacade database, CancellationToken cancellationToken = default)
        where TEntity : class
        => RunSystemViewCommandAsync<TEntity>(database, "REFRESH", cancellationToken);

    /// <summary>
    /// <c>SYSTEM STOP VIEW &lt;name&gt;</c> — pause scheduled refreshes.
    /// </summary>
    public static Task<int> StopViewAsync<TEntity>(this DatabaseFacade database, CancellationToken cancellationToken = default)
        where TEntity : class
        => RunSystemViewCommandAsync<TEntity>(database, "STOP", cancellationToken);

    /// <summary>
    /// <c>SYSTEM START VIEW &lt;name&gt;</c> — resume scheduled refreshes.
    /// </summary>
    public static Task<int> StartViewAsync<TEntity>(this DatabaseFacade database, CancellationToken cancellationToken = default)
        where TEntity : class
        => RunSystemViewCommandAsync<TEntity>(database, "START", cancellationToken);

    /// <summary>
    /// <c>SYSTEM CANCEL VIEW &lt;name&gt;</c> — abort an in-flight refresh.
    /// </summary>
    public static Task<int> CancelViewAsync<TEntity>(this DatabaseFacade database, CancellationToken cancellationToken = default)
        where TEntity : class
        => RunSystemViewCommandAsync<TEntity>(database, "CANCEL", cancellationToken);

    /// <summary>
    /// Reads the current row from <c>system.view_refreshes</c> for this MV.
    /// Returns null if the view is unknown to ClickHouse.
    /// </summary>
    public static async Task<RefreshableViewStatus?> GetRefreshStatusAsync<TEntity>(
        this DatabaseFacade database,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        var (_, viewName, _) = ResolveRefreshable<TEntity>(database);
        return await ReadRefreshStatusAsync(database, viewName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Synchronous counterpart of <see cref="WaitForRefreshAsync{TEntity}"/>.
    /// </summary>
    public static void WaitForRefresh<TEntity>(
        this DatabaseFacade database,
        TimeSpan? timeout = null) where TEntity : class
        => WaitForRefreshAsync<TEntity>(database, timeout).GetAwaiter().GetResult();

    /// <summary>
    /// Polls <c>system.view_refreshes</c> until <c>last_success_time</c> advances past
    /// the moment this method was invoked, or the timeout expires.
    /// </summary>
    public static async Task WaitForRefreshAsync<TEntity>(
        this DatabaseFacade database,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        var (_, viewName, _) = ResolveRefreshable<TEntity>(database);
        var start = DateTime.UtcNow;
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(15));
        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var status = await ReadRefreshStatusAsync(database, viewName, cancellationToken).ConfigureAwait(false);
            if (status?.LastSuccessTime is { } t && t > start)
                return;
            await Task.Delay(200, cancellationToken).ConfigureAwait(false);
        }
        throw new TimeoutException(
            $"Refreshable MV '{viewName}' did not refresh within {(timeout ?? TimeSpan.FromSeconds(15))}.");
    }

    private static Task<int> RunSystemViewCommandAsync<TEntity>(DatabaseFacade database, string verb, CancellationToken ct)
        where TEntity : class
    {
        var (_, viewName, helper) = ResolveRefreshable<TEntity>(database);
        return database.ExecuteSqlRawAsync($"SYSTEM {verb} VIEW {helper.DelimitIdentifier(viewName)}", ct);
    }

    private static (IEntityType EntityType, string ViewName, ISqlGenerationHelper Helper) ResolveRefreshable<TEntity>(DatabaseFacade database)
        where TEntity : class
    {
        var context = database.GetService<ICurrentDbContext>().Context;
        var entityType = context.Model.FindEntityType(typeof(TEntity))
            ?? throw new InvalidOperationException($"Entity type '{typeof(TEntity).Name}' is not in the model.");
        var name = entityType.GetTableName()
            ?? throw new InvalidOperationException($"Entity '{typeof(TEntity).Name}' has no table name.");
        var helper = database.GetService<ISqlGenerationHelper>();
        return (entityType, name, helper);
    }

    private static async Task<RefreshableViewStatus?> ReadRefreshStatusAsync(
        DatabaseFacade database,
        string viewName,
        CancellationToken cancellationToken)
    {
        var connection = database.GetDbConnection();
        var ownsConnection = connection.State != ConnectionState.Open;
        if (ownsConnection)
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            await using var cmd = connection.CreateCommand();
            var escaped = viewName.Replace("'", "''");
            cmd.CommandText = $"""
                SELECT
                    view,
                    status,
                    last_refresh_time,
                    last_success_time,
                    next_refresh_time,
                    exception,
                    retry,
                    progress,
                    read_rows,
                    written_rows
                FROM system.view_refreshes
                WHERE view = '{escaped}'
                LIMIT 1
                """;
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                return null;

            return new RefreshableViewStatus(
                View: reader.GetString(0),
                Status: reader.IsDBNull(1) ? null : reader.GetString(1),
                LastRefreshTime: reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                LastSuccessTime: reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                NextRefreshTime: reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                ExceptionMessage: reader.IsDBNull(5) ? null : reader.GetString(5),
                Retry: reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6)),
                Progress: reader.IsDBNull(7) ? 0d : Convert.ToDouble(reader.GetValue(7)),
                ReadRows: reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                WrittenRows: reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)));
        }
        finally
        {
            if (ownsConnection)
                await connection.CloseAsync().ConfigureAwait(false);
        }
    }

    #endregion

    #region Projections

    /// <summary>
    /// <c>ALTER TABLE … ADD PROJECTION … (SELECT …)</c>. Call
    /// <see cref="MaterializeProjectionAsync"/> afterwards to backfill existing data.
    /// </summary>
    public static Task<int> AddProjectionAsync(
        this DatabaseFacade database,
        string tableName,
        string projectionName,
        string selectSql,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);
        ArgumentException.ThrowIfNullOrWhiteSpace(selectSql);
        var helper = database.GetService<ISqlGenerationHelper>();
        var sql =
            $"ALTER TABLE {helper.DelimitIdentifier(tableName)} " +
            $"ADD PROJECTION IF NOT EXISTS {helper.DelimitIdentifier(projectionName)} ({selectSql})";
        return database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    /// <summary>
    /// <c>ALTER TABLE … MATERIALIZE PROJECTION …</c> — backfills the named projection.
    /// </summary>
    public static Task<int> MaterializeProjectionAsync(
        this DatabaseFacade database,
        string tableName,
        string projectionName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);
        var helper = database.GetService<ISqlGenerationHelper>();
        var sql = $"ALTER TABLE {helper.DelimitIdentifier(tableName)} " +
                  $"MATERIALIZE PROJECTION {helper.DelimitIdentifier(projectionName)}";
        return database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    /// <summary>
    /// <c>ALTER TABLE … DROP PROJECTION …</c>.
    /// </summary>
    public static Task<int> DropProjectionAsync(
        this DatabaseFacade database,
        string tableName,
        string projectionName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        ArgumentException.ThrowIfNullOrWhiteSpace(projectionName);
        var helper = database.GetService<ISqlGenerationHelper>();
        var sql = $"ALTER TABLE {helper.DelimitIdentifier(tableName)} " +
                  $"DROP PROJECTION IF EXISTS {helper.DelimitIdentifier(projectionName)}";
        return database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    #endregion

    #region Parameterized Views

    /// <summary>
    /// Creates a parameterized view in ClickHouse. Always emits <c>IF NOT EXISTS</c>
    /// — the call is idempotent. To force a recreate, drop first via
    /// <see cref="DropParameterizedViewIfExistsAsync"/>.
    /// </summary>
    /// <param name="database">The database facade.</param>
    /// <param name="viewName">The name of the view to create.</param>
    /// <param name="selectSql">
    /// The SELECT SQL for the view, including parameter placeholders.
    /// Use ClickHouse syntax: <c>{parameter_name:Type}</c> for parameters.
    /// </param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <example>
    /// <code>
    /// await context.Database.CreateParameterizedViewAsync(
    ///     "user_events_view",
    ///     @"SELECT event_id, event_type, user_id, timestamp
    ///       FROM events
    ///       WHERE user_id = {user_id:UInt64}
    ///         AND timestamp >= {start_date:DateTime}");
    /// </code>
    /// </example>
    public static Task<int> CreateParameterizedViewAsync(
        this DatabaseFacade database,
        string viewName,
        string selectSql,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);
        ArgumentException.ThrowIfNullOrWhiteSpace(selectSql);

        var sql = BuildCreateParameterizedViewSql(database, viewName, selectSql);
        return database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    /// <summary>
    /// Drops a parameterized view from ClickHouse. Throws if the view does not exist.
    /// Use <see cref="DropParameterizedViewIfExistsAsync"/> for the silent variant.
    /// </summary>
    /// <param name="database">The database facade.</param>
    /// <param name="viewName">The name of the view to drop.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public static Task<int> DropParameterizedViewAsync(
        this DatabaseFacade database,
        string viewName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);

        var sql = BuildDropParameterizedViewSql(database, viewName, ifExists: false);
        return database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    /// <summary>
    /// Drops a parameterized view from ClickHouse, emitting <c>IF EXISTS</c>.
    /// Silent if the view does not exist.
    /// </summary>
    /// <param name="database">The database facade.</param>
    /// <param name="viewName">The name of the view to drop.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public static Task<int> DropParameterizedViewIfExistsAsync(
        this DatabaseFacade database,
        string viewName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);

        var sql = BuildDropParameterizedViewSql(database, viewName, ifExists: true);
        return database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    private static string BuildCreateParameterizedViewSql(
        DatabaseFacade database,
        string viewName,
        string selectSql)
    {
        var sqlHelper = database.GetService<ISqlGenerationHelper>();
        var quotedName = sqlHelper.DelimitIdentifier(viewName);

        // Escape curly braces to prevent ExecuteSqlRawAsync from interpreting them as format specifiers
        var escapedSql = selectSql.Trim().Replace("{", "{{").Replace("}", "}}");
        return $"CREATE VIEW IF NOT EXISTS {quotedName} AS\n{escapedSql}";
    }

    private static string BuildDropParameterizedViewSql(
        DatabaseFacade database,
        string viewName,
        bool ifExists)
    {
        var sqlHelper = database.GetService<ISqlGenerationHelper>();
        var quotedName = sqlHelper.DelimitIdentifier(viewName);

        var ifExistsClause = ifExists ? "IF EXISTS " : "";
        return $"DROP VIEW {ifExistsClause}{quotedName}";
    }

    /// <summary>
    /// Synchronous counterpart of <see cref="EnsureParameterizedViewsAsync"/>.
    /// Always emits <c>IF NOT EXISTS</c> — the operation is idempotent.
    /// </summary>
    public static int EnsureParameterizedViews(this DatabaseFacade database)
        => EnsureParameterizedViewsAsync(database).GetAwaiter().GetResult();

    /// <summary>
    /// Creates all parameterized views configured via AsParameterizedView in the model.
    /// Always emits <c>IF NOT EXISTS</c> — the operation is idempotent.
    /// </summary>
    /// <param name="database">The database facade.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The number of views created.</returns>
    /// <remarks>
    /// <para>
    /// This method scans the EF Core model for entity types configured with
    /// <c>AsParameterizedView&lt;TView, TSource&gt;</c> and creates the corresponding
    /// CREATE VIEW statements.
    /// </para>
    /// <para>
    /// Views configured with only <c>HasParameterizedView(name)</c> (without fluent configuration)
    /// are skipped since they don't have the projection and parameter metadata.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// // Create all views on startup
    /// await context.Database.EnsureCreatedAsync();
    /// await context.Database.EnsureParameterizedViewsAsync();
    /// </code>
    /// </example>
    public static async Task<int> EnsureParameterizedViewsAsync(
        this DatabaseFacade database,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(database);

        var context = database.GetService<ICurrentDbContext>().Context;
        var model = context.Model;
        var viewsCreated = 0;

        foreach (var entityType in model.GetEntityTypes())
        {
            // Check if this is a parameterized view with fluent configuration
            var isParameterizedView = entityType.FindAnnotation(
                Metadata.ClickHouseAnnotationNames.ParameterizedView)?.Value as bool? ?? false;

            if (!isParameterizedView)
                continue;

            var metadata = entityType.FindAnnotation(
                Metadata.ClickHouseAnnotationNames.ParameterizedViewMetadata)?.Value as ParameterizedViews.ParameterizedViewMetadataBase;

            if (metadata == null)
            {
                // Skip views configured with HasParameterizedView() only (no fluent configuration)
                continue;
            }

            var sql = ParameterizedViews.ParameterizedViewSqlGenerator.GenerateCreateViewSql(model, metadata, ifNotExists: true);

            // Escape curly braces for ExecuteSqlRawAsync
            var escapedSql = sql.Replace("{", "{{").Replace("}", "}}");
            await database.ExecuteSqlRawAsync(escapedSql, cancellationToken);
            viewsCreated++;
        }

        return viewsCreated;
    }

    /// <summary>
    /// Synchronous counterpart of <see cref="EnsureParameterizedViewAsync{TView}"/>.
    /// Always emits <c>IF NOT EXISTS</c> — the operation is idempotent.
    /// </summary>
    public static void EnsureParameterizedView<TView>(this DatabaseFacade database)
        where TView : class
        => EnsureParameterizedViewAsync<TView>(database).GetAwaiter().GetResult();

    /// <summary>
    /// Creates a specific parameterized view configured via AsParameterizedView.
    /// Always emits <c>IF NOT EXISTS</c> — the operation is idempotent.
    /// </summary>
    /// <typeparam name="TView">The view result entity type.</typeparam>
    /// <param name="database">The database facade.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">
    /// If the entity type is not configured as a parameterized view with fluent configuration.
    /// </exception>
    /// <example>
    /// <code>
    /// await context.Database.EnsureParameterizedViewAsync&lt;UserEventView&gt;();
    /// </code>
    /// </example>
    public static Task EnsureParameterizedViewAsync<TView>(
        this DatabaseFacade database,
        CancellationToken cancellationToken = default)
        where TView : class
    {
        ArgumentNullException.ThrowIfNull(database);

        var context = database.GetService<ICurrentDbContext>().Context;
        var model = context.Model;

        var entityType = model.FindEntityType(typeof(TView))
            ?? throw new InvalidOperationException(
                $"Entity type '{typeof(TView).Name}' is not configured in the model.");

        var isParameterizedView = entityType.FindAnnotation(
            Metadata.ClickHouseAnnotationNames.ParameterizedView)?.Value as bool? ?? false;

        if (!isParameterizedView)
        {
            throw new InvalidOperationException(
                $"Entity type '{typeof(TView).Name}' is not configured as a parameterized view. " +
                "Use AsParameterizedView<TView, TSource>() in OnModelCreating.");
        }

        var metadata = entityType.FindAnnotation(
            Metadata.ClickHouseAnnotationNames.ParameterizedViewMetadata)?.Value as ParameterizedViews.ParameterizedViewMetadataBase
            ?? throw new InvalidOperationException(
                $"Entity type '{typeof(TView).Name}' does not have fluent view configuration. " +
                "Use AsParameterizedView<TView, TSource>() instead of HasParameterizedView() to enable DDL generation.");

        var sql = ParameterizedViews.ParameterizedViewSqlGenerator.GenerateCreateViewSql(model, metadata, ifNotExists: true);

        // Escape curly braces for ExecuteSqlRawAsync
        var escapedSql = sql.Replace("{", "{{").Replace("}", "}}");
        return database.ExecuteSqlRawAsync(escapedSql, cancellationToken);
    }

    /// <summary>
    /// Gets the CREATE VIEW SQL for a parameterized view without executing it.
    /// Always includes <c>IF NOT EXISTS</c> to match the behaviour of
    /// <see cref="EnsureParameterizedViewAsync{TView}"/>.
    /// </summary>
    /// <typeparam name="TView">The view result entity type.</typeparam>
    /// <param name="database">The database facade.</param>
    /// <returns>The CREATE VIEW SQL statement.</returns>
    /// <remarks>
    /// Useful for debugging or generating migration scripts.
    /// </remarks>
    /// <example>
    /// <code>
    /// var sql = context.Database.GetParameterizedViewSql&lt;UserEventView&gt;();
    /// Console.WriteLine(sql);
    /// </code>
    /// </example>
    public static string GetParameterizedViewSql<TView>(this DatabaseFacade database)
        where TView : class
    {
        ArgumentNullException.ThrowIfNull(database);

        var context = database.GetService<ICurrentDbContext>().Context;
        var model = context.Model;

        var entityType = model.FindEntityType(typeof(TView))
            ?? throw new InvalidOperationException(
                $"Entity type '{typeof(TView).Name}' is not configured in the model.");

        var metadata = entityType.FindAnnotation(
            Metadata.ClickHouseAnnotationNames.ParameterizedViewMetadata)?.Value as ParameterizedViews.ParameterizedViewMetadataBase
            ?? throw new InvalidOperationException(
                $"Entity type '{typeof(TView).Name}' does not have fluent view configuration.");

        return ParameterizedViews.ParameterizedViewSqlGenerator.GenerateCreateViewSql(model, metadata, ifNotExists: true);
    }

    #endregion

    #region Plain Views

    /// <summary>
    /// Creates a ClickHouse view from a raw SELECT SQL string. Always emits
    /// <c>IF NOT EXISTS</c> — the call is idempotent. Pass <c>orReplace: true</c>
    /// to instead emit <c>CREATE OR REPLACE VIEW</c>.
    /// </summary>
    /// <param name="database">The database facade.</param>
    /// <param name="viewName">The view name.</param>
    /// <param name="selectSql">The SELECT SQL body (without CREATE VIEW prefix).</param>
    /// <param name="orReplace">
    /// Emit <c>CREATE OR REPLACE VIEW</c> instead of <c>CREATE VIEW IF NOT EXISTS</c>.
    /// </param>
    /// <param name="onCluster">Optional ON CLUSTER cluster name.</param>
    /// <param name="schema">Optional schema (database) qualifier.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    public static Task<int> CreateViewAsync(
        this DatabaseFacade database,
        string viewName,
        string selectSql,
        bool orReplace = false,
        string? onCluster = null,
        string? schema = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);
        ArgumentException.ThrowIfNullOrWhiteSpace(selectSql);

        var metadata = new Views.ViewMetadataBase
        {
            ViewName = viewName,
            ResultType = typeof(object),
            RawSelectSql = selectSql,
            // ClickHouse CREATE VIEW cannot combine IF NOT EXISTS with OR REPLACE,
            // so OR REPLACE wins when explicitly requested; otherwise we always
            // emit IF NOT EXISTS for idempotency.
            IfNotExists = !orReplace,
            OrReplace = orReplace,
            OnCluster = onCluster,
            Schema = schema
        };

        var context = database.GetService<ICurrentDbContext>().Context;
        var sql = Views.ViewSqlGenerator.GenerateCreateViewSql(context.Model, metadata);
        var escapedSql = sql.Replace("{", "{{").Replace("}", "}}");
        return database.ExecuteSqlRawAsync(escapedSql, cancellationToken);
    }

    /// <summary>
    /// Drops a ClickHouse view. Throws if the view does not exist. Use
    /// <see cref="DropViewIfExistsAsync"/> for the silent variant.
    /// </summary>
    public static Task<int> DropViewAsync(
        this DatabaseFacade database,
        string viewName,
        string? onCluster = null,
        string? schema = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);

        var sql = Views.ViewSqlGenerator.GenerateDropViewSql(viewName, schema, ifExists: false, onCluster);
        return database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    /// <summary>
    /// Drops a ClickHouse view, emitting <c>IF EXISTS</c>. Silent if the view
    /// does not exist.
    /// </summary>
    public static Task<int> DropViewIfExistsAsync(
        this DatabaseFacade database,
        string viewName,
        string? onCluster = null,
        string? schema = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);

        var sql = Views.ViewSqlGenerator.GenerateDropViewSql(viewName, schema, ifExists: true, onCluster);
        return database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    /// <summary>
    /// Synchronous counterpart of <see cref="EnsureViewsAsync"/>.
    /// </summary>
    public static int EnsureViews(this DatabaseFacade database)
        => EnsureViewsAsync(database).GetAwaiter().GetResult();

    /// <summary>
    /// Creates all plain views configured via <c>AsView</c> / <c>AsViewRaw</c> in the model.
    /// Always emits <c>IF NOT EXISTS</c> — the operation is idempotent. Skips entities
    /// marked <c>AsViewDeferred</c> and entities configured with only <c>HasView(name)</c>
    /// (no DDL metadata to emit).
    /// </summary>
    /// <returns>The number of views created.</returns>
    public static async Task<int> EnsureViewsAsync(
        this DatabaseFacade database,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(database);

        var context = database.GetService<ICurrentDbContext>().Context;
        var model = context.Model;
        var viewsCreated = 0;

        foreach (var entityType in model.GetEntityTypes())
        {
            var isView = entityType.FindAnnotation(
                Metadata.ClickHouseAnnotationNames.View)?.Value as bool? ?? false;
            if (!isView)
                continue;

            var deferred = entityType.FindAnnotation(
                Metadata.ClickHouseAnnotationNames.ViewDeferred)?.Value as bool? ?? false;
            if (deferred)
                continue;

            var metadata = entityType.FindAnnotation(
                Metadata.ClickHouseAnnotationNames.ViewMetadata)?.Value as Views.ViewMetadataBase;
            if (metadata == null)
                continue;

            var sql = Views.ViewSqlGenerator.GenerateCreateViewSql(model, ForceIfNotExists(metadata));
            var escapedSql = sql.Replace("{", "{{").Replace("}", "}}");
            await database.ExecuteSqlRawAsync(escapedSql, cancellationToken);
            viewsCreated++;
        }

        return viewsCreated;
    }

    /// <summary>
    /// Synchronous counterpart of <see cref="EnsureViewAsync{TView}"/>.
    /// Always emits <c>IF NOT EXISTS</c> — the operation is idempotent.
    /// </summary>
    public static void EnsureView<TView>(this DatabaseFacade database)
        where TView : class
        => EnsureViewAsync<TView>(database).GetAwaiter().GetResult();

    /// <summary>
    /// Creates a single plain view configured via <c>AsView</c> / <c>AsViewRaw</c>.
    /// Always emits <c>IF NOT EXISTS</c> — the operation is idempotent.
    /// </summary>
    /// <typeparam name="TView">The view result entity type.</typeparam>
    public static Task EnsureViewAsync<TView>(
        this DatabaseFacade database,
        CancellationToken cancellationToken = default)
        where TView : class
    {
        ArgumentNullException.ThrowIfNull(database);

        var context = database.GetService<ICurrentDbContext>().Context;
        var model = context.Model;

        var entityType = model.FindEntityType(typeof(TView))
            ?? throw new InvalidOperationException(
                $"Entity type '{typeof(TView).Name}' is not configured in the model.");

        var isView = entityType.FindAnnotation(
            Metadata.ClickHouseAnnotationNames.View)?.Value as bool? ?? false;
        if (!isView)
        {
            throw new InvalidOperationException(
                $"Entity type '{typeof(TView).Name}' is not configured as a view. " +
                "Use AsView<TView, TSource>() or AsViewRaw<TView>() in OnModelCreating.");
        }

        var metadata = entityType.FindAnnotation(
            Metadata.ClickHouseAnnotationNames.ViewMetadata)?.Value as Views.ViewMetadataBase
            ?? throw new InvalidOperationException(
                $"Entity type '{typeof(TView).Name}' does not have view DDL metadata. " +
                "Use AsView<TView, TSource>() or AsViewRaw<TView>() instead of HasView() to enable DDL generation.");

        var sql = Views.ViewSqlGenerator.GenerateCreateViewSql(model, ForceIfNotExists(metadata));
        var escapedSql = sql.Replace("{", "{{").Replace("}", "}}");
        return database.ExecuteSqlRawAsync(escapedSql, cancellationToken);
    }

    // Ensure* methods are idempotent by contract: clone the configured view metadata
    // with IF NOT EXISTS forced on (and OR REPLACE off — the two are mutually exclusive
    // in ClickHouse). Used by EnsureViewsAsync / EnsureViewAsync<T> so the user's
    // AsView(...orReplace: true) configuration doesn't bleed into the runtime ensure path.
    private static Views.ViewMetadataBase ForceIfNotExists(Views.ViewMetadataBase source)
        => new()
        {
            ViewName = source.ViewName,
            ResultType = source.ResultType,
            SourceType = source.SourceType,
            SourceTable = source.SourceTable,
            ProjectionExpression = source.ProjectionExpression,
            WhereExpressions = source.WhereExpressions,
            RawSelectSql = source.RawSelectSql,
            IfNotExists = true,
            OrReplace = false,
            OnCluster = source.OnCluster,
            Schema = source.Schema
        };

    /// <summary>
    /// Returns the CREATE VIEW SQL for a plain view configured via <c>AsView</c> / <c>AsViewRaw</c>
    /// without executing it. Useful for debugging and migration scaffolding.
    /// </summary>
    public static string GetViewSql<TView>(this DatabaseFacade database)
        where TView : class
    {
        ArgumentNullException.ThrowIfNull(database);

        var context = database.GetService<ICurrentDbContext>().Context;
        var model = context.Model;

        var entityType = model.FindEntityType(typeof(TView))
            ?? throw new InvalidOperationException(
                $"Entity type '{typeof(TView).Name}' is not configured in the model.");

        var metadata = entityType.FindAnnotation(
            Metadata.ClickHouseAnnotationNames.ViewMetadata)?.Value as Views.ViewMetadataBase
            ?? throw new InvalidOperationException(
                $"Entity type '{typeof(TView).Name}' does not have view DDL metadata.");

        return Views.ViewSqlGenerator.GenerateCreateViewSql(model, metadata);
    }

    #endregion
}
