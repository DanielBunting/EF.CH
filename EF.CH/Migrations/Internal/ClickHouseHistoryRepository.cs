using EF.CH.Configuration;
using EF.CH.Infrastructure;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Migrations.Internal;

/// <summary>
/// ClickHouse-specific implementation of migration history tracking.
/// Stores applied migrations in __EFMigrationsHistory table with MergeTree engine.
/// Supports replicated history tables on clusters.
/// </summary>
public class ClickHouseHistoryRepository : HistoryRepository
{
    private readonly IDbContextOptions _options;

    public ClickHouseHistoryRepository(HistoryRepositoryDependencies dependencies, IDbContextOptions options)
        : base(dependencies)
    {
        _options = options;
    }

    private ClickHouseOptionsExtension? GetOptionsExtension()
        => _options.FindExtension<ClickHouseOptionsExtension>();

    /// <summary>
    /// Gets the SQL to check if the history table exists.
    /// Uses ClickHouse system.tables to check for table existence.
    /// </summary>
    protected override string ExistsSql
    {
        get
        {
            var rawTableName = TableName;

            return $"""
                SELECT 1
                FROM system.tables
                WHERE database = currentDatabase()
                AND name = '{rawTableName}'
                """;
        }
    }

    /// <summary>
    /// Interprets the result from the ExistsSql query.
    /// Returns true if any row was returned (table exists).
    /// </summary>
    protected override bool InterpretExistsResult(object? value)
    {
        // If we get any result (1), the table exists
        return value is not null && value != DBNull.Value;
    }

    /// <summary>
    /// Generates SQL to check if the history table exists.
    /// </summary>
    public override string GetCreateIfNotExistsScript()
    {
        // ClickHouse supports IF NOT EXISTS directly
        var script = GetCreateScript();
        return script.Replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ");
    }

    /// <summary>
    /// Generates SQL to create the history table with MergeTree engine.
    /// Supports cluster-aware and replicated configurations.
    /// </summary>
    public override string GetCreateScript()
    {
        var tableName = SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema);
        var migrationIdColumn = SqlGenerationHelper.DelimitIdentifier(MigrationIdColumnName);
        var productVersionColumn = SqlGenerationHelper.DelimitIdentifier(ProductVersionColumnName);

        var extension = GetOptionsExtension();
        var clusterClause = GetClusterClause(extension);
        var engineClause = GetEngineClause(extension);

        return $"""
            CREATE TABLE {tableName}{clusterClause} (
                {migrationIdColumn} String,
                {productVersionColumn} String
            )
            ENGINE = {engineClause}
            ORDER BY ({migrationIdColumn})
            {SqlGenerationHelper.StatementTerminator}
            """;
    }

    /// <summary>
    /// Gets the ON CLUSTER clause for the history table.
    /// </summary>
    private string GetClusterClause(ClickHouseOptionsExtension? extension)
    {
        // Check for migrations history cluster in configuration
        var clusterName = extension?.Configuration?.Defaults?.MigrationsHistoryCluster
                       ?? extension?.ClusterName;

        if (string.IsNullOrEmpty(clusterName))
        {
            return string.Empty;
        }

        return $" ON CLUSTER {FormatClusterName(clusterName)}";
    }

    private string FormatClusterName(string clusterName)
        => ClickHouseClusterMacros.ContainsMacro(clusterName)
            ? $"'{clusterName.Replace("'", "''")}'"
            : SqlGenerationHelper.DelimitIdentifier(clusterName);

    /// <summary>
    /// Gets the ENGINE clause for the history table.
    /// Uses ReplicatedMergeTree if configured for replication.
    /// </summary>
    private string GetEngineClause(ClickHouseOptionsExtension? extension)
    {
        var shouldReplicate = extension?.Configuration?.Defaults?.ReplicateMigrationsHistory ?? false;
        var hasCluster = !string.IsNullOrEmpty(extension?.Configuration?.Defaults?.MigrationsHistoryCluster)
                      || !string.IsNullOrEmpty(extension?.ClusterName);

        if (shouldReplicate && hasCluster)
        {
            // Use ReplicatedMergeTree for cluster-aware history
            var clusterName = extension?.Configuration?.Defaults?.MigrationsHistoryCluster
                           ?? extension?.ClusterName;
            var basePath = GetReplicationBasePath(extension, clusterName);

            return $"ReplicatedMergeTree('{basePath}', '{{replica}}')";
        }

        return "MergeTree()";
    }

    /// <summary>
    /// Gets the ZooKeeper base path for the history table.
    /// </summary>
    private string GetReplicationBasePath(ClickHouseOptionsExtension? extension, string? clusterName)
    {
        // Try to get from cluster configuration
        if (!string.IsNullOrEmpty(clusterName) &&
            extension?.Configuration?.Clusters?.TryGetValue(clusterName, out var clusterConfig) == true)
        {
            var basePath = clusterConfig.Replication.ZooKeeperBasePath
                .Replace("{database}", "{database}")
                .Replace("{table}", "__EFMigrationsHistory");
            return basePath;
        }

        // Default path
        return "/clickhouse/tables/{uuid}";
    }

    /// <summary>
    /// Generates SQL to insert a migration record into the history table.
    /// </summary>
    public override string GetInsertScript(HistoryRow row)
    {
        ArgumentNullException.ThrowIfNull(row);

        var tableName = SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema);
        var migrationIdColumn = SqlGenerationHelper.DelimitIdentifier(MigrationIdColumnName);
        var productVersionColumn = SqlGenerationHelper.DelimitIdentifier(ProductVersionColumnName);

        // Escape backslash first, then apostrophe — ClickHouse interprets `\` as
        // a C-style escape inside `'…'` literals, so a value ending in `\` would
        // otherwise escape the closing quote and break out of the literal.
        var migrationId = EscapeSqlLiteral(row.MigrationId);
        var productVersion = EscapeSqlLiteral(row.ProductVersion);

        return $"""
            INSERT INTO {tableName} ({migrationIdColumn}, {productVersionColumn})
            VALUES ('{migrationId}', '{productVersion}')
            {SqlGenerationHelper.StatementTerminator}
            """;
    }

    /// <summary>
    /// Generates SQL to delete a migration record from the history table.
    /// Note: ClickHouse DELETE is a mutation (async ALTER TABLE DELETE).
    /// </summary>
    public override string GetDeleteScript(string migrationId)
    {
        ArgumentNullException.ThrowIfNull(migrationId);

        var tableName = SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema);
        var migrationIdColumn = SqlGenerationHelper.DelimitIdentifier(MigrationIdColumnName);
        var escapedMigrationId = EscapeSqlLiteral(migrationId);

        // ClickHouse uses ALTER TABLE DELETE for row deletion (mutation)
        return $"""
            ALTER TABLE {tableName} DELETE WHERE {migrationIdColumn} = '{escapedMigrationId}'
            {SqlGenerationHelper.StatementTerminator}
            """;
    }

    /// <summary>
    /// Configures the model for the history table entity.
    /// </summary>
    protected override void ConfigureTable(EntityTypeBuilder<HistoryRow> history)
    {
        base.ConfigureTable(history);

        // ClickHouse uses String type
        history.Property(h => h.MigrationId).HasColumnType("String");
        history.Property(h => h.ProductVersion).HasColumnType("String");
    }

    /// <inheritdoc />
    public override LockReleaseBehavior LockReleaseBehavior => LockReleaseBehavior.Explicit;

    /// <inheritdoc />
    public override IMigrationsDatabaseLock AcquireDatabaseLock()
    {
        // ClickHouse does not support database-level locks
        return new ClickHouseNoOpDatabaseLock(this);
    }

    /// <inheritdoc />
    public override Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(CancellationToken cancellationToken = default)
    {
        // ClickHouse does not support database-level locks
        return Task.FromResult<IMigrationsDatabaseLock>(new ClickHouseNoOpDatabaseLock(this));
    }

    private sealed class ClickHouseNoOpDatabaseLock(IHistoryRepository historyRepository) : IMigrationsDatabaseLock
    {
        public IHistoryRepository HistoryRepository => historyRepository;
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>
    /// Gets a SQL script that returns true if migrations table exists, false otherwise.
    /// </summary>
    public override string GetBeginIfExistsScript(string migrationId)
    {
        // ClickHouse doesn't support procedural IF statements
        // Return empty - we handle existence checks differently
        return string.Empty;
    }

    /// <summary>
    /// Gets a SQL script that returns true if migrations table doesn't exist.
    /// </summary>
    public override string GetBeginIfNotExistsScript(string migrationId)
    {
        // ClickHouse doesn't support procedural IF statements
        return string.Empty;
    }

    /// <summary>
    /// Gets the end of a conditional script block.
    /// </summary>
    public override string GetEndIfScript()
    {
        // ClickHouse doesn't support procedural IF statements
        return string.Empty;
    }

    /// <summary>
    /// Escapes a value for embedding inside a single-quoted ClickHouse string
    /// literal. Backslash is escaped first, then apostrophe — without the
    /// backslash escape, a value ending in <c>\</c> would escape the closing
    /// quote and break out of the literal.
    /// </summary>
    private static string EscapeSqlLiteral(string value) =>
        value.Replace("\\", "\\\\").Replace("'", "\\'");
}
