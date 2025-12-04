using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Migrations.Internal;

/// <summary>
/// ClickHouse-specific implementation of migration history tracking.
/// Stores applied migrations in __EFMigrationsHistory table with MergeTree engine.
/// </summary>
public class ClickHouseHistoryRepository : HistoryRepository
{
    public ClickHouseHistoryRepository(HistoryRepositoryDependencies dependencies)
        : base(dependencies)
    {
    }

    /// <summary>
    /// ClickHouse doesn't support distributed locks in the traditional sense.
    /// We use explicit transactions (none available) or advisory locking (not available).
    /// </summary>
    public override LockReleaseBehavior LockReleaseBehavior => LockReleaseBehavior.Explicit;

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
    /// Acquires a database lock for migration operations.
    /// ClickHouse doesn't support traditional locking, so this returns a no-op lock.
    /// </summary>
    public override IMigrationsDatabaseLock AcquireDatabaseLock()
    {
        // ClickHouse doesn't support database locks in the traditional sense
        // Return a no-op lock
        return new ClickHouseNoOpDatabaseLock(this);
    }

    /// <summary>
    /// Acquires a database lock asynchronously for migration operations.
    /// ClickHouse doesn't support traditional locking, so this returns a no-op lock.
    /// </summary>
    public override Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(CancellationToken cancellationToken = default)
    {
        // ClickHouse doesn't support database locks in the traditional sense
        // Return a no-op lock
        return Task.FromResult<IMigrationsDatabaseLock>(new ClickHouseNoOpDatabaseLock(this));
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
    /// </summary>
    public override string GetCreateScript()
    {
        var tableName = SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema);
        var migrationIdColumn = SqlGenerationHelper.DelimitIdentifier(MigrationIdColumnName);
        var productVersionColumn = SqlGenerationHelper.DelimitIdentifier(ProductVersionColumnName);

        return $"""
            CREATE TABLE {tableName} (
                {migrationIdColumn} String,
                {productVersionColumn} String
            )
            ENGINE = MergeTree()
            ORDER BY ({migrationIdColumn})
            {SqlGenerationHelper.StatementTerminator}
            """;
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

        // Escape single quotes in values
        var migrationId = row.MigrationId.Replace("'", "\\'");
        var productVersion = row.ProductVersion.Replace("'", "\\'");

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
        var escapedMigrationId = migrationId.Replace("'", "\\'");

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
}

/// <summary>
/// No-op database lock for ClickHouse since it doesn't support traditional locking.
/// </summary>
internal sealed class ClickHouseNoOpDatabaseLock : IMigrationsDatabaseLock
{
    private readonly IHistoryRepository _historyRepository;

    public ClickHouseNoOpDatabaseLock(IHistoryRepository? historyRepository = null)
    {
        _historyRepository = historyRepository!;
    }

    public IHistoryRepository HistoryRepository => _historyRepository;

    public void Dispose() { }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
