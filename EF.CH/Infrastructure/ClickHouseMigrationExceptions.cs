namespace EF.CH.Infrastructure;

/// <summary>
/// Thrown when attempting to execute a Down migration on ClickHouse.
/// ClickHouse lacks ACID transactions, making rollback migrations inherently unsafe.
/// Use forward-only migrations instead - create a new migration that reverses schema changes.
/// </summary>
public class ClickHouseDownMigrationNotSupportedException : NotSupportedException
{
    /// <summary>
    /// The migration ID that was attempted to roll back.
    /// </summary>
    public string MigrationId { get; }

    /// <summary>
    /// Creates a new exception for a blocked Down migration.
    /// </summary>
    /// <param name="migrationId">The migration ID being rolled back.</param>
    public ClickHouseDownMigrationNotSupportedException(string migrationId)
        : base($"Down migration '{migrationId}' is not supported. " +
               "ClickHouse lacks ACID transactions - partial rollbacks leave the database in an inconsistent state. " +
               "Use forward-only migrations: create a new migration that reverses the schema changes.")
    {
        MigrationId = migrationId;
    }
}
