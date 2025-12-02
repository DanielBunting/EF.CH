namespace EF.CH.Infrastructure;

/// <summary>
/// Exception thrown when an operation is not supported by ClickHouse.
/// Provides rich context about the unsupported operation and suggested alternatives.
/// </summary>
public class ClickHouseUnsupportedOperationException : NotSupportedException
{
    /// <summary>
    /// Categories of unsupported operations for structured handling.
    /// </summary>
    public enum OperationCategory
    {
        /// <summary>Row-level UPDATE operations.</summary>
        Update,
        /// <summary>ACID transactions.</summary>
        Transaction,
        /// <summary>Foreign key constraints.</summary>
        ForeignKey,
        /// <summary>Primary key modification after table creation.</summary>
        PrimaryKey,
        /// <summary>Column rename operations.</summary>
        ColumnRename,
        /// <summary>Unique indexes/constraints.</summary>
        UniqueConstraint,
        /// <summary>Auto-increment/identity columns.</summary>
        Identity
    }

    /// <summary>
    /// The category of unsupported operation.
    /// </summary>
    public OperationCategory Category { get; }

    /// <summary>
    /// The table name involved in the operation, if applicable.
    /// </summary>
    public string? TableName { get; }

    /// <summary>
    /// The column name involved in the operation, if applicable.
    /// </summary>
    public string? ColumnName { get; }

    /// <summary>
    /// The constraint or index name involved in the operation, if applicable.
    /// </summary>
    public string? ObjectName { get; }

    /// <summary>
    /// Suggested workaround for the unsupported operation.
    /// </summary>
    public string? Workaround { get; }

    private ClickHouseUnsupportedOperationException(
        string message,
        OperationCategory category,
        string? tableName = null,
        string? columnName = null,
        string? objectName = null,
        string? workaround = null)
        : base(message)
    {
        Category = category;
        TableName = tableName;
        ColumnName = columnName;
        ObjectName = objectName;
        Workaround = workaround;
    }

    /// <summary>
    /// Creates an exception for UPDATE operations.
    /// </summary>
    public static ClickHouseUnsupportedOperationException Update(string tableName) =>
        new(
            $"ClickHouse does not support efficient row-level UPDATE operations. " +
            $"Consider using INSERT with ReplacingMergeTree engine for updates, " +
            $"or delete and re-insert the entity. Table: '{tableName}'.",
            OperationCategory.Update,
            tableName: tableName,
            workaround: "Use ReplacingMergeTree engine with delete-and-reinsert pattern, or use INSERT to add new versions.");

    /// <summary>
    /// Creates an exception for transaction operations.
    /// </summary>
    public static ClickHouseUnsupportedOperationException Transaction() =>
        new(
            "ClickHouse does not support ACID transactions. " +
            "Writes are atomic at the block level but there is no rollback capability. " +
            "Consider using idempotent operations or implementing saga patterns for complex workflows. " +
            "For bulk operations, use the ClickHouse-specific batch insert functionality.",
            OperationCategory.Transaction,
            workaround: "Use idempotent operations, saga patterns, or batch inserts.");

    /// <summary>
    /// Creates an exception for adding foreign key constraints.
    /// </summary>
    public static ClickHouseUnsupportedOperationException AddForeignKey(string? constraintName, string tableName) =>
        new(
            $"ClickHouse does not support foreign key constraints. " +
            $"Cannot add foreign key '{constraintName}' on table '{tableName}'.",
            OperationCategory.ForeignKey,
            tableName: tableName,
            objectName: constraintName,
            workaround: "Enforce referential integrity at the application level.");

    /// <summary>
    /// Creates an exception for dropping foreign key constraints.
    /// </summary>
    public static ClickHouseUnsupportedOperationException DropForeignKey(string? constraintName, string tableName) =>
        new(
            $"ClickHouse does not support foreign key constraints. " +
            $"Cannot drop foreign key '{constraintName}' on table '{tableName}'.",
            OperationCategory.ForeignKey,
            tableName: tableName,
            objectName: constraintName,
            workaround: "Enforce referential integrity at the application level.");

    /// <summary>
    /// Creates an exception for adding primary key after table creation.
    /// </summary>
    public static ClickHouseUnsupportedOperationException AddPrimaryKey(string tableName) =>
        new(
            $"ClickHouse does not support adding primary keys after table creation. " +
            $"Primary keys are defined via ORDER BY clause at table creation time. " +
            $"Table: '{tableName}'.",
            OperationCategory.PrimaryKey,
            tableName: tableName,
            workaround: "Define ORDER BY clause when creating the table, or recreate the table with the desired key.");

    /// <summary>
    /// Creates an exception for dropping primary key.
    /// </summary>
    public static ClickHouseUnsupportedOperationException DropPrimaryKey(string tableName) =>
        new(
            $"ClickHouse does not support dropping primary keys. " +
            $"Primary keys are defined via ORDER BY clause at table creation time. " +
            $"Table: '{tableName}'.",
            OperationCategory.PrimaryKey,
            tableName: tableName,
            workaround: "Recreate the table with the desired ORDER BY clause.");

    /// <summary>
    /// Creates an exception for column rename operations.
    /// </summary>
    public static ClickHouseUnsupportedOperationException RenameColumn(string tableName, string columnName) =>
        new(
            $"ClickHouse does not support renaming columns. " +
            $"Consider adding a new column and migrating data instead. " +
            $"Table: '{tableName}', Column: '{columnName}'.",
            OperationCategory.ColumnRename,
            tableName: tableName,
            columnName: columnName,
            workaround: "Add a new column, copy data, and drop the old column.");

    /// <summary>
    /// Creates an exception for unique index creation.
    /// </summary>
    public static ClickHouseUnsupportedOperationException UniqueIndex(string? indexName, string tableName) =>
        new(
            $"ClickHouse does not support unique indexes. " +
            $"Consider using ReplacingMergeTree engine for deduplication. " +
            $"Index: '{indexName}' on table '{tableName}'.",
            OperationCategory.UniqueConstraint,
            tableName: tableName,
            objectName: indexName,
            workaround: "Use ReplacingMergeTree engine for deduplication based on ORDER BY columns.");

    /// <summary>
    /// Creates an exception for identity/auto-increment column configuration.
    /// </summary>
    public static ClickHouseUnsupportedOperationException Identity(string tableName, string columnName) =>
        new(
            $"ClickHouse does not support auto-increment/identity columns. " +
            $"Table: '{tableName}', Column: '{columnName}'. " +
            $"Use Guid for client-generated IDs, provide a value generator, or set a default value.",
            OperationCategory.Identity,
            tableName: tableName,
            columnName: columnName,
            workaround: "Use Guid type, call .ValueGeneratedNever(), provide .HasValueGenerator(), or set .HasDefaultValueSql().");
}
