using EF.CH.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations.Builders;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for MigrationBuilder to add ClickHouse-specific operations.
/// </summary>
public static class ClickHouseMigrationBuilderExtensions
{
    /// <summary>
    /// Adds a projection to an existing table.
    /// </summary>
    /// <param name="migrationBuilder">The migration builder.</param>
    /// <param name="table">The table name.</param>
    /// <param name="name">The projection name.</param>
    /// <param name="selectSql">The projection SELECT SQL.</param>
    /// <param name="schema">The table schema (optional).</param>
    /// <param name="materialize">Whether to materialize existing data (default: true).</param>
    /// <returns>The operation builder.</returns>
    /// <example>
    /// <code>
    /// migrationBuilder.AddProjection(
    ///     table: "orders",
    ///     name: "prj_by_status",
    ///     selectSql: "SELECT * ORDER BY (\"Status\", \"OrderDate\")");
    /// </code>
    /// </example>
    public static OperationBuilder<AddProjectionOperation> AddProjection(
        this MigrationBuilder migrationBuilder,
        string table,
        string name,
        string selectSql,
        string? schema = null,
        bool materialize = true)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(selectSql);

        var operation = new AddProjectionOperation
        {
            Table = table,
            Schema = schema,
            Name = name,
            SelectSql = selectSql,
            Materialize = materialize
        };

        migrationBuilder.Operations.Add(operation);
        return new OperationBuilder<AddProjectionOperation>(operation);
    }

    /// <summary>
    /// Drops a projection from a table.
    /// </summary>
    /// <param name="migrationBuilder">The migration builder.</param>
    /// <param name="table">The table name.</param>
    /// <param name="name">The projection name to drop.</param>
    /// <param name="schema">The table schema (optional).</param>
    /// <param name="ifExists">Whether to use IF EXISTS (default: true).</param>
    /// <returns>The operation builder.</returns>
    /// <example>
    /// <code>
    /// migrationBuilder.DropProjection(
    ///     table: "orders",
    ///     name: "prj_old");
    /// </code>
    /// </example>
    public static OperationBuilder<DropProjectionOperation> DropProjection(
        this MigrationBuilder migrationBuilder,
        string table,
        string name,
        string? schema = null,
        bool ifExists = true)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var operation = new DropProjectionOperation
        {
            Table = table,
            Schema = schema,
            Name = name,
            IfExists = ifExists
        };

        migrationBuilder.Operations.Add(operation);
        return new OperationBuilder<DropProjectionOperation>(operation);
    }

    /// <summary>
    /// Materializes a projection for existing data.
    /// </summary>
    /// <param name="migrationBuilder">The migration builder.</param>
    /// <param name="table">The table name.</param>
    /// <param name="name">The projection name to materialize.</param>
    /// <param name="schema">The table schema (optional).</param>
    /// <param name="inPartition">Optional partition to materialize.</param>
    /// <returns>The operation builder.</returns>
    /// <example>
    /// <code>
    /// // Materialize all partitions
    /// migrationBuilder.MaterializeProjection(
    ///     table: "orders",
    ///     name: "prj_by_status");
    ///
    /// // Materialize specific partition
    /// migrationBuilder.MaterializeProjection(
    ///     table: "orders",
    ///     name: "prj_by_status",
    ///     inPartition: "202401");
    /// </code>
    /// </example>
    public static OperationBuilder<MaterializeProjectionOperation> MaterializeProjection(
        this MigrationBuilder migrationBuilder,
        string table,
        string name,
        string? schema = null,
        string? inPartition = null)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var operation = new MaterializeProjectionOperation
        {
            Table = table,
            Schema = schema,
            Name = name,
            InPartition = inPartition
        };

        migrationBuilder.Operations.Add(operation);
        return new OperationBuilder<MaterializeProjectionOperation>(operation);
    }
}
