using System.Text;
using EF.CH.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations.Builders;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for MigrationBuilder to add ClickHouse-specific operations.
/// </summary>
public static class ClickHouseMigrationBuilderExtensions
{
    #region Parameterized Views

    /// <summary>
    /// Creates a ClickHouse parameterized view.
    /// </summary>
    /// <param name="migrationBuilder">The migration builder.</param>
    /// <param name="viewName">The name of the view to create.</param>
    /// <param name="selectSql">
    /// The SELECT SQL for the view, including parameter placeholders.
    /// Use ClickHouse syntax: <c>{parameter_name:Type}</c> for parameters.
    /// </param>
    /// <param name="parameters">
    /// The parameter definitions as (name, ClickHouse type) tuples.
    /// These should match the placeholders in the selectSql.
    /// </param>
    /// <param name="schema">The optional schema name.</param>
    /// <returns>The migration builder for chaining.</returns>
    /// <remarks>
    /// <para>
    /// Parameterized views in ClickHouse allow you to create views with parameters that
    /// are substituted at query time. The view is queried using:
    /// <c>SELECT * FROM view_name(param1 = value1, param2 = value2)</c>
    /// </para>
    /// <para>
    /// Common ClickHouse types for parameters:
    /// - UInt8, UInt16, UInt32, UInt64 - unsigned integers
    /// - Int8, Int16, Int32, Int64 - signed integers
    /// - Float32, Float64 - floating point
    /// - String - text
    /// - Date, DateTime, DateTime64 - date/time types
    /// - UUID - unique identifiers
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// migrationBuilder.CreateParameterizedView(
    ///     viewName: "user_events_view",
    ///     selectSql: @"
    ///         SELECT event_id, event_type, timestamp, user_id
    ///         FROM events
    ///         WHERE user_id = {user_id:UInt64}
    ///           AND timestamp >= {start_date:DateTime}
    ///           AND timestamp &lt; {end_date:DateTime}
    ///     ",
    ///     parameters: new[]
    ///     {
    ///         ("user_id", "UInt64"),
    ///         ("start_date", "DateTime"),
    ///         ("end_date", "DateTime")
    ///     });
    /// </code>
    /// </example>
    public static MigrationBuilder CreateParameterizedView(
        this MigrationBuilder migrationBuilder,
        string viewName,
        string selectSql,
        IEnumerable<(string Name, string Type)> parameters,
        string? schema = null)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);
        ArgumentException.ThrowIfNullOrWhiteSpace(selectSql);
        ArgumentNullException.ThrowIfNull(parameters);

        var sql = BuildCreateParameterizedViewSql(viewName, selectSql, parameters, schema);
        migrationBuilder.Sql(sql);

        return migrationBuilder;
    }

    /// <summary>
    /// Creates a ClickHouse parameterized view using a dictionary for parameters.
    /// </summary>
    /// <param name="migrationBuilder">The migration builder.</param>
    /// <param name="viewName">The name of the view to create.</param>
    /// <param name="selectSql">
    /// The SELECT SQL for the view, including parameter placeholders.
    /// Use ClickHouse syntax: <c>{parameter_name:Type}</c> for parameters.
    /// </param>
    /// <param name="parameters">
    /// The parameter definitions as a dictionary of name to ClickHouse type.
    /// </param>
    /// <param name="schema">The optional schema name.</param>
    /// <returns>The migration builder for chaining.</returns>
    public static MigrationBuilder CreateParameterizedView(
        this MigrationBuilder migrationBuilder,
        string viewName,
        string selectSql,
        IDictionary<string, string> parameters,
        string? schema = null)
    {
        return CreateParameterizedView(
            migrationBuilder,
            viewName,
            selectSql,
            parameters.Select(kvp => (kvp.Key, kvp.Value)),
            schema);
    }

    /// <summary>
    /// Drops a ClickHouse parameterized view.
    /// </summary>
    /// <param name="migrationBuilder">The migration builder.</param>
    /// <param name="viewName">The name of the view to drop.</param>
    /// <param name="schema">The optional schema name.</param>
    /// <param name="ifExists">Whether to include IF EXISTS clause (default: true).</param>
    /// <returns>The migration builder for chaining.</returns>
    /// <example>
    /// <code>
    /// migrationBuilder.DropParameterizedView("user_events_view");
    /// </code>
    /// </example>
    public static MigrationBuilder DropParameterizedView(
        this MigrationBuilder migrationBuilder,
        string viewName,
        string? schema = null,
        bool ifExists = true)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);

        var sql = BuildDropParameterizedViewSql(viewName, schema, ifExists);
        migrationBuilder.Sql(sql);

        return migrationBuilder;
    }

    private static string BuildCreateParameterizedViewSql(
        string viewName,
        string selectSql,
        IEnumerable<(string Name, string Type)> parameters,
        string? schema)
    {
        var sb = new StringBuilder();
        sb.Append("CREATE VIEW ");

        // Add schema prefix if specified
        if (!string.IsNullOrEmpty(schema))
        {
            sb.Append('"');
            sb.Append(schema);
            sb.Append("\".\"");
            sb.Append(viewName);
            sb.Append('"');
        }
        else
        {
            sb.Append('"');
            sb.Append(viewName);
            sb.Append('"');
        }

        sb.AppendLine(" AS");
        sb.Append(selectSql.Trim());

        return sb.ToString();
    }

    private static string BuildDropParameterizedViewSql(
        string viewName,
        string? schema,
        bool ifExists)
    {
        var sb = new StringBuilder();
        sb.Append("DROP VIEW ");

        if (ifExists)
        {
            sb.Append("IF EXISTS ");
        }

        // Add schema prefix if specified
        if (!string.IsNullOrEmpty(schema))
        {
            sb.Append('"');
            sb.Append(schema);
            sb.Append("\".\"");
            sb.Append(viewName);
            sb.Append('"');
        }
        else
        {
            sb.Append('"');
            sb.Append(viewName);
            sb.Append('"');
        }

        return sb.ToString();
    }

    #endregion

    #region Projections

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

    #endregion
}
