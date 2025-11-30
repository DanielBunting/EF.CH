using System.Linq.Expressions;
using EF.CH.Metadata;
using EF.CH.Query.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Migrations.Internal;

/// <summary>
/// Generates ClickHouse-specific SQL for migrations, including MergeTree ENGINE clauses.
/// </summary>
public class ClickHouseMigrationsSqlGenerator : MigrationsSqlGenerator
{
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public ClickHouseMigrationsSqlGenerator(
        MigrationsSqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
        _typeMappingSource = dependencies.TypeMappingSource;
    }

    /// <summary>
    /// Generates CREATE TABLE or CREATE MATERIALIZED VIEW with ClickHouse ENGINE clause.
    /// </summary>
    protected override void Generate(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(builder);

        // Check if this is a materialized view
        var entityType = model?.GetEntityTypes()
            .FirstOrDefault(e => e.GetTableName() == operation.Name
                              && (e.GetSchema() ?? model.GetDefaultSchema()) == operation.Schema);

        var isMaterializedView = GetAnnotation<bool?>(operation, ClickHouseAnnotationNames.MaterializedView)
                              ?? GetEntityAnnotation<bool?>(entityType, ClickHouseAnnotationNames.MaterializedView)
                              ?? false;

        if (isMaterializedView)
        {
            GenerateMaterializedView(operation, entityType, model, builder, terminate);
            return;
        }

        builder
            .Append("CREATE TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema))
            .AppendLine(" (");

        using (builder.Indent())
        {
            CreateTableColumns(operation, model, builder);
            CreateTableConstraints(operation, model, builder);
        }

        builder.Append(")");

        // Add ENGINE clause
        GenerateEngineClause(operation, model, builder);

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>
    /// Generates CREATE MATERIALIZED VIEW statement.
    /// </summary>
    private void GenerateMaterializedView(
        CreateTableOperation operation,
        IEntityType? entityType,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate)
    {
        var viewQuery = GetAnnotation<string>(operation, ClickHouseAnnotationNames.MaterializedViewQuery)
                     ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.MaterializedViewQuery);
        var populate = GetAnnotation<bool?>(operation, ClickHouseAnnotationNames.MaterializedViewPopulate)
                    ?? GetEntityAnnotation<bool?>(entityType, ClickHouseAnnotationNames.MaterializedViewPopulate)
                    ?? false;

        // If no raw SQL query, check for LINQ expression
        if (string.IsNullOrEmpty(viewQuery) && entityType != null && model != null)
        {
            viewQuery = TranslateMaterializedViewExpression(entityType, model);
        }

        if (string.IsNullOrEmpty(viewQuery))
        {
            throw new InvalidOperationException(
                $"Materialized view '{operation.Name}' must have a view query defined via AsMaterializedViewRaw() or AsMaterializedView().");
        }

        builder
            .Append("CREATE MATERIALIZED VIEW ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema));

        builder.AppendLine();

        // Add ENGINE clause (materialized views need storage engine too)
        GenerateEngineClauseForView(operation, entityType, model, builder);

        // POPULATE clause - backfills existing data from source table
        if (populate)
        {
            builder.AppendLine();
            builder.Append("POPULATE");
        }

        // AS SELECT query
        builder.AppendLine();
        builder.Append("AS ");
        builder.Append(viewQuery);

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>
    /// Generates the ENGINE clause for materialized views (without column definitions).
    /// </summary>
    private void GenerateEngineClauseForView(
        CreateTableOperation operation,
        IEntityType? entityType,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        var engine = GetAnnotation<string>(operation, ClickHouseAnnotationNames.Engine)
                  ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.Engine)
                  ?? "MergeTree";
        var orderBy = GetAnnotation<string[]>(operation, ClickHouseAnnotationNames.OrderBy)
                   ?? GetEntityAnnotation<string[]>(entityType, ClickHouseAnnotationNames.OrderBy);
        var partitionBy = GetAnnotation<string>(operation, ClickHouseAnnotationNames.PartitionBy)
                       ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.PartitionBy);
        var primaryKey = GetAnnotation<string[]>(operation, ClickHouseAnnotationNames.PrimaryKey)
                      ?? GetEntityAnnotation<string[]>(entityType, ClickHouseAnnotationNames.PrimaryKey);
        var ttl = GetAnnotation<string>(operation, ClickHouseAnnotationNames.Ttl)
               ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.Ttl);
        var versionColumn = GetAnnotation<string>(operation, ClickHouseAnnotationNames.VersionColumn)
                         ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.VersionColumn);
        var settings = GetAnnotation<IDictionary<string, string>>(operation, ClickHouseAnnotationNames.Settings)
                    ?? GetEntityAnnotation<IDictionary<string, string>>(entityType, ClickHouseAnnotationNames.Settings);

        builder.Append("ENGINE = ");

        // Generate engine with parameters
        switch (engine)
        {
            case "ReplacingMergeTree" when !string.IsNullOrEmpty(versionColumn):
                builder.Append($"ReplacingMergeTree({Dependencies.SqlGenerationHelper.DelimitIdentifier(versionColumn)})");
                break;
            default:
                builder.Append(engine);
                if (engine.EndsWith("MergeTree", StringComparison.OrdinalIgnoreCase))
                {
                    builder.Append("()");
                }
                break;
        }

        // PARTITION BY
        if (!string.IsNullOrEmpty(partitionBy))
        {
            builder.AppendLine();
            builder.Append($"PARTITION BY {partitionBy}");
        }

        // ORDER BY (required for MergeTree family)
        if (orderBy is { Length: > 0 })
        {
            builder.AppendLine();
            builder.Append("ORDER BY (");
            builder.Append(string.Join(", ", orderBy.Select(c => Dependencies.SqlGenerationHelper.DelimitIdentifier(c))));
            builder.Append(")");
        }
        else if (engine.EndsWith("MergeTree", StringComparison.OrdinalIgnoreCase))
        {
            // Default ORDER BY tuple() for views without explicit ordering
            builder.AppendLine();
            builder.Append("ORDER BY tuple()");
        }

        // PRIMARY KEY (optional, defaults to ORDER BY)
        if (primaryKey is { Length: > 0 })
        {
            builder.AppendLine();
            builder.Append("PRIMARY KEY (");
            builder.Append(string.Join(", ", primaryKey.Select(c => Dependencies.SqlGenerationHelper.DelimitIdentifier(c))));
            builder.Append(")");
        }

        // TTL
        if (!string.IsNullOrEmpty(ttl))
        {
            builder.AppendLine();
            builder.Append($"TTL {ttl}");
        }

        // SETTINGS
        if (settings is { Count: > 0 })
        {
            builder.AppendLine();
            builder.Append("SETTINGS ");
            builder.Append(string.Join(", ", settings.Select(kvp => $"{kvp.Key} = {kvp.Value}")));
        }
    }

    /// <summary>
    /// Generates the ENGINE clause for MergeTree tables.
    /// </summary>
    private void GenerateEngineClause(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        // First try to get annotations from operation, then fall back to entity type in model
        var entityType = model?.GetEntityTypes()
            .FirstOrDefault(e => e.GetTableName() == operation.Name
                              && (e.GetSchema() ?? model.GetDefaultSchema()) == operation.Schema);

        // Get engine from annotations or default to MergeTree
        var engine = GetAnnotation<string>(operation, ClickHouseAnnotationNames.Engine)
                  ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.Engine)
                  ?? "MergeTree";
        var orderBy = GetAnnotation<string[]>(operation, ClickHouseAnnotationNames.OrderBy)
                   ?? GetEntityAnnotation<string[]>(entityType, ClickHouseAnnotationNames.OrderBy);
        var partitionBy = GetAnnotation<string>(operation, ClickHouseAnnotationNames.PartitionBy)
                       ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.PartitionBy);
        var primaryKey = GetAnnotation<string[]>(operation, ClickHouseAnnotationNames.PrimaryKey)
                      ?? GetEntityAnnotation<string[]>(entityType, ClickHouseAnnotationNames.PrimaryKey);
        var sampleBy = GetAnnotation<string>(operation, ClickHouseAnnotationNames.SampleBy)
                    ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.SampleBy);
        var ttl = GetAnnotation<string>(operation, ClickHouseAnnotationNames.Ttl)
               ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.Ttl);
        var versionColumn = GetAnnotation<string>(operation, ClickHouseAnnotationNames.VersionColumn)
                         ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.VersionColumn);
        var settings = GetAnnotation<IDictionary<string, string>>(operation, ClickHouseAnnotationNames.Settings)
                    ?? GetEntityAnnotation<IDictionary<string, string>>(entityType, ClickHouseAnnotationNames.Settings);

        builder.AppendLine();
        builder.Append("ENGINE = ");

        // Generate engine with parameters
        switch (engine)
        {
            case "ReplacingMergeTree" when !string.IsNullOrEmpty(versionColumn):
                builder.Append($"ReplacingMergeTree({Dependencies.SqlGenerationHelper.DelimitIdentifier(versionColumn)})");
                break;
            default:
                builder.Append(engine);
                if (engine.EndsWith("MergeTree", StringComparison.OrdinalIgnoreCase))
                {
                    builder.Append("()");
                }
                break;
        }

        // PARTITION BY
        if (!string.IsNullOrEmpty(partitionBy))
        {
            builder.AppendLine();
            builder.Append($"PARTITION BY {partitionBy}");
        }

        // ORDER BY (required for MergeTree family)
        if (orderBy is { Length: > 0 })
        {
            builder.AppendLine();
            builder.Append("ORDER BY (");
            builder.Append(string.Join(", ", orderBy.Select(c => Dependencies.SqlGenerationHelper.DelimitIdentifier(c))));
            builder.Append(")");
        }
        else if (engine.EndsWith("MergeTree", StringComparison.OrdinalIgnoreCase))
        {
            // Default ORDER BY using primary key columns
            var pkColumns = operation.PrimaryKey?.Columns;
            if (pkColumns is { Length: > 0 })
            {
                builder.AppendLine();
                builder.Append("ORDER BY (");
                builder.Append(string.Join(", ", pkColumns.Select(c => Dependencies.SqlGenerationHelper.DelimitIdentifier(c))));
                builder.Append(")");
            }
            else
            {
                // Use tuple() for no ordering (not recommended but valid)
                builder.AppendLine();
                builder.Append("ORDER BY tuple()");
            }
        }

        // PRIMARY KEY (optional, defaults to ORDER BY)
        if (primaryKey is { Length: > 0 })
        {
            builder.AppendLine();
            builder.Append("PRIMARY KEY (");
            builder.Append(string.Join(", ", primaryKey.Select(c => Dependencies.SqlGenerationHelper.DelimitIdentifier(c))));
            builder.Append(")");
        }

        // SAMPLE BY
        if (!string.IsNullOrEmpty(sampleBy))
        {
            builder.AppendLine();
            builder.Append($"SAMPLE BY {sampleBy}");
        }

        // TTL
        if (!string.IsNullOrEmpty(ttl))
        {
            builder.AppendLine();
            builder.Append($"TTL {ttl}");
        }

        // SETTINGS
        if (settings is { Count: > 0 })
        {
            builder.AppendLine();
            builder.Append("SETTINGS ");
            builder.Append(string.Join(", ", settings.Select(kvp => $"{kvp.Key} = {kvp.Value}")));
        }
    }

    /// <summary>
    /// Override to skip foreign key constraints (ClickHouse doesn't support them).
    /// </summary>
    protected override void CreateTableConstraints(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        // ClickHouse doesn't use PRIMARY KEY constraint syntax in column definitions
        // The PRIMARY KEY is specified after ENGINE clause
        // Skip foreign keys - ClickHouse doesn't support them
    }

    /// <summary>
    /// Generate DROP TABLE.
    /// </summary>
    protected override void Generate(
        DropTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(builder);

        builder
            .Append("DROP TABLE IF EXISTS ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema));

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>
    /// Generate ALTER TABLE ADD COLUMN.
    /// </summary>
    protected override void Generate(
        AddColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(builder);

        builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" ADD COLUMN ");

        ColumnDefinition(operation, model, builder);

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>
    /// Generate ALTER TABLE DROP COLUMN.
    /// </summary>
    protected override void Generate(
        DropColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(builder);

        builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" DROP COLUMN ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name));

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>
    /// Generate column definition with ClickHouse types.
    /// </summary>
    protected override void ColumnDefinition(
        AddColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(builder);

        builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name));
        builder.Append(" ");

        var columnType = operation.ColumnType
            ?? GetColumnType(operation.Schema, operation.Table, operation.Name, operation, model)
            ?? "String"; // Default to String if type cannot be determined

        // Wrap nullable columns with Nullable()
        if (operation.IsNullable && !columnType.StartsWith("Nullable(", StringComparison.OrdinalIgnoreCase))
        {
            builder.Append($"Nullable({columnType})");
        }
        else
        {
            builder.Append(columnType);
        }

        // Default value
        if (operation.DefaultValue != null)
        {
            var typeMapping = _typeMappingSource.FindMapping(operation.DefaultValue.GetType());
            builder.Append(" DEFAULT ");
            builder.Append(typeMapping?.GenerateSqlLiteral(operation.DefaultValue)
                ?? operation.DefaultValue.ToString() ?? "NULL");
        }
        else if (!string.IsNullOrEmpty(operation.DefaultValueSql))
        {
            builder.Append(" DEFAULT ");
            builder.Append(operation.DefaultValueSql);
        }
    }

    /// <summary>
    /// Throws NotSupportedException - ClickHouse doesn't support foreign keys.
    /// </summary>
    protected override void Generate(
        AddForeignKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        throw new NotSupportedException(
            $"ClickHouse does not support foreign key constraints. " +
            $"Cannot add foreign key '{operation.Name}' on table '{operation.Table}'.");
    }

    /// <summary>
    /// Throws NotSupportedException - ClickHouse doesn't support foreign keys.
    /// </summary>
    protected override void Generate(
        DropForeignKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        throw new NotSupportedException(
            $"ClickHouse does not support foreign key constraints. " +
            $"Cannot drop foreign key '{operation.Name}' on table '{operation.Table}'.");
    }

    /// <summary>
    /// Throws NotSupportedException for adding primary keys after table creation.
    /// ClickHouse primary keys are defined at table creation via ORDER BY.
    /// </summary>
    protected override void Generate(
        AddPrimaryKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        throw new NotSupportedException(
            $"ClickHouse does not support adding primary keys after table creation. " +
            $"Primary keys are defined via ORDER BY clause at table creation time. " +
            $"Table: '{operation.Table}'.");
    }

    /// <summary>
    /// Throws NotSupportedException for dropping primary keys.
    /// ClickHouse primary keys are defined at table creation via ORDER BY.
    /// </summary>
    protected override void Generate(
        DropPrimaryKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        throw new NotSupportedException(
            $"ClickHouse does not support dropping primary keys. " +
            $"Primary keys are defined via ORDER BY clause at table creation time. " +
            $"Table: '{operation.Table}'.");
    }

    /// <summary>
    /// Throws NotSupportedException for renaming columns.
    /// ClickHouse doesn't support RENAME COLUMN.
    /// </summary>
    protected override void Generate(
        RenameColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        throw new NotSupportedException(
            $"ClickHouse does not support renaming columns. " +
            $"Consider adding a new column and migrating data instead. " +
            $"Table: '{operation.Table}', Column: '{operation.Name}'.");
    }

    /// <summary>
    /// Generates RENAME TABLE for table rename operations.
    /// </summary>
    protected override void Generate(
        RenameTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(builder);

        var oldName = Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema);
        var newName = Dependencies.SqlGenerationHelper.DelimitIdentifier(
            operation.NewName ?? operation.Name,
            operation.NewSchema ?? operation.Schema);

        builder
            .Append("RENAME TABLE ")
            .Append(oldName)
            .Append(" TO ")
            .Append(newName)
            .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);

        EndStatement(builder);
    }

    /// <summary>
    /// Generates ALTER TABLE MODIFY COLUMN for column alterations.
    /// Note: ClickHouse has limited ALTER support.
    /// </summary>
    protected override void Generate(
        AlterColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(builder);

        var tableName = Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema);
        var columnName = Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name);

        var columnType = operation.ColumnType
            ?? GetColumnType(operation.Schema, operation.Table, operation.Name, operation, model)
            ?? "String";

        // Wrap nullable columns with Nullable()
        if (operation.IsNullable && !columnType.StartsWith("Nullable(", StringComparison.OrdinalIgnoreCase))
        {
            columnType = $"Nullable({columnType})";
        }

        builder
            .Append("ALTER TABLE ")
            .Append(tableName)
            .Append(" MODIFY COLUMN ")
            .Append(columnName)
            .Append(" ")
            .Append(columnType);

        // Default value
        if (operation.DefaultValue != null)
        {
            var typeMapping = _typeMappingSource.FindMapping(operation.DefaultValue.GetType());
            builder.Append(" DEFAULT ");
            builder.Append(typeMapping?.GenerateSqlLiteral(operation.DefaultValue)
                ?? operation.DefaultValue.ToString() ?? "NULL");
        }
        else if (!string.IsNullOrEmpty(operation.DefaultValueSql))
        {
            builder.Append(" DEFAULT ");
            builder.Append(operation.DefaultValueSql);
        }

        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder);
    }

    /// <summary>
    /// Generates CREATE INDEX. Throws for unique indexes which ClickHouse doesn't support.
    /// </summary>
    protected override void Generate(
        CreateIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(builder);

        if (operation.IsUnique)
        {
            throw new NotSupportedException(
                $"ClickHouse does not support unique indexes. " +
                $"Consider using ReplacingMergeTree engine for deduplication. " +
                $"Index: '{operation.Name}' on table '{operation.Table}'.");
        }

        // ClickHouse uses ALTER TABLE ADD INDEX syntax for data skipping indexes
        var tableName = Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema);
        var indexName = Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name);

        builder
            .Append("ALTER TABLE ")
            .Append(tableName)
            .Append(" ADD INDEX ")
            .Append(indexName)
            .Append(" (");

        builder.Append(string.Join(", ",
            operation.Columns.Select(c => Dependencies.SqlGenerationHelper.DelimitIdentifier(c))));

        builder.Append(") TYPE minmax GRANULARITY 3");

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>
    /// Generates ALTER TABLE DROP INDEX.
    /// </summary>
    protected override void Generate(
        DropIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(builder);

        var tableName = Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table!, operation.Schema);
        var indexName = Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name);

        builder
            .Append("ALTER TABLE ")
            .Append(tableName)
            .Append(" DROP INDEX ")
            .Append(indexName);

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>
    /// Translates a stored LINQ expression to ClickHouse SQL for materialized views.
    /// </summary>
    private static string? TranslateMaterializedViewExpression(IEntityType entityType, IModel model)
    {
        const string expressionAnnotation = "ClickHouse:MaterializedViewExpression";

        var annotation = entityType.FindAnnotation(expressionAnnotation);
        if (annotation?.Value is not LambdaExpression expression)
            return null;

        var sourceTableName = GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.MaterializedViewSource);
        if (string.IsNullOrEmpty(sourceTableName))
            return null;

        // Get the source and result types from the expression
        var funcType = expression.Type;
        if (!funcType.IsGenericType || funcType.GetGenericTypeDefinition() != typeof(Func<,>))
            return null;

        var genericArgs = funcType.GetGenericArguments();
        // genericArgs[0] = IQueryable<TSource>, genericArgs[1] = IQueryable<TResult>
        var sourceQueryableType = genericArgs[0];
        var resultQueryableType = genericArgs[1];

        if (!sourceQueryableType.IsGenericType || !resultQueryableType.IsGenericType)
            return null;

        var sourceType = sourceQueryableType.GetGenericArguments()[0];

        // Create the translator and translate the expression
        var translator = new MaterializedViewSqlTranslator(model, sourceTableName);

        // Use reflection to call the generic Translate method
        var translateMethod = typeof(MaterializedViewSqlTranslator)
            .GetMethod(nameof(MaterializedViewSqlTranslator.Translate))!
            .MakeGenericMethod(sourceType, entityType.ClrType);

        try
        {
            return (string?)translateMethod.Invoke(translator, [expression]);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Failed to translate materialized view expression for entity '{entityType.Name}': {ex.Message}",
                ex);
        }
    }

    /// <summary>
    /// Helper to get annotation value from operation.
    /// </summary>
    private static T? GetAnnotation<T>(MigrationOperation operation, string name)
    {
        var annotation = operation.FindAnnotation(name);
        return annotation?.Value is T value ? value : default;
    }

    /// <summary>
    /// Helper to get annotation value from entity type.
    /// </summary>
    private static T? GetEntityAnnotation<T>(IEntityType? entityType, string name)
    {
        if (entityType == null)
            return default;
        var annotation = entityType.FindAnnotation(name);
        return annotation?.Value is T value ? value : default;
    }
}
