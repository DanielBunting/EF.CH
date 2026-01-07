using System.Linq.Expressions;
using EF.CH.Dictionaries;
using EF.CH.Infrastructure;
using EF.CH.Metadata;
using EF.CH.Migrations.Operations;
using EF.CH.Projections;
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
    /// Override the main Generate method to sort operations so that source tables
    /// are created before materialized views and dictionaries that depend on them.
    /// </summary>
    public override IReadOnlyList<MigrationCommand> Generate(
        IReadOnlyList<MigrationOperation> operations,
        IModel? model,
        MigrationsSqlGenerationOptions options = MigrationsSqlGenerationOptions.Default)
    {
        var sortedOperations = SortOperationsForClickHouse(operations, model);
        return base.Generate(sortedOperations, model, options);
    }

    /// <summary>
    /// Sorts migration operations to ensure proper dependency ordering:
    /// 1. Source tables are created before materialized views and dictionaries that depend on them
    /// 2. Tables are created before any operations that modify them (indices, projections, columns, etc.)
    /// </summary>
    private IReadOnlyList<MigrationOperation> SortOperationsForClickHouse(
        IReadOnlyList<MigrationOperation> operations,
        IModel? model)
    {
        if (operations.Count < 2)
        {
            return operations;
        }

        // Build a set of tables being created in this migration
        var tablesBeingCreated = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var op in operations)
        {
            if (op is CreateTableOperation createOp)
            {
                tablesBeingCreated.Add(createOp.Name);
            }
        }

        // If no tables being created, no reordering needed
        if (tablesBeingCreated.Count == 0)
        {
            return operations;
        }

        // Build dependency graph: operation index -> set of table names it depends on
        var dependencies = new Dictionary<int, HashSet<string>>();
        for (int i = 0; i < operations.Count; i++)
        {
            var deps = GetOperationDependencies(operations[i], model);
            // Only track dependencies on tables being created in this migration
            deps.IntersectWith(tablesBeingCreated);
            dependencies[i] = deps;
        }

        // Build reverse map: table name -> index of its CreateTableOperation
        var tableToCreateIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < operations.Count; i++)
        {
            if (operations[i] is CreateTableOperation createOp)
            {
                tableToCreateIndex[createOp.Name] = i;
            }
        }

        // Topological sort using Kahn's algorithm
        // Calculate in-degree for each operation (how many CreateTableOperations must come before it)
        var inDegree = new int[operations.Count];
        for (int i = 0; i < operations.Count; i++)
        {
            foreach (var dep in dependencies[i])
            {
                if (tableToCreateIndex.TryGetValue(dep, out var createIdx) && createIdx != i)
                {
                    inDegree[i]++;
                }
            }
        }

        // Start with operations that have no dependencies (in-degree = 0)
        var ready = new List<int>();
        for (int i = 0; i < operations.Count; i++)
        {
            if (inDegree[i] == 0)
            {
                ready.Add(i);
            }
        }

        var result = new List<MigrationOperation>(operations.Count);
        var processed = new HashSet<int>();

        while (ready.Count > 0)
        {
            // Sort ready operations by original index to maintain relative order
            ready.Sort();

            // Process all currently ready operations
            var currentBatch = new List<int>(ready);
            ready.Clear();

            foreach (var idx in currentBatch)
            {
                if (processed.Contains(idx))
                    continue;

                result.Add(operations[idx]);
                processed.Add(idx);

                // If this is a CreateTableOperation, decrease in-degree of dependent operations
                if (operations[idx] is CreateTableOperation createOp)
                {
                    for (int i = 0; i < operations.Count; i++)
                    {
                        if (!processed.Contains(i) && dependencies[i].Contains(createOp.Name))
                        {
                            inDegree[i]--;
                            if (inDegree[i] == 0)
                            {
                                ready.Add(i);
                            }
                        }
                    }
                }
            }
        }

        // If we couldn't process all operations (cycle detected), add remaining in original order
        if (result.Count < operations.Count)
        {
            for (int i = 0; i < operations.Count; i++)
            {
                if (!processed.Contains(i))
                {
                    result.Add(operations[i]);
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Gets the table names that an operation depends on.
    /// </summary>
    private HashSet<string> GetOperationDependencies(MigrationOperation operation, IModel? model)
    {
        var deps = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        switch (operation)
        {
            case CreateTableOperation createOp:
                // MVs and Dictionaries depend on their source tables
                var sourceDep = GetSourceTableDependency(createOp, model);
                if (!string.IsNullOrEmpty(sourceDep))
                {
                    deps.Add(sourceDep);
                }
                break;

            case CreateIndexOperation indexOp when !string.IsNullOrEmpty(indexOp.Table):
                deps.Add(indexOp.Table);
                break;

            case DropIndexOperation dropIndexOp when !string.IsNullOrEmpty(dropIndexOp.Table):
                deps.Add(dropIndexOp.Table);
                break;

            case AddColumnOperation addColOp when !string.IsNullOrEmpty(addColOp.Table):
                deps.Add(addColOp.Table);
                break;

            case DropColumnOperation dropColOp when !string.IsNullOrEmpty(dropColOp.Table):
                deps.Add(dropColOp.Table);
                break;

            case AlterColumnOperation alterColOp when !string.IsNullOrEmpty(alterColOp.Table):
                deps.Add(alterColOp.Table);
                break;

            case RenameColumnOperation renameColOp when !string.IsNullOrEmpty(renameColOp.Table):
                deps.Add(renameColOp.Table);
                break;

            case AddPrimaryKeyOperation addPkOp when !string.IsNullOrEmpty(addPkOp.Table):
                deps.Add(addPkOp.Table);
                break;

            case DropPrimaryKeyOperation dropPkOp when !string.IsNullOrEmpty(dropPkOp.Table):
                deps.Add(dropPkOp.Table);
                break;

            case AddForeignKeyOperation addFkOp when !string.IsNullOrEmpty(addFkOp.Table):
                deps.Add(addFkOp.Table);
                break;

            case DropForeignKeyOperation dropFkOp when !string.IsNullOrEmpty(dropFkOp.Table):
                deps.Add(dropFkOp.Table);
                break;

            case AddUniqueConstraintOperation addUniqueOp when !string.IsNullOrEmpty(addUniqueOp.Table):
                deps.Add(addUniqueOp.Table);
                break;

            case DropUniqueConstraintOperation dropUniqueOp when !string.IsNullOrEmpty(dropUniqueOp.Table):
                deps.Add(dropUniqueOp.Table);
                break;

            case AddCheckConstraintOperation addCheckOp when !string.IsNullOrEmpty(addCheckOp.Table):
                deps.Add(addCheckOp.Table);
                break;

            case DropCheckConstraintOperation dropCheckOp when !string.IsNullOrEmpty(dropCheckOp.Table):
                deps.Add(dropCheckOp.Table);
                break;

            // Custom ClickHouse operations
            case Migrations.Operations.AddProjectionOperation addProjOp when !string.IsNullOrEmpty(addProjOp.Table):
                deps.Add(addProjOp.Table);
                break;

            case Migrations.Operations.DropProjectionOperation dropProjOp when !string.IsNullOrEmpty(dropProjOp.Table):
                deps.Add(dropProjOp.Table);
                break;

            case Migrations.Operations.MaterializeProjectionOperation matProjOp when !string.IsNullOrEmpty(matProjOp.Table):
                deps.Add(matProjOp.Table);
                break;

            // DropTableOperation, RenameTableOperation, SqlOperation, etc. have no table dependency
            // (or operate on tables that must already exist)
        }

        return deps;
    }

    /// <summary>
    /// Gets the source table that a CreateTableOperation depends on, if any.
    /// Materialized views depend on their source table.
    /// Dictionaries sourced from ClickHouse tables depend on that table.
    /// </summary>
    private string? GetSourceTableDependency(CreateTableOperation operation, IModel? model)
    {
        // Get entity type for additional annotation lookup
        var entityType = model?.GetEntityTypes()
            .FirstOrDefault(e => e.GetTableName() == operation.Name
                              && (e.GetSchema() ?? model.GetDefaultSchema()) == operation.Schema);

        // Check for materialized view source
        var mvSource = GetAnnotation<string>(operation, ClickHouseAnnotationNames.MaterializedViewSource)
                    ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.MaterializedViewSource);
        if (!string.IsNullOrEmpty(mvSource))
        {
            return mvSource;
        }

        // Check for dictionary source (only for ClickHouse-sourced dictionaries)
        var dictSource = GetAnnotation<string>(operation, ClickHouseAnnotationNames.DictionarySource)
                      ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.DictionarySource);
        var sourceProvider = GetAnnotation<string>(operation, ClickHouseAnnotationNames.DictionarySourceProvider)
                          ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.DictionarySourceProvider)
                          ?? "clickhouse";

        // Only consider dependency if dictionary is sourced from a ClickHouse table
        if (!string.IsNullOrEmpty(dictSource) && sourceProvider == "clickhouse")
        {
            return dictSource;
        }

        return null;
    }

    /// <summary>
    /// Generates CREATE TABLE or CREATE MATERIALIZED VIEW with ClickHouse ENGINE clause.
    /// Skips DDL for external entities (they use table functions, not physical tables).
    /// </summary>
    protected override void Generate(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(builder);

        // Check if this is a materialized view, dictionary, or external entity
        var entityType = model?.GetEntityTypes()
            .FirstOrDefault(e => e.GetTableName() == operation.Name
                              && (e.GetSchema() ?? model.GetDefaultSchema()) == operation.Schema);

        // Skip DDL for external entities - they use table functions like postgresql(...)
        var isExternal = GetAnnotation<bool?>(operation, ClickHouseAnnotationNames.IsExternalTableFunction)
                      ?? GetEntityAnnotation<bool?>(entityType, ClickHouseAnnotationNames.IsExternalTableFunction)
                      ?? false;

        if (isExternal)
        {
            // External entities don't create ClickHouse tables - no DDL needed
            return;
        }

        var isMaterializedView = GetAnnotation<bool?>(operation, ClickHouseAnnotationNames.MaterializedView)
                              ?? GetEntityAnnotation<bool?>(entityType, ClickHouseAnnotationNames.MaterializedView)
                              ?? false;

        if (isMaterializedView)
        {
            GenerateMaterializedView(operation, entityType, model, builder, terminate);
            return;
        }

        var isDictionary = GetAnnotation<bool?>(operation, ClickHouseAnnotationNames.Dictionary)
                        ?? GetEntityAnnotation<bool?>(entityType, ClickHouseAnnotationNames.Dictionary)
                        ?? false;

        if (isDictionary)
        {
            GenerateDictionary(operation, entityType, model, builder, terminate);
            return;
        }

        builder
            .Append("CREATE TABLE IF NOT EXISTS ")
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

            // Generate ADD PROJECTION statements after table creation
            GenerateProjections(operation, entityType, builder);
        }
    }

    /// <summary>
    /// Generates ALTER TABLE ADD PROJECTION statements for entity projections.
    /// </summary>
    private void GenerateProjections(
        CreateTableOperation operation,
        IEntityType? entityType,
        MigrationCommandListBuilder builder)
    {
        var projections = GetEntityAnnotation<List<Projections.ProjectionDefinition>>(
            entityType, ClickHouseAnnotationNames.Projections);

        if (projections == null || projections.Count == 0)
            return;

        var tableName = Dependencies.SqlGenerationHelper.DelimitIdentifier(
            operation.Name, operation.Schema);

        foreach (var projection in projections)
        {
            // ALTER TABLE "table" ADD PROJECTION IF NOT EXISTS "name" (SELECT ...)
            builder.Append("ALTER TABLE ");
            builder.Append(tableName);
            builder.Append(" ADD PROJECTION IF NOT EXISTS ");
            builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(projection.Name));
            builder.Append(" (");
            builder.Append(projection.SelectSql);
            builder.Append(")");
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);

            // Optionally materialize existing data
            if (projection.Materialize)
            {
                builder.Append("ALTER TABLE ");
                builder.Append(tableName);
                builder.Append(" MATERIALIZE PROJECTION ");
                builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(projection.Name));
                builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
                EndStatement(builder);
            }
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
            .Append("CREATE MATERIALIZED VIEW IF NOT EXISTS ")
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
    /// Generates CREATE DICTIONARY statement.
    /// </summary>
    private void GenerateDictionary(
        CreateTableOperation operation,
        IEntityType? entityType,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate)
    {
        // Check if this is an external dictionary (PostgreSQL, MySQL, HTTP)
        // External dictionaries are skipped in migrations because they contain credentials
        // They should be created at runtime via context.EnsureDictionariesAsync()
        var sourceProvider = GetAnnotation<string>(operation, ClickHouseAnnotationNames.DictionarySourceProvider)
                          ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.DictionarySourceProvider)
                          ?? "clickhouse";

        if (sourceProvider != "clickhouse")
        {
            // Emit comment explaining that this dictionary is created at runtime
            builder.AppendLine($"-- Dictionary '{operation.Name}' uses external source '{sourceProvider}'");
            builder.AppendLine($"-- This dictionary is NOT created by migrations because it contains credentials.");
            builder.AppendLine($"-- Create it at runtime using: await context.EnsureDictionariesAsync();");
            builder.AppendLine();

            if (terminate)
            {
                EndStatement(builder);
            }
            return;
        }

        // Get dictionary configuration from annotations
        var sourceTable = GetAnnotation<string>(operation, ClickHouseAnnotationNames.DictionarySource)
                       ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.DictionarySource);
        var keyColumns = GetAnnotation<string[]>(operation, ClickHouseAnnotationNames.DictionaryKeyColumns)
                      ?? GetEntityAnnotation<string[]>(entityType, ClickHouseAnnotationNames.DictionaryKeyColumns);
        var layout = GetAnnotation<DictionaryLayout?>(operation, ClickHouseAnnotationNames.DictionaryLayout)
                  ?? GetEntityAnnotation<DictionaryLayout?>(entityType, ClickHouseAnnotationNames.DictionaryLayout)
                  ?? DictionaryLayout.Hashed;
        var layoutOptions = GetAnnotation<Dictionary<string, object>>(operation, ClickHouseAnnotationNames.DictionaryLayoutOptions)
                         ?? GetEntityAnnotation<Dictionary<string, object>>(entityType, ClickHouseAnnotationNames.DictionaryLayoutOptions);
        var lifetimeMin = GetAnnotation<int?>(operation, ClickHouseAnnotationNames.DictionaryLifetimeMin)
                       ?? GetEntityAnnotation<int?>(entityType, ClickHouseAnnotationNames.DictionaryLifetimeMin)
                       ?? 0;
        var lifetimeMax = GetAnnotation<int?>(operation, ClickHouseAnnotationNames.DictionaryLifetimeMax)
                       ?? GetEntityAnnotation<int?>(entityType, ClickHouseAnnotationNames.DictionaryLifetimeMax)
                       ?? 300;
        var defaults = GetAnnotation<Dictionary<string, object>>(operation, ClickHouseAnnotationNames.DictionaryDefaults)
                    ?? GetEntityAnnotation<Dictionary<string, object>>(entityType, ClickHouseAnnotationNames.DictionaryDefaults);
        var sourceQuery = GetAnnotation<string>(operation, ClickHouseAnnotationNames.DictionarySourceQuery)
                       ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.DictionarySourceQuery);

        if (keyColumns == null || keyColumns.Length == 0)
        {
            throw new InvalidOperationException(
                $"Dictionary '{operation.Name}' must have key columns defined.");
        }

        // Build SOURCE query if projection/filter expressions exist
        if (string.IsNullOrEmpty(sourceQuery) && entityType != null)
        {
            sourceQuery = BuildDictionarySourceQuery(operation, entityType, model);
        }

        // CREATE DICTIONARY
        builder.Append("CREATE DICTIONARY IF NOT EXISTS ");
        builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema));
        builder.AppendLine();
        builder.AppendLine("(");

        // Column definitions
        using (builder.Indent())
        {
            var isFirst = true;
            foreach (var column in operation.Columns)
            {
                if (!isFirst)
                {
                    builder.AppendLine(",");
                }
                isFirst = false;

                // Column name and type
                builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(column.Name));
                builder.Append(" ");
                builder.Append(column.ColumnType ?? GetClickHouseType(column.ClrType));

                // DEFAULT value for this column
                if (defaults?.TryGetValue(column.Name, out var defaultValue) == true)
                {
                    builder.Append(" DEFAULT ");
                    builder.Append(FormatDefaultValue(defaultValue));
                }
            }
        }

        builder.AppendLine();
        builder.AppendLine(")");

        // PRIMARY KEY
        builder.Append("PRIMARY KEY ");
        if (keyColumns.Length == 1)
        {
            builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(keyColumns[0]));
        }
        else
        {
            builder.Append("(");
            builder.Append(string.Join(", ", keyColumns.Select(c => Dependencies.SqlGenerationHelper.DelimitIdentifier(c))));
            builder.Append(")");
        }
        builder.AppendLine();

        // SOURCE
        builder.Append("SOURCE(CLICKHOUSE(");
        if (!string.IsNullOrEmpty(sourceQuery))
        {
            // Use custom query
            builder.Append("QUERY '");
            builder.Append(sourceQuery.Replace("'", "''"));
            builder.Append("'");
        }
        else
        {
            // Use table reference
            builder.Append("TABLE '");
            builder.Append(sourceTable ?? operation.Name);
            builder.Append("'");
        }
        builder.AppendLine("))");

        // LAYOUT
        builder.Append("LAYOUT(");
        builder.Append(GetLayoutSql(layout, layoutOptions));
        builder.AppendLine(")");

        // LIFETIME
        if (lifetimeMin == 0 && lifetimeMax == 0)
        {
            builder.AppendLine("LIFETIME(0)");
        }
        else if (lifetimeMin == lifetimeMax || lifetimeMin == 0)
        {
            builder.Append("LIFETIME(");
            builder.Append(lifetimeMax.ToString());
            builder.AppendLine(")");
        }
        else
        {
            builder.Append("LIFETIME(MIN ");
            builder.Append(lifetimeMin.ToString());
            builder.Append(" MAX ");
            builder.Append(lifetimeMax.ToString());
            builder.AppendLine(")");
        }

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>
    /// Builds the SOURCE query for a dictionary from projection/filter expressions.
    /// </summary>
    private string? BuildDictionarySourceQuery(
        CreateTableOperation operation,
        IEntityType entityType,
        IModel? model)
    {
        var projectionExpr = GetEntityAnnotation<LambdaExpression>(entityType, ClickHouseAnnotationNames.DictionaryProjectionExpression);
        var filterExpr = GetEntityAnnotation<LambdaExpression>(entityType, ClickHouseAnnotationNames.DictionaryFilterExpression);

        if (projectionExpr == null && filterExpr == null)
            return null;

        var sourceTable = GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.DictionarySource);
        if (string.IsNullOrEmpty(sourceTable))
            return null;

        var columns = new List<string>();

        // If projection exists, extract column mappings
        if (projectionExpr != null)
        {
            columns = ExtractProjectionColumns(projectionExpr, operation.Columns);
        }
        else
        {
            // Use all columns from the operation
            columns = operation.Columns.Select(c => Dependencies.SqlGenerationHelper.DelimitIdentifier(c.Name)).ToList();
        }

        var query = $"SELECT {string.Join(", ", columns)} FROM {Dependencies.SqlGenerationHelper.DelimitIdentifier(sourceTable)}";

        // Add WHERE clause from filter expression
        if (filterExpr != null)
        {
            var whereClause = ExtractFilterWhereClause(filterExpr);
            if (!string.IsNullOrEmpty(whereClause))
            {
                query += $" WHERE {whereClause}";
            }
        }

        return query;
    }

    /// <summary>
    /// Extracts column selections from a projection expression.
    /// </summary>
    private List<string> ExtractProjectionColumns(LambdaExpression projection, IReadOnlyList<AddColumnOperation> targetColumns)
    {
        var columns = new List<string>();

        // For now, we'll use a simple approach: match target column names
        // In a more complete implementation, we'd parse the expression tree to get source->target mappings
        foreach (var col in targetColumns)
        {
            columns.Add(Dependencies.SqlGenerationHelper.DelimitIdentifier(col.Name));
        }

        return columns;
    }

    /// <summary>
    /// Extracts a WHERE clause from a filter expression.
    /// </summary>
    private static string? ExtractFilterWhereClause(LambdaExpression filter)
    {
        // This is a simplified implementation
        // A complete implementation would parse the expression tree and convert to SQL
        // For now, we support simple Where() calls with basic predicates

        if (filter.Body is MethodCallExpression methodCall
            && methodCall.Method.Name == "Where"
            && methodCall.Arguments.Count >= 2
            && methodCall.Arguments[1] is UnaryExpression { Operand: LambdaExpression predicate })
        {
            return ConvertPredicateToSql(predicate);
        }

        return null;
    }

    /// <summary>
    /// Converts a simple predicate expression to SQL.
    /// </summary>
    private static string? ConvertPredicateToSql(LambdaExpression predicate)
    {
        if (predicate.Body is BinaryExpression binary)
        {
            var left = GetExpressionSql(binary.Left);
            var right = GetExpressionSql(binary.Right);
            var op = binary.NodeType switch
            {
                ExpressionType.Equal => "=",
                ExpressionType.NotEqual => "!=",
                ExpressionType.GreaterThan => ">",
                ExpressionType.GreaterThanOrEqual => ">=",
                ExpressionType.LessThan => "<",
                ExpressionType.LessThanOrEqual => "<=",
                ExpressionType.AndAlso => "AND",
                ExpressionType.OrElse => "OR",
                _ => null
            };

            if (op != null && left != null && right != null)
            {
                return $"{left} {op} {right}";
            }
        }
        else if (predicate.Body is MemberExpression member && member.Type == typeof(bool))
        {
            // Simple boolean property access like x => x.IsActive
            return $"\"{member.Member.Name}\" = 1";
        }
        else if (predicate.Body is UnaryExpression { NodeType: ExpressionType.Not } unary
                 && unary.Operand is MemberExpression notMember && notMember.Type == typeof(bool))
        {
            // Negated boolean like x => !x.IsActive
            return $"\"{notMember.Member.Name}\" = 0";
        }

        return null;
    }

    /// <summary>
    /// Gets SQL representation of an expression part.
    /// </summary>
    private static string? GetExpressionSql(Expression expr)
    {
        return expr switch
        {
            MemberExpression member => $"\"{member.Member.Name}\"",
            ConstantExpression constant => FormatConstantValue(constant.Value),
            UnaryExpression { NodeType: ExpressionType.Convert } unary => GetExpressionSql(unary.Operand),
            _ => null
        };
    }

    /// <summary>
    /// Formats a constant value for SQL.
    /// </summary>
    private static string? FormatConstantValue(object? value)
    {
        return value switch
        {
            null => "NULL",
            string s => $"'{s.Replace("'", "''")}'",
            bool b => b ? "1" : "0",
            DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss}'",
            _ => value.ToString()
        };
    }

    /// <summary>
    /// Gets the SQL for a dictionary layout.
    /// </summary>
    private static string GetLayoutSql(DictionaryLayout layout, Dictionary<string, object>? options)
    {
        var layoutName = layout switch
        {
            DictionaryLayout.Flat => "FLAT",
            DictionaryLayout.Hashed => "HASHED",
            DictionaryLayout.HashedArray => "HASHED_ARRAY",
            DictionaryLayout.ComplexKeyHashed => "COMPLEX_KEY_HASHED",
            DictionaryLayout.ComplexKeyHashedArray => "COMPLEX_KEY_HASHED_ARRAY",
            DictionaryLayout.RangeHashed => "RANGE_HASHED",
            DictionaryLayout.Cache => "CACHE",
            DictionaryLayout.Direct => "DIRECT",
            _ => "HASHED"
        };

        if (options == null || options.Count == 0)
        {
            return $"{layoutName}()";
        }

        var optionStrings = options.Select(kvp =>
        {
            var value = kvp.Value switch
            {
                bool b => b ? "1" : "0",
                _ => kvp.Value.ToString()
            };
            return $"{kvp.Key.ToUpperInvariant()} {value}";
        });

        return $"{layoutName}({string.Join(" ", optionStrings)})";
    }

    /// <summary>
    /// Gets the ClickHouse type for a CLR type.
    /// </summary>
    private string GetClickHouseType(Type clrType)
    {
        var mapping = _typeMappingSource.FindMapping(clrType);
        return mapping?.StoreType ?? "String";
    }

    /// <summary>
    /// Formats a default value for DDL.
    /// </summary>
    private static string FormatDefaultValue(object value)
    {
        return value switch
        {
            string s => $"'{s.Replace("'", "''")}'",
            bool b => b ? "1" : "0",
            DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss}'",
            _ => value.ToString() ?? "NULL"
        };
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
        var isDeletedColumn = GetAnnotation<string>(operation, ClickHouseAnnotationNames.IsDeletedColumn)
                           ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.IsDeletedColumn);
        var signColumn = GetAnnotation<string>(operation, ClickHouseAnnotationNames.SignColumn)
                      ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.SignColumn);
        var settings = GetAnnotation<IDictionary<string, string>>(operation, ClickHouseAnnotationNames.Settings)
                    ?? GetEntityAnnotation<IDictionary<string, string>>(entityType, ClickHouseAnnotationNames.Settings);

        builder.Append("ENGINE = ");

        // Generate engine with parameters
        switch (engine)
        {
            case "ReplacingMergeTree" when !string.IsNullOrEmpty(versionColumn) && !string.IsNullOrEmpty(isDeletedColumn):
                // ReplacingMergeTree(version, is_deleted) - ClickHouse 23.2+
                builder.Append($"ReplacingMergeTree({Dependencies.SqlGenerationHelper.DelimitIdentifier(versionColumn)}, {Dependencies.SqlGenerationHelper.DelimitIdentifier(isDeletedColumn)})");
                break;
            case "ReplacingMergeTree" when !string.IsNullOrEmpty(versionColumn):
                builder.Append($"ReplacingMergeTree({Dependencies.SqlGenerationHelper.DelimitIdentifier(versionColumn)})");
                break;
            case "CollapsingMergeTree" when !string.IsNullOrEmpty(signColumn):
                builder.Append($"CollapsingMergeTree({Dependencies.SqlGenerationHelper.DelimitIdentifier(signColumn)})");
                break;
            case "VersionedCollapsingMergeTree" when !string.IsNullOrEmpty(signColumn) && !string.IsNullOrEmpty(versionColumn):
                builder.Append($"VersionedCollapsingMergeTree({Dependencies.SqlGenerationHelper.DelimitIdentifier(signColumn)}, {Dependencies.SqlGenerationHelper.DelimitIdentifier(versionColumn)})");
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
        var isDeletedColumn = GetAnnotation<string>(operation, ClickHouseAnnotationNames.IsDeletedColumn)
                           ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.IsDeletedColumn);
        var signColumn = GetAnnotation<string>(operation, ClickHouseAnnotationNames.SignColumn)
                      ?? GetEntityAnnotation<string>(entityType, ClickHouseAnnotationNames.SignColumn);
        var settings = GetAnnotation<IDictionary<string, string>>(operation, ClickHouseAnnotationNames.Settings)
                    ?? GetEntityAnnotation<IDictionary<string, string>>(entityType, ClickHouseAnnotationNames.Settings);

        builder.AppendLine();
        builder.Append("ENGINE = ");

        // Generate engine with parameters
        switch (engine)
        {
            case "ReplacingMergeTree" when !string.IsNullOrEmpty(versionColumn) && !string.IsNullOrEmpty(isDeletedColumn):
                // ReplacingMergeTree(version, is_deleted) - ClickHouse 23.2+
                builder.Append($"ReplacingMergeTree({Dependencies.SqlGenerationHelper.DelimitIdentifier(versionColumn)}, {Dependencies.SqlGenerationHelper.DelimitIdentifier(isDeletedColumn)})");
                break;
            case "ReplacingMergeTree" when !string.IsNullOrEmpty(versionColumn):
                builder.Append($"ReplacingMergeTree({Dependencies.SqlGenerationHelper.DelimitIdentifier(versionColumn)})");
                break;
            case "CollapsingMergeTree" when !string.IsNullOrEmpty(signColumn):
                builder.Append($"CollapsingMergeTree({Dependencies.SqlGenerationHelper.DelimitIdentifier(signColumn)})");
                break;
            case "VersionedCollapsingMergeTree" when !string.IsNullOrEmpty(signColumn) && !string.IsNullOrEmpty(versionColumn):
                builder.Append($"VersionedCollapsingMergeTree({Dependencies.SqlGenerationHelper.DelimitIdentifier(signColumn)}, {Dependencies.SqlGenerationHelper.DelimitIdentifier(versionColumn)})");
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
            .Append(" ADD COLUMN IF NOT EXISTS ");

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
            .Append(" DROP COLUMN IF EXISTS ")
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

        // Check for identity column annotations from other providers
        if (IsIdentityColumnAttempt(operation))
        {
            throw ClickHouseUnsupportedOperationException.Identity(operation.Table, operation.Name);
        }

        builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name));
        builder.Append(" ");

        var columnType = operation.ColumnType
            ?? GetColumnType(operation.Schema, operation.Table, operation.Name, operation, model)
            ?? "String"; // Default to String if type cannot be determined

        // Check for JSON type parameters (max_dynamic_paths, max_dynamic_types)
        if (string.Equals(columnType, "JSON", StringComparison.OrdinalIgnoreCase))
        {
            var (maxPaths, maxTypes) = GetJsonParameters(model, operation.Schema, operation.Table, operation.Name);
            if (maxPaths.HasValue || maxTypes.HasValue)
            {
                columnType = BuildJsonType(maxPaths, maxTypes);
            }
        }

        // Check for default-for-null annotation
        var defaultForNull = GetPropertyDefaultForNull(model, operation.Schema, operation.Table, operation.Name);

        // Wrap nullable columns with Nullable() - unless using default-for-null
        // Also skip if type already contains Nullable() (e.g., LowCardinality(Nullable(String)))
        // Skip JSON types - ClickHouse doesn't support Nullable(JSON), JSON handles nulls internally
        if (operation.IsNullable && defaultForNull == null &&
            !columnType.StartsWith("Nullable(", StringComparison.OrdinalIgnoreCase) &&
            !columnType.Contains("Nullable(", StringComparison.OrdinalIgnoreCase) &&
            !columnType.StartsWith("JSON", StringComparison.OrdinalIgnoreCase))
        {
            builder.Append($"Nullable({columnType})");
        }
        else
        {
            builder.Append(columnType);
        }

        // Check for MATERIALIZED / ALIAS / DEFAULT expression (mutually exclusive)
        // These take precedence over operation.DefaultValue and operation.DefaultValueSql
        var materializedExpr = GetPropertyMaterializedExpression(model, operation.Schema, operation.Table, operation.Name);
        var aliasExpr = GetPropertyAliasExpression(model, operation.Schema, operation.Table, operation.Name);
        var defaultExpr = GetPropertyDefaultExpression(model, operation.Schema, operation.Table, operation.Name);

        if (!string.IsNullOrEmpty(materializedExpr))
        {
            builder.Append(" MATERIALIZED ");
            builder.Append(materializedExpr);
        }
        else if (!string.IsNullOrEmpty(aliasExpr))
        {
            builder.Append(" ALIAS ");
            builder.Append(aliasExpr);
        }
        else if (!string.IsNullOrEmpty(defaultExpr))
        {
            builder.Append(" DEFAULT ");
            builder.Append(defaultExpr);
        }
        else if (defaultForNull != null)
        {
            // Default-for-null takes precedence over operation defaults
            var typeMapping = _typeMappingSource.FindMapping(defaultForNull.GetType());
            builder.Append(" DEFAULT ");
            builder.Append(typeMapping?.GenerateSqlLiteral(defaultForNull)
                ?? defaultForNull.ToString() ?? "0");
        }
        else if (operation.DefaultValue != null)
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

        // Compression codec - applied after MATERIALIZED/ALIAS/DEFAULT expressions
        var codecSpec = GetPropertyCodec(model, operation.Schema, operation.Table, operation.Name);
        if (!string.IsNullOrEmpty(codecSpec))
        {
            builder.Append(" CODEC(");
            builder.Append(codecSpec);
            builder.Append(")");
        }
    }

    /// <summary>
    /// Gets the default-for-null value for a property, if configured.
    /// </summary>
    private static object? GetPropertyDefaultForNull(IModel? model, string? schema, string table, string columnName)
    {
        if (model == null)
            return null;

        // Find the entity type by table name
        var entityType = model.GetEntityTypes()
            .FirstOrDefault(e => e.GetTableName() == table
                              && (e.GetSchema() ?? model.GetDefaultSchema()) == schema);

        if (entityType == null)
            return null;

        // Find the property by column name
        var property = entityType.GetProperties()
            .FirstOrDefault(p => (p.GetColumnName() ?? p.Name) == columnName);

        if (property == null)
            return null;

        // Return the default-for-null value if configured
        return property.FindAnnotation(ClickHouseAnnotationNames.DefaultForNull)?.Value;
    }

    /// <summary>
    /// Gets the compression codec for a property, if configured.
    /// </summary>
    private static string? GetPropertyCodec(IModel? model, string? schema, string table, string columnName)
    {
        if (model == null)
            return null;

        // Find the entity type by table name
        var entityType = model.GetEntityTypes()
            .FirstOrDefault(e => e.GetTableName() == table
                              && (e.GetSchema() ?? model.GetDefaultSchema()) == schema);

        if (entityType == null)
            return null;

        // Find the property by column name
        var property = entityType.GetProperties()
            .FirstOrDefault(p => (p.GetColumnName() ?? p.Name) == columnName);

        if (property == null)
            return null;

        // Return the codec specification if configured
        return property.FindAnnotation(ClickHouseAnnotationNames.CompressionCodec)?.Value as string;
    }

    /// <summary>
    /// Gets JSON type parameters (max_dynamic_paths, max_dynamic_types) for a property, if configured.
    /// </summary>
    private static (int? maxDynamicPaths, int? maxDynamicTypes) GetJsonParameters(IModel? model, string? schema, string table, string columnName)
    {
        if (model == null)
            return (null, null);

        // Find the entity type by table name
        var entityType = model.GetEntityTypes()
            .FirstOrDefault(e => e.GetTableName() == table
                              && (e.GetSchema() ?? model.GetDefaultSchema()) == schema);

        if (entityType == null)
            return (null, null);

        // Find the property by column name
        var property = entityType.GetProperties()
            .FirstOrDefault(p => (p.GetColumnName() ?? p.Name) == columnName);

        if (property == null)
            return (null, null);

        // Get JSON parameters from annotations
        var maxPaths = property.FindAnnotation(ClickHouseAnnotationNames.JsonMaxDynamicPaths)?.Value as int?;
        var maxTypes = property.FindAnnotation(ClickHouseAnnotationNames.JsonMaxDynamicTypes)?.Value as int?;

        return (maxPaths, maxTypes);
    }

    /// <summary>
    /// Builds the JSON type string with optional parameters.
    /// </summary>
    private static string BuildJsonType(int? maxDynamicPaths, int? maxDynamicTypes)
    {
        if (maxDynamicPaths == null && maxDynamicTypes == null)
            return "JSON";

        var parameters = new List<string>();
        if (maxDynamicPaths.HasValue)
            parameters.Add($"max_dynamic_paths={maxDynamicPaths.Value}");
        if (maxDynamicTypes.HasValue)
            parameters.Add($"max_dynamic_types={maxDynamicTypes.Value}");

        return $"JSON({string.Join(", ", parameters)})";
    }

    /// <summary>
    /// Gets the MATERIALIZED expression for a property, if configured.
    /// </summary>
    private static string? GetPropertyMaterializedExpression(IModel? model, string? schema, string table, string columnName)
    {
        if (model == null)
            return null;

        // Find the entity type by table name
        var entityType = model.GetEntityTypes()
            .FirstOrDefault(e => e.GetTableName() == table
                              && (e.GetSchema() ?? model.GetDefaultSchema()) == schema);

        if (entityType == null)
            return null;

        // Find the property by column name
        var property = entityType.GetProperties()
            .FirstOrDefault(p => (p.GetColumnName() ?? p.Name) == columnName);

        if (property == null)
            return null;

        // Return the MATERIALIZED expression if configured
        return property.FindAnnotation(ClickHouseAnnotationNames.MaterializedExpression)?.Value as string;
    }

    /// <summary>
    /// Gets the ALIAS expression for a property, if configured.
    /// </summary>
    private static string? GetPropertyAliasExpression(IModel? model, string? schema, string table, string columnName)
    {
        if (model == null)
            return null;

        // Find the entity type by table name
        var entityType = model.GetEntityTypes()
            .FirstOrDefault(e => e.GetTableName() == table
                              && (e.GetSchema() ?? model.GetDefaultSchema()) == schema);

        if (entityType == null)
            return null;

        // Find the property by column name
        var property = entityType.GetProperties()
            .FirstOrDefault(p => (p.GetColumnName() ?? p.Name) == columnName);

        if (property == null)
            return null;

        // Return the ALIAS expression if configured
        return property.FindAnnotation(ClickHouseAnnotationNames.AliasExpression)?.Value as string;
    }

    /// <summary>
    /// Gets the DEFAULT expression for a property, if configured.
    /// </summary>
    private static string? GetPropertyDefaultExpression(IModel? model, string? schema, string table, string columnName)
    {
        if (model == null)
            return null;

        // Find the entity type by table name
        var entityType = model.GetEntityTypes()
            .FirstOrDefault(e => e.GetTableName() == table
                              && (e.GetSchema() ?? model.GetDefaultSchema()) == schema);

        if (entityType == null)
            return null;

        // Find the property by column name
        var property = entityType.GetProperties()
            .FirstOrDefault(p => (p.GetColumnName() ?? p.Name) == columnName);

        if (property == null)
            return null;

        // Return the DEFAULT expression if configured
        return property.FindAnnotation(ClickHouseAnnotationNames.DefaultExpression)?.Value as string;
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
        throw ClickHouseUnsupportedOperationException.AddForeignKey(operation.Name, operation.Table);
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
        throw ClickHouseUnsupportedOperationException.DropForeignKey(operation.Name, operation.Table);
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
        throw ClickHouseUnsupportedOperationException.AddPrimaryKey(operation.Table);
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
        throw ClickHouseUnsupportedOperationException.DropPrimaryKey(operation.Table);
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
        throw ClickHouseUnsupportedOperationException.RenameColumn(operation.Table, operation.Name);
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

        // Check for JSON type parameters (max_dynamic_paths, max_dynamic_types)
        if (string.Equals(columnType, "JSON", StringComparison.OrdinalIgnoreCase))
        {
            var (maxPaths, maxTypes) = GetJsonParameters(model, operation.Schema, operation.Table, operation.Name);
            if (maxPaths.HasValue || maxTypes.HasValue)
            {
                columnType = BuildJsonType(maxPaths, maxTypes);
            }
        }

        // Check for default-for-null annotation
        var defaultForNull = GetPropertyDefaultForNull(model, operation.Schema, operation.Table, operation.Name);

        // Wrap nullable columns with Nullable() - unless using default-for-null
        // Also skip if type already contains Nullable() (e.g., LowCardinality(Nullable(String)))
        // Skip JSON types - ClickHouse doesn't support Nullable(JSON), JSON handles nulls internally
        if (operation.IsNullable && defaultForNull == null &&
            !columnType.StartsWith("Nullable(", StringComparison.OrdinalIgnoreCase) &&
            !columnType.Contains("Nullable(", StringComparison.OrdinalIgnoreCase) &&
            !columnType.StartsWith("JSON", StringComparison.OrdinalIgnoreCase))
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

        // Compression codec - applied after type, before default
        var codecSpec = GetPropertyCodec(model, operation.Schema, operation.Table, operation.Name);
        if (!string.IsNullOrEmpty(codecSpec))
        {
            builder.Append(" CODEC(");
            builder.Append(codecSpec);
            builder.Append(")");
        }

        // Default value - default-for-null takes precedence
        if (defaultForNull != null)
        {
            var typeMapping = _typeMappingSource.FindMapping(defaultForNull.GetType());
            builder.Append(" DEFAULT ");
            builder.Append(typeMapping?.GenerateSqlLiteral(defaultForNull)
                ?? defaultForNull.ToString() ?? "0");
        }
        else if (operation.DefaultValue != null)
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
            throw ClickHouseUnsupportedOperationException.UniqueIndex(operation.Name, operation.Table);
        }

        // Get skip index configuration from model annotations
        var indexType = GetSkipIndexType(operation, model);
        var granularity = GetSkipIndexGranularity(operation, model);
        var indexParams = GetSkipIndexParams(operation, model);
        var typeSpec = BuildTypeSpecification(indexType, indexParams);

        // ClickHouse uses ALTER TABLE ADD INDEX syntax for data skipping indexes
        var tableName = Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema);
        var indexName = Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name);

        builder
            .Append("ALTER TABLE ")
            .Append(tableName)
            .Append(" ADD INDEX IF NOT EXISTS ")
            .Append(indexName)
            .Append(" (");

        builder.Append(string.Join(", ",
            operation.Columns.Select(c => Dependencies.SqlGenerationHelper.DelimitIdentifier(c))));

        builder.Append(") ");
        builder.Append(typeSpec);
        builder.Append(" GRANULARITY ");
        builder.Append(granularity.ToString());

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>
    /// Gets the skip index type from the model or defaults to Minmax.
    /// </summary>
    private static SkipIndexType GetSkipIndexType(CreateIndexOperation operation, IModel? model)
    {
        // Check operation annotations first (from migration)
        if (operation.FindAnnotation(ClickHouseAnnotationNames.SkipIndexType)?.Value is SkipIndexType operationType)
            return operationType;

        // Then check model annotations
        var index = FindIndexInModel(model, operation.Table, operation.Name);
        if (index?.FindAnnotation(ClickHouseAnnotationNames.SkipIndexType)?.Value is SkipIndexType modelType)
            return modelType;

        return SkipIndexType.Minmax;
    }

    /// <summary>
    /// Gets the skip index granularity from the model or defaults to 3.
    /// </summary>
    private static int GetSkipIndexGranularity(CreateIndexOperation operation, IModel? model)
    {
        // Check operation annotations first (from migration)
        if (operation.FindAnnotation(ClickHouseAnnotationNames.SkipIndexGranularity)?.Value is int operationGranularity)
            return operationGranularity;

        // Then check model annotations
        var index = FindIndexInModel(model, operation.Table, operation.Name);
        if (index?.FindAnnotation(ClickHouseAnnotationNames.SkipIndexGranularity)?.Value is int modelGranularity)
            return modelGranularity;

        return 3;
    }

    /// <summary>
    /// Gets the skip index parameters from the model.
    /// </summary>
    private static SkipIndexParams? GetSkipIndexParams(CreateIndexOperation operation, IModel? model)
    {
        // Check operation annotations first (from migration)
        if (operation.FindAnnotation(ClickHouseAnnotationNames.SkipIndexParams)?.Value is SkipIndexParams operationParams)
            return operationParams;

        // Then check model annotations
        var index = FindIndexInModel(model, operation.Table, operation.Name);
        return index?.FindAnnotation(ClickHouseAnnotationNames.SkipIndexParams)?.Value as SkipIndexParams;
    }

    /// <summary>
    /// Finds an index in the model by table name and index name.
    /// </summary>
    private static IIndex? FindIndexInModel(IModel? model, string? tableName, string? indexName)
    {
        if (model == null || tableName == null || indexName == null)
            return null;

        foreach (var entityType in model.GetEntityTypes())
        {
            if (entityType.GetTableName() != tableName)
                continue;

            foreach (var index in entityType.GetIndexes())
            {
                if (index.GetDatabaseName() == indexName)
                    return index;
            }
        }

        return null;
    }

    /// <summary>
    /// Builds the TYPE specification for skip index DDL.
    /// </summary>
    private static string BuildTypeSpecification(SkipIndexType indexType, SkipIndexParams? indexParams)
    {
        var paramsOrDefault = indexParams ?? new SkipIndexParams();
        return paramsOrDefault.BuildTypeSpecification(indexType);
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
            .Append(" DROP INDEX IF EXISTS ")
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
    /// Routes custom migration operations to their handlers.
    /// </summary>
    protected override void Generate(
        MigrationOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        switch (operation)
        {
            case AddProjectionOperation addProjection:
                Generate(addProjection, model, builder);
                break;
            case DropProjectionOperation dropProjection:
                Generate(dropProjection, model, builder);
                break;
            case MaterializeProjectionOperation materializeProjection:
                Generate(materializeProjection, model, builder);
                break;
            default:
                base.Generate(operation, model, builder);
                break;
        }
    }

    #region Projection Operations

    /// <summary>
    /// Generates SQL for AddProjectionOperation.
    /// </summary>
    protected virtual void Generate(
        AddProjectionOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(builder);

        var tableName = Dependencies.SqlGenerationHelper.DelimitIdentifier(
            operation.Table, operation.Schema);
        var projectionName = Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name);

        builder.Append("ALTER TABLE ");
        builder.Append(tableName);
        builder.Append(" ADD PROJECTION IF NOT EXISTS ");
        builder.Append(projectionName);
        builder.Append(" (");
        builder.Append(operation.SelectSql);
        builder.Append(")");
        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder);

        // Materialize if requested
        if (operation.Materialize)
        {
            builder.Append("ALTER TABLE ");
            builder.Append(tableName);
            builder.Append(" MATERIALIZE PROJECTION ");
            builder.Append(projectionName);
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }
    }

    /// <summary>
    /// Generates SQL for DropProjectionOperation.
    /// </summary>
    protected virtual void Generate(
        DropProjectionOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(builder);

        var tableName = Dependencies.SqlGenerationHelper.DelimitIdentifier(
            operation.Table, operation.Schema);
        var projectionName = Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name);

        builder.Append("ALTER TABLE ");
        builder.Append(tableName);
        builder.Append(" DROP PROJECTION ");
        if (operation.IfExists)
        {
            builder.Append("IF EXISTS ");
        }
        builder.Append(projectionName);
        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder);
    }

    /// <summary>
    /// Generates SQL for MaterializeProjectionOperation.
    /// </summary>
    protected virtual void Generate(
        MaterializeProjectionOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(builder);

        var tableName = Dependencies.SqlGenerationHelper.DelimitIdentifier(
            operation.Table, operation.Schema);
        var projectionName = Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name);

        builder.Append("ALTER TABLE ");
        builder.Append(tableName);
        builder.Append(" MATERIALIZE PROJECTION ");
        builder.Append(projectionName);

        if (!string.IsNullOrEmpty(operation.InPartition))
        {
            builder.Append(" IN PARTITION ");
            builder.Append(operation.InPartition);
        }

        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatement(builder);
    }

    #endregion

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

    /// <summary>
    /// Checks if an AddColumnOperation contains identity/auto-increment annotations
    /// from SQL Server, PostgreSQL, or other providers.
    /// </summary>
    private static bool IsIdentityColumnAttempt(AddColumnOperation operation)
    {
        // Check for SQL Server identity annotation (e.g., "SqlServer:Identity")
        foreach (var annotation in operation.GetAnnotations())
        {
            var name = annotation.Name;
            if (name.Contains("Identity", StringComparison.OrdinalIgnoreCase))
                return true;

            // Check for Npgsql serial/identity value generation strategy
            if (name.Contains("ValueGenerationStrategy", StringComparison.OrdinalIgnoreCase))
            {
                var value = annotation.Value?.ToString();
                if (value != null && (
                    value.Contains("Identity", StringComparison.OrdinalIgnoreCase) ||
                    value.Contains("Serial", StringComparison.OrdinalIgnoreCase)))
                {
                    return true;
                }
            }
        }

        return false;
    }
}
