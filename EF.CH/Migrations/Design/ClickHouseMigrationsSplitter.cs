using EF.CH.Metadata;
using EF.CH.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EF.CH.Migrations.Design;

/// <summary>
/// Record representing a single step in a split migration.
/// </summary>
public sealed record StepMigration(
    int StepNumber,
    string OperationDescription,
    MigrationOperation Operation,
    int OriginalIndex)
{
    /// <summary>
    /// Gets the step suffix formatted as a 3-digit string (e.g., "001", "002").
    /// </summary>
    public string StepSuffix => StepNumber.ToString("D3");
}

/// <summary>
/// Splits migration operations into individual step migrations for ClickHouse.
/// Uses topological sorting to ensure dependencies are respected.
/// </summary>
public class ClickHouseMigrationsSplitter
{
    /// <summary>
    /// Splits operations into individual step migrations, sorted by dependencies.
    /// </summary>
    /// <param name="operations">The migration operations to split.</param>
    /// <param name="model">The optional model for additional context.</param>
    /// <returns>A list of step migrations, one per operation.</returns>
    public IReadOnlyList<StepMigration> Split(
        IReadOnlyList<MigrationOperation> operations,
        IModel? model = null)
    {
        if (operations.Count == 0)
            return [];

        // Sort operations by dependencies
        var sortedOperations = SortOperationsByDependencies(operations, model);

        // Create step migrations
        var steps = new List<StepMigration>(sortedOperations.Count);
        for (int i = 0; i < sortedOperations.Count; i++)
        {
            var (operation, originalIndex) = sortedOperations[i];
            var description = GetOperationDescription(operation);
            steps.Add(new StepMigration(i + 1, description, operation, originalIndex));
        }

        return steps;
    }

    /// <summary>
    /// Sorts operations using phase-based ordering to ensure robust dependency handling.
    /// This approach doesn't rely on annotation detection which can fail for LINQ-based MVs.
    /// </summary>
    /// <remarks>
    /// Phase order:
    /// 1. Drop projections/indexes (remove dependents first)
    /// 2. Drop tables (can't reliably detect MV vs regular table)
    /// 3. Create regular tables (base tables first)
    /// 4. Create MVs and dictionaries (source tables now exist)
    /// 5. Add projections and indexes (parent tables now exist)
    /// 6. Other operations (column adds/drops, alters, renames)
    /// </remarks>
    private List<(MigrationOperation Operation, int OriginalIndex)> SortOperationsByDependencies(
        IReadOnlyList<MigrationOperation> operations,
        IModel? model)
    {
        if (operations.Count < 2)
        {
            return operations.Select((op, idx) => (op, idx)).ToList();
        }

        // Phase-based ordering for robust dependency handling
        var phase1_DropProjectionsIndexes = new List<(MigrationOperation, int)>();
        var phase2_DropTables = new List<(MigrationOperation, int)>();
        var phase3_CreateTables = new List<(MigrationOperation, int)>();
        var phase4_CreateMvsDicts = new List<(MigrationOperation, int)>();
        var phase5_AddProjectionsIndexes = new List<(MigrationOperation, int)>();
        var phase6_Other = new List<(MigrationOperation, int)>();

        for (int i = 0; i < operations.Count; i++)
        {
            var op = operations[i];
            var item = (op, i);

            switch (op)
            {
                case DropProjectionOperation:
                case DropIndexOperation:
                    phase1_DropProjectionsIndexes.Add(item);
                    break;

                case DropTableOperation:
                    phase2_DropTables.Add(item);
                    break;

                case CreateTableOperation createOp:
                    if (IsMaterializedViewOrDictionary(createOp))
                        phase4_CreateMvsDicts.Add(item);
                    else
                        phase3_CreateTables.Add(item);
                    break;

                case AddProjectionOperation:
                case MaterializeProjectionOperation:
                case CreateIndexOperation:
                    phase5_AddProjectionsIndexes.Add(item);
                    break;

                default:
                    phase6_Other.Add(item);
                    break;
            }
        }

        // Combine phases in order, preserving original order within each phase
        var result = new List<(MigrationOperation, int)>(operations.Count);
        result.AddRange(phase1_DropProjectionsIndexes);
        result.AddRange(phase2_DropTables);
        result.AddRange(phase3_CreateTables);
        result.AddRange(phase4_CreateMvsDicts);
        result.AddRange(phase5_AddProjectionsIndexes);
        result.AddRange(phase6_Other);

        return result;
    }

    /// <summary>
    /// Determines if a CreateTableOperation is for a materialized view or dictionary.
    /// </summary>
    private bool IsMaterializedViewOrDictionary(CreateTableOperation operation)
    {
        var isMv = GetAnnotation<bool?>(operation, ClickHouseAnnotationNames.MaterializedView) == true;
        var isDict = GetAnnotation<bool?>(operation, ClickHouseAnnotationNames.Dictionary) == true;
        return isMv || isDict;
    }

    /// <summary>
    /// Generates a human-readable description for an operation.
    /// </summary>
    private static string GetOperationDescription(MigrationOperation operation)
    {
        return operation switch
        {
            CreateTableOperation createTable => $"CreateTable_{createTable.Name}",
            DropTableOperation dropTable => $"DropTable_{dropTable.Name}",
            AddColumnOperation addColumn => $"AddColumn_{addColumn.Table}_{addColumn.Name}",
            DropColumnOperation dropColumn => $"DropColumn_{dropColumn.Table}_{dropColumn.Name}",
            AlterColumnOperation alterColumn => $"AlterColumn_{alterColumn.Table}_{alterColumn.Name}",
            CreateIndexOperation createIndex => $"CreateIndex_{createIndex.Name}",
            DropIndexOperation dropIndex => $"DropIndex_{dropIndex.Name}",
            AddProjectionOperation addProjection => $"AddProjection_{addProjection.Name}",
            DropProjectionOperation dropProjection => $"DropProjection_{dropProjection.Name}",
            MaterializeProjectionOperation materialize => $"MaterializeProjection_{materialize.Name}",
            RenameTableOperation renameTable => $"RenameTable_{renameTable.Name}_to_{renameTable.NewName}",
            SqlOperation sqlOp => $"SqlOperation_{TruncateSql(sqlOp.Sql)}",
            _ => operation.GetType().Name
        };
    }

    /// <summary>
    /// Truncates SQL for description purposes.
    /// </summary>
    private static string TruncateSql(string sql)
    {
        const int maxLength = 30;
        var sanitized = sql.Replace("\n", " ").Replace("\r", "").Trim();
        if (sanitized.Length <= maxLength)
            return sanitized;
        return sanitized[..maxLength] + "...";
    }

    /// <summary>
    /// Helper to get annotation value from operation.
    /// </summary>
    private static T? GetAnnotation<T>(MigrationOperation operation, string name)
    {
        var annotation = operation.FindAnnotation(name);
        return annotation?.Value is T value ? value : default;
    }
}
