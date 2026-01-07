using EF.CH.Metadata;
using EF.CH.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EF.CH.Migrations.Design;

/// <summary>
/// Record representing a single step in a split migration.
/// </summary>
/// <param name="StepNumber">The 1-based step number (formatted as "001", "002", etc.).</param>
/// <param name="OperationDescription">Human-readable description of the operation (e.g., "CreateTable_Orders").</param>
/// <param name="Operation">The migration operation for this step.</param>
/// <param name="OriginalIndex">The original index in the unsorted operations list.</param>
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
/// Each operation becomes its own migration to enable atomic execution and resume after failure.
/// Uses topological sorting to ensure dependencies (table creation before indices/projections) are respected.
/// </summary>
public class ClickHouseMigrationsSplitter
{
    /// <summary>
    /// Splits operations into individual step migrations.
    /// Operations are topologically sorted to ensure dependencies are handled correctly.
    /// </summary>
    /// <param name="operations">The migration operations to split.</param>
    /// <param name="model">The target model for dependency resolution.</param>
    /// <returns>A list of step migrations, one per operation.</returns>
    public IReadOnlyList<StepMigration> Split(
        IReadOnlyList<MigrationOperation> operations,
        IModel? model = null)
    {
        if (operations.Count == 0)
            return [];

        // Topologically sort operations to respect dependencies
        var sortedOperations = SortOperations(operations, model);

        // Create step migrations
        var steps = new List<StepMigration>(sortedOperations.Count);
        for (var i = 0; i < sortedOperations.Count; i++)
        {
            var (operation, originalIndex) = sortedOperations[i];
            steps.Add(new StepMigration(
                StepNumber: i + 1,
                OperationDescription: GetOperationDescription(operation),
                Operation: operation,
                OriginalIndex: originalIndex));
        }

        return steps;
    }

    /// <summary>
    /// Topologically sorts operations so tables are created before dependent operations.
    /// Uses Kahn's algorithm to ensure correct ordering while preserving original order
    /// for operations with no dependencies.
    /// </summary>
    private static IReadOnlyList<(MigrationOperation Operation, int OriginalIndex)> SortOperations(
        IReadOnlyList<MigrationOperation> operations,
        IModel? model)
    {
        if (operations.Count <= 1)
            return operations.Select((op, idx) => (op, idx)).ToList();

        // Build map of table name -> CreateTableOperation with original index
        var tableOperations = new Dictionary<string, (MigrationOperation Op, int Index)>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < operations.Count; i++)
        {
            if (operations[i] is CreateTableOperation createOp)
            {
                tableOperations[createOp.Name] = (operations[i], i);
            }
        }

        // Build dependency graph using Kahn's algorithm
        var inDegree = new Dictionary<int, int>();
        var dependents = new Dictionary<int, List<int>>();

        for (var i = 0; i < operations.Count; i++)
        {
            inDegree[i] = 0;
            dependents[i] = [];
        }

        // Calculate dependencies
        for (var i = 0; i < operations.Count; i++)
        {
            var deps = GetOperationDependencies(operations[i], model);
            foreach (var dep in deps)
            {
                if (tableOperations.TryGetValue(dep, out var depInfo) && depInfo.Index != i)
                {
                    dependents[depInfo.Index].Add(i);
                    inDegree[i]++;
                }
            }
        }

        // Topological sort - preserve original order for equal-priority operations
        var queue = new Queue<int>();
        for (var i = 0; i < operations.Count; i++)
        {
            if (inDegree[i] == 0)
                queue.Enqueue(i);
        }

        var result = new List<(MigrationOperation, int)>();
        while (queue.Count > 0)
        {
            var idx = queue.Dequeue();
            result.Add((operations[idx], idx));

            foreach (var dependent in dependents[idx].OrderBy(d => d))
            {
                inDegree[dependent]--;
                if (inDegree[dependent] == 0)
                    queue.Enqueue(dependent);
            }
        }

        // Add any remaining (shouldn't happen with valid migrations, but be safe)
        for (var i = 0; i < operations.Count; i++)
        {
            if (!result.Any(r => r.Item2 == i))
                result.Add((operations[i], i));
        }

        return result;
    }

    /// <summary>
    /// Gets table names that this operation depends on.
    /// </summary>
    private static HashSet<string> GetOperationDependencies(MigrationOperation operation, IModel? model)
    {
        var deps = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        switch (operation)
        {
            case CreateTableOperation createOp:
                var sourceDep = GetSourceTableDependency(createOp);
                if (!string.IsNullOrEmpty(sourceDep))
                    deps.Add(sourceDep);
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
            case AddProjectionOperation addProjOp when !string.IsNullOrEmpty(addProjOp.Table):
                deps.Add(addProjOp.Table);
                break;
            case DropProjectionOperation dropProjOp when !string.IsNullOrEmpty(dropProjOp.Table):
                deps.Add(dropProjOp.Table);
                break;
            case MaterializeProjectionOperation matProjOp when !string.IsNullOrEmpty(matProjOp.Table):
                deps.Add(matProjOp.Table);
                break;
        }

        return deps;
    }

    /// <summary>
    /// Gets the source table dependency for materialized views and dictionaries.
    /// </summary>
    private static string? GetSourceTableDependency(CreateTableOperation operation)
    {
        // Check for materialized view source
        if (operation.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewSource)?.Value is string mvSource
            && !string.IsNullOrEmpty(mvSource))
        {
            return mvSource;
        }

        // Check for dictionary source
        if (operation.FindAnnotation(ClickHouseAnnotationNames.DictionarySource)?.Value is string dictSource
            && !string.IsNullOrEmpty(dictSource))
        {
            return dictSource;
        }

        return null;
    }

    /// <summary>
    /// Gets a human-readable description of an operation for the step file comment.
    /// </summary>
    private static string GetOperationDescription(MigrationOperation operation) => operation switch
    {
        CreateTableOperation op => $"CreateTable_{op.Name}",
        DropTableOperation op => $"DropTable_{op.Name}",
        AddColumnOperation op => $"AddColumn_{op.Table}_{op.Name}",
        DropColumnOperation op => $"DropColumn_{op.Table}_{op.Name}",
        AlterColumnOperation op => $"AlterColumn_{op.Table}_{op.Name}",
        CreateIndexOperation op => $"CreateIndex_{op.Name ?? op.Table}",
        DropIndexOperation op => $"DropIndex_{op.Name ?? op.Table}",
        AddProjectionOperation op => $"AddProjection_{op.Name}",
        DropProjectionOperation op => $"DropProjection_{op.Name}",
        MaterializeProjectionOperation op => $"MaterializeProjection_{op.Name}",
        RenameTableOperation op => $"RenameTable_{op.Name}",
        RenameColumnOperation op => $"RenameColumn_{op.Table}_{op.Name}",
        AddForeignKeyOperation op => $"AddForeignKey_{op.Name}",
        DropForeignKeyOperation op => $"DropForeignKey_{op.Name}",
        AddPrimaryKeyOperation op => $"AddPrimaryKey_{op.Name}",
        DropPrimaryKeyOperation op => $"DropPrimaryKey_{op.Name}",
        AddUniqueConstraintOperation op => $"AddUniqueConstraint_{op.Name}",
        DropUniqueConstraintOperation op => $"DropUniqueConstraint_{op.Name}",
        AddCheckConstraintOperation op => $"AddCheckConstraint_{op.Name}",
        DropCheckConstraintOperation op => $"DropCheckConstraint_{op.Name}",
        SqlOperation => "SqlOperation",
        InsertDataOperation op => $"InsertData_{op.Table}",
        DeleteDataOperation op => $"DeleteData_{op.Table}",
        UpdateDataOperation op => $"UpdateData_{op.Table}",
        _ => operation.GetType().Name
    };
}
