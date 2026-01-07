using EF.CH.Metadata;
using EF.CH.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EF.CH.Migrations.Design;

/// <summary>
/// Migration operation phases for dependency-correct ordering.
/// </summary>
internal enum MigrationPhase
{
    /// <summary>Phase 1: Drop projections and drop indexes (remove dependents first).</summary>
    DropProjectionsIndexes = 1,

    /// <summary>Phase 2: Drop MVs and dictionaries (topo-sorted: dependents before sources).</summary>
    DropMvsDicts = 2,

    /// <summary>Phase 3: Drop regular tables.</summary>
    DropTables = 3,

    /// <summary>Phase 4: Create regular tables.</summary>
    CreateTables = 4,

    /// <summary>Phase 5: Add columns (must happen before MVs that might reference new columns).</summary>
    AddColumns = 5,

    /// <summary>Phase 6: Create MVs and dictionaries (topo-sorted: sources before dependents).</summary>
    CreateMvsDicts = 6,

    /// <summary>Phase 7: Alter, drop, and rename columns (safe after MVs created).</summary>
    ModifyColumns = 7,

    /// <summary>Phase 8: Create indexes.</summary>
    CreateIndexes = 8,

    /// <summary>Phase 9: Add and materialize projections.</summary>
    AddProjections = 9
}

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
/// Uses phase-based ordering with topological sorting within MV/dictionary phases
/// to ensure dependencies are respected.
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
    /// Sorts operations using phase-based ordering with topological sorting for MV/dictionary phases.
    /// </summary>
    /// <remarks>
    /// Phase order:
    /// 1. Drop projections/indexes (remove dependents first)
    /// 2. Drop MVs and dictionaries (topo-sorted: dependents before sources)
    /// 3. Drop regular tables
    /// 4. Create regular tables
    /// 5. Add columns (must happen before MVs that might reference new columns)
    /// 6. Create MVs and dictionaries (topo-sorted: sources before dependents)
    /// 7. Alter/drop/rename columns (safe after MVs created)
    /// 8. Create indexes
    /// 9. Add/materialize projections
    /// </remarks>
    private List<(MigrationOperation Operation, int OriginalIndex)> SortOperationsByDependencies(
        IReadOnlyList<MigrationOperation> operations,
        IModel? model)
    {
        if (operations.Count < 2)
        {
            return operations.Select((op, idx) => (op, idx)).ToList();
        }

        // 1. Classify operations into phases
        var phaseGroups = new Dictionary<MigrationPhase, List<(MigrationOperation Op, int Index)>>();
        foreach (MigrationPhase phase in Enum.GetValues<MigrationPhase>())
            phaseGroups[phase] = [];

        for (int i = 0; i < operations.Count; i++)
        {
            var phase = ClassifyOperation(operations[i]);
            phaseGroups[phase].Add((operations[i], i));
        }

        // 2. Build result by processing phases in order
        var result = new List<(MigrationOperation, int)>(operations.Count);

        // Phase 1: Drop projections/indexes (original order within phase)
        result.AddRange(phaseGroups[MigrationPhase.DropProjectionsIndexes]);

        // Phase 2: Drop MVs/dicts (topo-sorted, dependents first)
        result.AddRange(SortByDependencies(phaseGroups[MigrationPhase.DropMvsDicts], reverseForDrops: true));

        // Phase 3: Drop tables (original order)
        result.AddRange(phaseGroups[MigrationPhase.DropTables]);

        // Phase 4: Create tables (original order)
        result.AddRange(phaseGroups[MigrationPhase.CreateTables]);

        // Phase 5: Add columns (original order) - before MVs that might need them
        result.AddRange(phaseGroups[MigrationPhase.AddColumns]);

        // Phase 6: Create MVs/dicts (topo-sorted, sources first)
        result.AddRange(SortByDependencies(phaseGroups[MigrationPhase.CreateMvsDicts], reverseForDrops: false));

        // Phase 7: Alter/drop/rename columns (original order)
        result.AddRange(phaseGroups[MigrationPhase.ModifyColumns]);

        // Phase 8: Create indexes (original order)
        result.AddRange(phaseGroups[MigrationPhase.CreateIndexes]);

        // Phase 9: Add projections (original order)
        result.AddRange(phaseGroups[MigrationPhase.AddProjections]);

        return result;
    }

    /// <summary>
    /// Classifies an operation into the appropriate migration phase.
    /// </summary>
    private MigrationPhase ClassifyOperation(MigrationOperation op)
    {
        return op switch
        {
            DropProjectionOperation or DropIndexOperation
                => MigrationPhase.DropProjectionsIndexes,

            DropTableOperation dropOp when IsMvOrDictionary(dropOp)
                => MigrationPhase.DropMvsDicts,

            DropTableOperation
                => MigrationPhase.DropTables,

            CreateTableOperation createOp when IsMaterializedViewOrDictionary(createOp)
                => MigrationPhase.CreateMvsDicts,

            CreateTableOperation
                => MigrationPhase.CreateTables,

            // AddColumnOperation goes to Phase 5 - before MVs that might need the new column
            AddColumnOperation
                => MigrationPhase.AddColumns,

            // Alter/Drop/Rename go to Phase 7 - after MVs (you'd drop MV first if it uses the column)
            AlterColumnOperation or DropColumnOperation or RenameColumnOperation
                => MigrationPhase.ModifyColumns,

            CreateIndexOperation
                => MigrationPhase.CreateIndexes,

            AddProjectionOperation or MaterializeProjectionOperation
                => MigrationPhase.AddProjections,

            _ => MigrationPhase.ModifyColumns  // Default: other ops go to phase 7
        };
    }

    /// <summary>
    /// Sorts MV/dictionary operations by their dependencies using Kahn's algorithm.
    /// For creates: sources before dependents.
    /// For drops: dependents before sources (reverse order).
    /// </summary>
    private List<(MigrationOperation Op, int Index)> SortByDependencies(
        List<(MigrationOperation Op, int Index)> operations,
        bool reverseForDrops)
    {
        if (operations.Count < 2)
            return operations;

        // Build dependency graph
        // graph[A] = {B, C} means A is a dependency of B and C (B and C depend on A)
        var graph = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        var tableToOp = new Dictionary<string, (MigrationOperation Op, int Index)>(StringComparer.OrdinalIgnoreCase);

        // First pass: collect all table names
        foreach (var (op, idx) in operations)
        {
            var tableName = GetTableName(op);
            if (string.IsNullOrEmpty(tableName))
                continue;

            tableToOp[tableName] = (op, idx);
            graph[tableName] = [];
        }

        // Second pass: build edges based on dependencies
        foreach (var (op, idx) in operations)
        {
            var tableName = GetTableName(op);
            if (string.IsNullOrEmpty(tableName))
                continue;

            // Get dependencies (source tables this MV/dict depends on)
            var deps = GetDependencies(op);
            foreach (var dep in deps)
            {
                // If the dependency is in our operation set, add an edge
                if (graph.ContainsKey(dep))
                {
                    // dep → tableName means tableName depends on dep
                    graph[dep].Add(tableName);
                }
            }
        }

        // Kahn's algorithm for topological sort
        var inDegree = graph.Keys.ToDictionary(k => k, _ => 0, StringComparer.OrdinalIgnoreCase);
        foreach (var edges in graph.Values)
        {
            foreach (var to in edges)
            {
                if (inDegree.ContainsKey(to))
                    inDegree[to]++;
            }
        }

        // Start with nodes that have no incoming edges (no dependencies)
        var queue = new Queue<string>(inDegree.Where(kv => kv.Value == 0).Select(kv => kv.Key));
        var sortedResult = new List<(MigrationOperation Op, int Index)>();

        while (queue.Count > 0)
        {
            var node = queue.Dequeue();
            if (tableToOp.TryGetValue(node, out var opData))
                sortedResult.Add(opData);

            foreach (var neighbor in graph[node])
            {
                if (inDegree.ContainsKey(neighbor))
                {
                    inDegree[neighbor]--;
                    if (inDegree[neighbor] == 0)
                        queue.Enqueue(neighbor);
                }
            }
        }

        // Handle cycles or missing entries: add any remaining ops in original order
        var processedIndices = new HashSet<int>(sortedResult.Select(r => r.Index));
        foreach (var (op, idx) in operations)
        {
            if (!processedIndices.Contains(idx))
                sortedResult.Add((op, idx));
        }

        // Reverse for drops (dependents first, then their sources)
        if (reverseForDrops)
            sortedResult.Reverse();

        return sortedResult;
    }

    /// <summary>
    /// Gets the dependencies (source tables) for an MV or dictionary operation.
    /// </summary>
    private HashSet<string> GetDependencies(MigrationOperation op)
    {
        var deps = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // Check MaterializedViewSource annotation
        var mvSource = GetAnnotation<string>(op, ClickHouseAnnotationNames.MaterializedViewSource);
        if (!string.IsNullOrEmpty(mvSource))
            deps.Add(mvSource);

        // Check DictionarySource annotation
        var dictSource = GetAnnotation<string>(op, ClickHouseAnnotationNames.DictionarySource);
        if (!string.IsNullOrEmpty(dictSource))
            deps.Add(dictSource);

        // Parse MaterializedViewQuery for additional table references
        var mvQuery = GetAnnotation<string>(op, ClickHouseAnnotationNames.MaterializedViewQuery);
        if (!string.IsNullOrEmpty(mvQuery))
        {
            var tableRefs = SqlTableExtractor.ExtractTableReferences(mvQuery);
            foreach (var tableRef in tableRefs)
                deps.Add(tableRef);
        }

        return deps;
    }

    /// <summary>
    /// Gets the table name from an operation.
    /// </summary>
    private static string? GetTableName(MigrationOperation op)
    {
        return op switch
        {
            CreateTableOperation createOp => createOp.Name,
            DropTableOperation dropOp => dropOp.Name,
            _ => null
        };
    }

    /// <summary>
    /// Determines if a DropTableOperation is for a materialized view or dictionary.
    /// Relies on annotations being persisted by the scaffolder.
    /// </summary>
    private bool IsMvOrDictionary(DropTableOperation op)
    {
        return GetAnnotation<bool?>(op, ClickHouseAnnotationNames.MaterializedView) == true
            || GetAnnotation<bool?>(op, ClickHouseAnnotationNames.Dictionary) == true;
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
            RenameColumnOperation renameColumn => $"RenameColumn_{renameColumn.Table}_{renameColumn.Name}",
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
