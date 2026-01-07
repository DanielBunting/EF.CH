# Migration Phase Ordering

When EF.CH splits a migration into individual step files, operations are sorted into phases to ensure dependencies are satisfied. This document explains the ordering strategy.

## The Problem

ClickHouse has unique constraints that make migration ordering critical:

1. **No transactions**: If step 3 of 5 fails, steps 1-2 have already been applied and cannot be rolled back
2. **Materialized views depend on source tables**: An MV cannot be created before its source table exists
3. **Projections depend on parent tables**: A projection cannot be added to a non-existent table
4. **LINQ-based MVs**: When using `AsMaterializedView<TTarget, TSource>()`, the source table dependency is embedded in the query expression, not always detectable via annotations

The original approach used topological sorting based on annotation detection. This failed for LINQ-based materialized views where the source table couldn't be reliably determined.

## The Solution: Phase-Based Ordering

Instead of detecting individual dependencies, operations are categorized into phases that guarantee correct ordering:

| Phase | Operations | Rationale |
|-------|-----------|-----------|
| 1 | Drop projections, Drop indexes | Remove dependents before their parents |
| 2 | Drop tables | Safe after dependents removed |
| 3 | Create regular tables | Base tables must exist first |
| 4 | Create MVs and dictionaries | Source tables now exist |
| 5 | Add projections, Add indexes | Parent tables now exist |
| 6 | Other (columns, alters, renames) | Table modifications |

## How It Works

### Detection

- **Regular tables**: `CreateTableOperation` without MV or Dictionary annotations
- **MVs**: `CreateTableOperation` with `ClickHouse:MaterializedView = true` annotation
- **Dictionaries**: `CreateTableOperation` with `ClickHouse:Dictionary = true` annotation

### Example

Given these operations in any order:
```
AddProjectionOperation (prj_daily on Orders)
CreateTableOperation (HourlySummary, MV=true)
DropIndexOperation (IX_Old on OldTable)
CreateTableOperation (Orders)
DropTableOperation (OldTable)
CreateIndexOperation (IX_Orders_Date on Orders)
```

After phase sorting:
```
Phase 1: DropIndexOperation (IX_Old)
Phase 2: DropTableOperation (OldTable)
Phase 3: CreateTableOperation (Orders)           <- Regular table
Phase 4: CreateTableOperation (HourlySummary)    <- MV, after all tables
Phase 5: AddProjectionOperation (prj_daily)
Phase 5: CreateIndexOperation (IX_Orders_Date)
```

## Limitations

### Drop Ordering for MVs

When dropping tables, we cannot reliably detect whether a `DropTableOperation` is for a materialized view or a regular table. The entity has been removed from the model, so annotations aren't available.

**Conservative approach**: All table drops happen in phase 2, after projection/index drops. If you're dropping both an MV and its source table in the same migration, you may need to split them into separate migrations or manually order the operations.

### Within-Phase Ordering

Operations within the same phase maintain their original order from the migration. This preserves any intentional ordering you've specified.

## Implementation

The ordering is implemented in `ClickHouseMigrationsSplitter.SortOperationsByDependencies()`:

```csharp
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
```

## Why Not Topological Sort?

The previous implementation used Kahn's algorithm to build a dependency graph based on:
- `MaterializedViewSource` annotation for MVs
- Table names for indexes, projections, columns

This failed when:
1. LINQ-based MVs didn't have the source table annotated
2. Complex queries referenced multiple tables
3. The annotation was set but didn't match the actual table name

Phase-based ordering is:
- **Simpler**: No graph traversal, just categorization
- **More robust**: Works regardless of how MVs are defined
- **Predictable**: Same input always produces same output

## See Also

- [Split Migrations](features/split-migrations.md) - How migrations are split into steps
- [Migrations](migrations.md) - General migration documentation
- [Materialized Views](features/materialized-views.md) - MV configuration
