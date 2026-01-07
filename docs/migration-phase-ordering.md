# Migration Phase Ordering

When EF.CH splits a migration into individual step files, operations are automatically sorted to ensure they execute in a safe order. This document explains why ordering matters and what to expect.

## Why Ordering Matters

ClickHouse lacks transaction support. If step 3 of a 5-step migration fails, steps 1-2 have already been applied and cannot be rolled back. This makes operation ordering critical for:

- **Materialized views**: Must be created after their source tables exist
- **Cascading MVs**: If MV-B reads from MV-A, MV-A must be created first
- **Indexes**: Must be created after the columns they reference
- **Dictionaries**: Must be created after their source tables
- **Projections**: Must be added after their parent tables exist

When dropping objects, the order reverses - dependents must be dropped before their sources.

## Automatic Phase Ordering

EF.CH automatically sorts operations into 9 phases:

| Phase | What Happens |
|-------|-------------|
| 1 | Drop projections and indexes |
| 2 | Drop materialized views and dictionaries |
| 3 | Drop regular tables |
| 4 | Create regular tables |
| 5 | Add columns |
| 6 | Create materialized views and dictionaries |
| 7 | Modify columns (alter, drop, rename) |
| 8 | Create indexes |
| 9 | Add and materialize projections |

You don't need to worry about the order you define operations in your migration - EF.CH will reorder them correctly.

## Cascading Dependencies

When you have materialized views that read from other materialized views, EF.CH analyzes the SQL queries to determine the correct order:

```
Orders (table)
    ↓
HourlySummary (MV reading from Orders)
    ↓
DailySummary (MV reading from HourlySummary)
```

**Creating**: Orders → HourlySummary → DailySummary

**Dropping**: DailySummary → HourlySummary → Orders

This works automatically for both raw SQL and LINQ-based materialized views.

## What You See

When you run `dotnet ef migrations add`, each operation becomes a separate step file:

```
Migrations/
├── 20250107_AddAnalytics_001_CreateTable_Orders.cs
├── 20250107_AddAnalytics_002_CreateTable_HourlySummary.cs
├── 20250107_AddAnalytics_003_CreateTable_DailySummary.cs
├── 20250107_AddAnalytics_004_CreateIndex_IX_Orders_Date.cs
└── 20250107_AddAnalyticsModelSnapshot.cs
```

The step numbers reflect the safe execution order, not the order you defined them.

## Edge Cases

### Dropping MVs and Their Source Tables

When you remove both an MV and its source table in the same migration, EF.CH ensures the MV is dropped first. The system remembers what type each entity was (table, MV, or dictionary) even after you remove it from your model.

### Adding Columns with Indexes

If you add a new column and an index on that column in the same migration:

```csharp
// In your migration
migrationBuilder.AddColumn<string>("Email", "Users");
migrationBuilder.CreateIndex("IX_Users_Email", "Users", "Email");
```

EF.CH ensures the column is added (Phase 5) before the index is created (Phase 8).

### Adding Columns Referenced by MVs

If you add a column and create a materialized view that uses it:

```csharp
// In your migration
migrationBuilder.AddColumn<decimal>("Discount", "Orders");
migrationBuilder.CreateTable("DiscountSummary", /* MV that references Discount */);
```

EF.CH ensures the column is added (Phase 5) before the MV is created (Phase 6).

### Dictionary Drops

Dictionaries use `DROP DICTIONARY` syntax instead of `DROP TABLE`. EF.CH generates the correct DDL automatically based on annotations from your model configuration.

## Troubleshooting

### Migration Failed Partway Through

Check `__EFMigrationsHistory` to see which steps completed. Fix the underlying issue, then re-run `dotnet ef database update` - completed steps are skipped automatically.

### Unexpected Order

If operations aren't in the order you expected, it's likely because EF.CH reordered them for safety. The step file names include the operation description so you can verify the order makes sense.

## See Also

- [Split Migrations](features/split-migrations.md) - How migrations are split into steps
- [Migrations](migrations.md) - General migration documentation
- [Materialized Views](features/materialized-views.md) - MV configuration
