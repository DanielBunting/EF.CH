# Split Migrations

EF.CH automatically splits multi-operation migrations into individual step files for safer execution on ClickHouse.

## Why Split Migrations?

ClickHouse lacks ACID transactions. When a standard EF Core migration contains multiple operations:

```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    migrationBuilder.CreateTable("Orders", ...);      // Step 1
    migrationBuilder.CreateTable("OrderItems", ...);  // Step 2
    migrationBuilder.CreateIndex("IX_OrderItems_OrderId", ...);  // Step 3
}
```

If step 2 fails, step 1 has already been applied and **cannot be rolled back**. The database is left in an inconsistent state with no record of what succeeded.

## How It Works

EF.CH's `ClickHouseMigrationsScaffolder` splits each migration into individual step files:

```
Migrations/
├── 20250107120000_AddOrders_001_CreateTable_Orders.cs
├── 20250107120000_AddOrders_002_CreateTable_OrderItems.cs
├── 20250107120000_AddOrders_003_CreateIndex_IX_OrderItems_OrderId.cs
└── 20250107120000_AddOrdersModelSnapshot.cs
```

Each step file contains exactly **one operation** and is tracked independently in the migration history table.

### File Naming Convention

```
{Timestamp}_{MigrationName}_{StepNumber:D3}_{OperationDescription}.cs
```

- **Timestamp**: Standard EF Core timestamp (e.g., `20250107120000`)
- **MigrationName**: Your migration name (e.g., `AddOrders`)
- **StepNumber**: 3-digit padded step number (e.g., `001`, `002`)
- **OperationDescription**: Human-readable operation name (e.g., `CreateTable_Orders`)

### Operation Descriptions

| Operation | Description Format |
|-----------|-------------------|
| `CreateTableOperation` | `CreateTable_{TableName}` |
| `DropTableOperation` | `DropTable_{TableName}` |
| `AddColumnOperation` | `AddColumn_{Table}_{Column}` |
| `DropColumnOperation` | `DropColumn_{Table}_{Column}` |
| `CreateIndexOperation` | `CreateIndex_{IndexName}` |
| `DropIndexOperation` | `DropIndex_{IndexName}` |
| `AddProjectionOperation` | `AddProjection_{Name}` |
| `DropProjectionOperation` | `DropProjection_{Name}` |
| `RenameTableOperation` | `RenameTable_{OldName}_to_{NewName}` |
| `SqlOperation` | `SqlOperation_{TruncatedSql}` |

## Phase-Based Ordering

Operations are automatically sorted into phases to ensure dependencies are satisfied:

| Phase | Operations | Rationale |
|-------|-----------|-----------|
| 1 | Drop projections, Drop indexes | Remove dependents first |
| 2 | Drop tables | Safe after dependents removed |
| 3 | Create regular tables | Base tables must exist first |
| 4 | Create MVs and dictionaries | Source tables now exist |
| 5 | Add projections, Add indexes | Parent tables now exist |
| 6 | Other (columns, alters, renames) | Table modifications |

This ensures materialized views are always created after their source tables, and projections are always created after their parent tables—regardless of the order you specify in your migration.

See [Migration Phase Ordering](../migration-phase-ordering.md) for detailed documentation.

## Idempotent DDL

All generated DDL uses idempotent syntax so steps can be safely re-run:

| Operation | SQL Pattern |
|-----------|-------------|
| Create table | `CREATE TABLE IF NOT EXISTS` |
| Drop table | `DROP TABLE IF EXISTS` |
| Add column | `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` |
| Drop column | `ALTER TABLE ... DROP COLUMN IF EXISTS` |
| Add index | `ALTER TABLE ... ADD INDEX IF NOT EXISTS` |
| Drop index | `ALTER TABLE ... DROP INDEX IF EXISTS` |
| Add projection | `ALTER TABLE ... ADD PROJECTION IF NOT EXISTS` |
| Drop projection | `ALTER TABLE ... DROP PROJECTION IF EXISTS` |
| Create MV | `CREATE MATERIALIZED VIEW IF NOT EXISTS` |
| Create dictionary | `CREATE DICTIONARY IF NOT EXISTS` |

If a step fails and you fix the issue, you can re-run the migration and already-completed steps will be skipped.

## Forward-Only Migrations

ClickHouse migrations are **forward-only**. The `Down()` method throws `ClickHouseDownMigrationNotSupportedException`:

```csharp
protected override void Down(MigrationBuilder migrationBuilder)
{
    throw new ClickHouseDownMigrationNotSupportedException("20250107120000_AddOrders_001");
}
```

**Why?**

1. **No transactions**: A multi-step rollback could fail partway through, leaving the database in an even worse state
2. **Data loss risk**: Dropping tables loses data permanently
3. **ClickHouse philosophy**: ClickHouse is designed for append-only analytics workloads

**To "undo" changes**, create a new forward migration:

```bash
# Instead of: dotnet ef database update PreviousMigration
# Do this:
dotnet ef migrations add RevertOrdersChanges
# Then manually write the reversal logic
```

## Migration History Tracking

Each step is tracked independently in `__EFMigrationsHistory`:

```sql
SELECT * FROM "__EFMigrationsHistory" ORDER BY "MigrationId";
```

```
┌─MigrationId─────────────────────────────────────────────────┬─ProductVersion─┐
│ 20250107120000_AddOrders_001_CreateTable_Orders             │ 8.0.0          │
│ 20250107120000_AddOrders_002_CreateTable_OrderItems         │ 8.0.0          │
│ 20250107120000_AddOrders_003_CreateIndex_IX_OrderItems      │ 8.0.0          │
└─────────────────────────────────────────────────────────────┴────────────────┘
```

This provides:
- **Granular tracking**: Know exactly which steps succeeded
- **Resume capability**: If step 2 fails, fix the issue and re-run; step 1 won't be re-executed
- **Audit trail**: See exactly when each schema change was applied

## Usage

Split migrations work automatically with standard EF Core CLI commands:

```bash
# Create migration (generates split files)
dotnet ef migrations add AddOrders

# Apply all pending steps
dotnet ef database update

# Generate SQL script for all steps
dotnet ef migrations script
```

### Programmatic Application

```csharp
// Apply all pending migration steps
await context.Database.MigrateAsync();
```

## Example

Given this model configuration:

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.OrderDate, x.Id });
});

modelBuilder.Entity<HourlySummary>(entity =>
{
    entity.HasKey(e => new { e.Hour, e.ProductId });
    entity.UseSummingMergeTree(x => new { x.Hour, x.ProductId });
    entity.AsMaterializedView<HourlySummary, Order>(
        query: orders => orders
            .GroupBy(o => new { Hour = o.OrderDate.Date, o.ProductId })
            .Select(g => new HourlySummary { ... }),
        populate: false);
});
```

Running `dotnet ef migrations add InitialCreate` generates:

**Step 001: CreateTable_Orders.cs**
```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    migrationBuilder.CreateTable(
        name: "Orders",
        columns: table => new { ... });
}
```

**Step 002: CreateTable_HourlySummary.cs** (MV comes after regular table)
```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    migrationBuilder.CreateTable(
        name: "HourlySummary",
        columns: table => new { ... });
    // Includes MV creation SQL
}
```

The MV is automatically placed in step 002 because phase-based ordering ensures all regular tables (phase 3) are created before MVs (phase 4).

## Recovering from Failures

If a migration step fails:

1. **Check the history table** to see which steps succeeded:
   ```sql
   SELECT * FROM "__EFMigrationsHistory"
   WHERE "MigrationId" LIKE '20250107120000_AddOrders%';
   ```

2. **Fix the underlying issue** (schema conflict, permissions, etc.)

3. **Re-run the migration**:
   ```bash
   dotnet ef database update
   ```

   Already-completed steps are skipped (idempotent DDL + history tracking).

4. **If needed, manually clean up**:
   ```sql
   -- Remove a stuck migration entry
   ALTER TABLE "__EFMigrationsHistory"
   DELETE WHERE "MigrationId" = '20250107120000_AddOrders_002_CreateTable_OrderItems';
   ```

## Configuration

Split migrations are enabled by default for ClickHouse. No additional configuration is required.

The `ClickHouseMigrationsScaffolder` and `ClickHouseMigrationsSplitter` are automatically registered when you use `.UseClickHouse()`.

## See Also

- [Migrations](../migrations.md) - General migration documentation
- [Migration Phase Ordering](../migration-phase-ordering.md) - Detailed phase ordering explanation
- [Materialized Views](materialized-views.md) - MV configuration
- [Projections](projections.md) - Projection configuration
