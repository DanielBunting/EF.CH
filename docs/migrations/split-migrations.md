# Split Migrations

ClickHouse does not support transactional DDL. If a migration containing multiple operations fails partway through, the operations that already executed cannot be rolled back. EF.CH addresses this by automatically splitting multi-operation migrations into individual step files, each containing exactly one DDL operation.

## Why Splitting Is Necessary

Consider a migration that adds a column and creates an index:

```csharp
// Standard EF Core migration -- two operations
protected override void Up(MigrationBuilder migrationBuilder)
{
    migrationBuilder.AddColumn<string>(
        name: "Category",
        table: "Orders",
        type: "String",
        nullable: false,
        defaultValue: "");

    migrationBuilder.CreateIndex(
        name: "IX_Orders_Category",
        table: "Orders",
        column: "Category");
}
```

In ClickHouse, these two operations cannot run in a single ALTER TABLE statement. If the first succeeds but the second fails, re-running the migration will fail because EF Core sees the migration as unapplied (it never recorded in `__EFMigrationsHistory`), but the column already exists.

## Automatic Splitting Behavior

When `Add-Migration` detects multiple operations, EF.CH's `ClickHouseMigrationsScaffolder` splits them into separate migration files.

```bash
dotnet ef migrations add AddCategoryAndIndex
```

Instead of one migration file, this produces:

```
Migrations/
    20250201100000_AddCategoryAndIndex_001.cs          # Step 1: AddColumn
    20250201100000_AddCategoryAndIndex_001.Designer.cs
    20250201100000_AddCategoryAndIndex_002.cs          # Step 2: CreateIndex
    20250201100000_AddCategoryAndIndex_002.Designer.cs
    AppDbContextModelSnapshot.cs                        # Shared snapshot (updated once)
```

Each step file contains a single operation:

**Step 1: AddColumn**
```csharp
// Step 1 of 2: AddColumn_Orders_Category
public partial class _20250201100000_AddCategoryAndIndex_001 : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<string>(
            name: "Category",
            table: "Orders",
            type: "String",
            nullable: false,
            defaultValue: "");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        throw new ClickHouseDownMigrationNotSupportedException(
            "20250201100000_AddCategoryAndIndex_001");
    }
}
```

**Step 2: CreateIndex**
```csharp
// Step 2 of 2: CreateIndex_IX_Orders_Category
public partial class _20250201100000_AddCategoryAndIndex_002 : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateIndex(
            name: "IX_Orders_Category",
            table: "Orders",
            column: "Category");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        throw new ClickHouseDownMigrationNotSupportedException(
            "20250201100000_AddCategoryAndIndex_002");
    }
}
```

## Recovery from Partial Failures

With split migrations, each step is recorded independently in `__EFMigrationsHistory`. If step 1 succeeds but step 2 fails:

1. `__EFMigrationsHistory` contains step 1's migration ID
2. Running `dotnet ef database update` again skips step 1 and retries step 2
3. No manual intervention is needed

## Forward-Only Migrations

All split migration steps generate a `Down` method that throws `ClickHouseDownMigrationNotSupportedException`. ClickHouse does not support transactional rollback, so Down migrations are not safe to execute.

> **Note:** To revert a schema change, create a new migration that performs the inverse operation (for example, drop the column that was added). This is the recommended approach for all ClickHouse migrations.

## Single-Operation Migrations

When a migration contains only one operation, EF.CH uses standard EF Core scaffolding without splitting. The resulting migration file is a single file as usual.

## Operation Ordering

The splitter does not just separate operations -- it also reorders them based on dependencies. See [Phase Ordering](phase-ordering.md) for details on how operations are sorted to respect ClickHouse DDL constraints.

## See Also

- [Migrations Overview](overview.md) -- what works and what does not in ClickHouse migrations
- [Phase Ordering](phase-ordering.md) -- dependency-correct ordering of DDL operations
- [Projection Operations](projection-operations.md) -- custom operations for projections
