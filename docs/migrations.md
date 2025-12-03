# Migrations

EF.CH supports EF Core migrations for managing database schema changes. Migrations generate ClickHouse-specific DDL including ENGINE, ORDER BY, PARTITION BY, and TTL clauses.

## Setup

### Install Design Package

Add the EF Core design package for CLI tools:

```bash
dotnet add package Microsoft.EntityFrameworkCore.Design
```

### Install EF Core Tools

```bash
dotnet tool install --global dotnet-ef
```

## Creating Migrations

### Initial Migration

```bash
dotnet ef migrations add InitialCreate
```

This creates a migration file in `Migrations/` with `Up()` and `Down()` methods.

### Adding Changes

After modifying your model:

```bash
dotnet ef migrations add AddProductCategory
```

## Applying Migrations

### To Database

```bash
dotnet ef database update
```

### Generate SQL Script

```bash
dotnet ef migrations script
```

### Programmatic Application

```csharp
await context.Database.MigrateAsync();
```

Or use `EnsureCreated` for development:

```csharp
await context.Database.EnsureCreatedAsync();
```

## Generated DDL

EF.CH generates ClickHouse-specific DDL from your model configuration.

### Basic MergeTree

```csharp
modelBuilder.Entity<Product>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => x.Id);
});
```

Generated DDL:
```sql
CREATE TABLE "Products" (
    "Id" UUID NOT NULL,
    "Name" String NOT NULL,
    "Price" Decimal(18, 4) NOT NULL
)
ENGINE = MergeTree
ORDER BY ("Id")
```

### With Partitioning and TTL

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });
    entity.HasPartitionByMonth(x => x.Timestamp);
    entity.HasTtl("Timestamp + INTERVAL 90 DAY");
});
```

Generated DDL:
```sql
CREATE TABLE "Events" (
    "Id" UUID NOT NULL,
    "Timestamp" DateTime64(3) NOT NULL,
    "EventType" String NOT NULL
)
ENGINE = MergeTree
PARTITION BY toYYYYMM("Timestamp")
ORDER BY ("Timestamp", "Id")
TTL "Timestamp" + INTERVAL 90 DAY
```

### ReplacingMergeTree

```csharp
entity.UseReplacingMergeTree(x => x.UpdatedAt, x => x.Id);
```

Generated DDL:
```sql
ENGINE = ReplacingMergeTree("UpdatedAt")
ORDER BY ("Id")
```

### Materialized Views

```csharp
entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });
entity.AsMaterializedViewRaw(
    sourceTable: "Orders",
    selectSql: "SELECT toDate(OrderDate) AS Date, ProductId, sum(Quantity) AS TotalQuantity FROM Orders GROUP BY Date, ProductId",
    populate: false);
```

Generated DDL:
```sql
-- Target table
CREATE TABLE "DailySales_MV" (...)
ENGINE = SummingMergeTree
ORDER BY ("Date", "ProductId")

-- Materialized view
CREATE MATERIALIZED VIEW "DailySales_MV_view"
TO "DailySales_MV"
AS SELECT toDate(OrderDate) AS Date, ProductId, sum(Quantity) AS TotalQuantity
FROM Orders GROUP BY Date, ProductId
```

## Migration File Example

```csharp
public partial class InitialCreate : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "Products",
            columns: table => new
            {
                Id = table.Column<Guid>(type: "UUID", nullable: false),
                Name = table.Column<string>(type: "String", nullable: false),
                Price = table.Column<decimal>(type: "Decimal(18, 4)", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_Products", x => x.Id);
            });
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(name: "Products");
    }
}
```

## Migration History

EF.CH tracks applied migrations in a `__EFMigrationsHistory` table:

```sql
SELECT * FROM "__EFMigrationsHistory";
```

## Keyless Entities

Keyless entities generate tables without primary key constraints:

```csharp
modelBuilder.Entity<EventLog>(entity =>
{
    entity.HasNoKey();
    entity.UseMergeTree(x => new { x.Timestamp, x.EventType });
});
```

Generated DDL:
```sql
CREATE TABLE "EventLogs" (
    "Timestamp" DateTime64(3) NOT NULL,
    "EventType" String NOT NULL,
    "Message" String NOT NULL
)
ENGINE = MergeTree
ORDER BY ("Timestamp", "EventType")
```

## Limitations

### No ALTER TABLE for Engine Changes

ClickHouse doesn't support changing ENGINE, ORDER BY, or PARTITION BY after table creation. These require:
1. Create new table with desired schema
2. Copy data: `INSERT INTO new_table SELECT * FROM old_table`
3. Drop old table
4. Rename new table

### Limited ALTER TABLE Support

Supported operations:
- `ADD COLUMN`
- `DROP COLUMN`
- `MODIFY COLUMN` (type changes with restrictions)
- `RENAME COLUMN`

Not supported via migrations:
- Changing ORDER BY columns
- Changing PARTITION BY expression
- Changing ENGINE type

### Column Type Changes

ClickHouse allows some type conversions:
```sql
ALTER TABLE "Products" MODIFY COLUMN "Price" Decimal(20, 6)
```

But not all (e.g., String to Int).

## Best Practices

### Use EnsureCreated for Development

```csharp
if (env.IsDevelopment())
{
    await context.Database.EnsureCreatedAsync();
}
else
{
    await context.Database.MigrateAsync();
}
```

### Review Generated SQL

Always review migration SQL before applying:

```bash
dotnet ef migrations script --idempotent
```

### Plan Schema Changes Carefully

Since ENGINE and ORDER BY can't be changed, plan your schema carefully upfront. Consider:
- What queries will you run?
- What's the partition strategy?
- What's the retention policy?

### Test Migrations

Test migrations against a copy of production data:

```bash
dotnet ef database update --connection "Host=test-server;Database=mydb"
```

## Troubleshooting

### "Table already exists"

Use `EnsureDeleted` before `EnsureCreated` in development:

```csharp
await context.Database.EnsureDeletedAsync();
await context.Database.EnsureCreatedAsync();
```

### Migration History Table Issues

If the history table is out of sync:

```sql
-- View applied migrations
SELECT * FROM "__EFMigrationsHistory";

-- Remove a migration entry (use with caution)
DELETE FROM "__EFMigrationsHistory" WHERE "MigrationId" = '20240115_InitialCreate';
```

## See Also

- [Getting Started](getting-started.md)
- [Scaffolding](scaffolding.md)
- [EF Core Migrations Docs](https://learn.microsoft.com/en-us/ef/core/managing-schemas/migrations/)
