# Migrations Overview

EF.CH integrates with EF Core's migration system to manage ClickHouse schema changes. Because ClickHouse differs significantly from traditional relational databases, several aspects of the migration workflow require special handling.

## Standard Workflow

The standard EF Core migration commands work with EF.CH.

### Add a Migration

```bash
dotnet ef migrations add InitialCreate
```

Or in the Package Manager Console:

```powershell
Add-Migration InitialCreate
```

### Apply Migrations

```bash
dotnet ef database update
```

Or in the Package Manager Console:

```powershell
Update-Database
```

### Programmatic Application

```csharp
await context.Database.MigrateAsync();
```

## What Works

EF.CH supports the following migration operations:

| Operation | Support |
|-----------|---------|
| CREATE TABLE | Full -- generates ClickHouse DDL with ENGINE, ORDER BY, PARTITION BY, etc. |
| DROP TABLE | Full |
| ADD COLUMN | Full -- via ALTER TABLE ADD COLUMN |
| DROP COLUMN | Full -- via ALTER TABLE DROP COLUMN |
| ALTER COLUMN | Full -- via ALTER TABLE MODIFY COLUMN |
| RENAME COLUMN | Full -- via ALTER TABLE RENAME COLUMN |
| CREATE INDEX | Full -- generates skip index DDL |
| DROP INDEX | Full |
| Raw SQL | Full -- via `migrationBuilder.Sql()` |
| Projections | Full -- custom operations (AddProjection, DropProjection, MaterializeProjection) |
| Parameterized Views | Full -- custom operations (CreateParameterizedView, DropParameterizedView) |

## What Does Not Work

| Feature | Reason |
|---------|--------|
| **Down migrations** | ClickHouse lacks transactions; partial rollbacks are unsafe. EF.CH generates forward-only migrations that throw `ClickHouseDownMigrationNotSupportedException` in their `Down` method. |
| **Foreign keys** | ClickHouse does not support foreign key constraints. |
| **Unique constraints** | ClickHouse does not enforce uniqueness at the constraint level. |
| **Computed columns via ALTER** | ClickHouse does not support adding computed columns with ALTER TABLE. These must be defined at CREATE TABLE time. |

## ClickHouse DDL Differences

### ENGINE Clause

Every ClickHouse table requires an ENGINE clause. EF.CH adds the engine configuration from the model:

```sql
CREATE TABLE "Order"
(
    "Id" UInt64,
    "OrderDate" DateTime64(3),
    "Amount" Decimal(18,4)
)
ENGINE = MergeTree()
ORDER BY ("OrderDate", "Id")
PARTITION BY toYYYYMM("OrderDate")
TTL "OrderDate" + INTERVAL 365 DAY;
```

### No Transactions

ClickHouse does not support DDL transactions. Each migration operation executes independently. If a migration with multiple operations fails partway through, the applied operations are not rolled back.

EF.CH addresses this by [splitting migrations](split-migrations.md) into individual step files, each containing a single DDL operation.

### ALTER TABLE Restrictions

ClickHouse's ALTER TABLE has constraints that differ from SQL Server or PostgreSQL:

- **No table aliases in mutations**: UPDATE and DELETE cannot use table aliases
- **WHERE required for UPDATE**: `ALTER TABLE UPDATE` requires a WHERE clause; EF.CH emits `WHERE 1` when no predicate is specified
- **Single operation per ALTER**: You cannot add a column and create an index in the same ALTER TABLE statement

EF.CH handles these restrictions automatically through [split migrations](split-migrations.md) and the SQL generator.

## Migration History Table

EF.CH stores migration history in a MergeTree table:

```sql
CREATE TABLE IF NOT EXISTS "__EFMigrationsHistory"
(
    "MigrationId" String,
    "ProductVersion" String
)
ENGINE = MergeTree()
ORDER BY ("MigrationId");
```

## Idempotent DDL

EF.CH generates idempotent DDL where possible:

- `CREATE TABLE IF NOT EXISTS`
- `DROP TABLE IF EXISTS`
- `DROP VIEW IF EXISTS`

This reduces the risk of failures when re-running migrations after partial completion.

## See Also

- [Split Migrations](split-migrations.md) -- how EF.CH splits multi-operation migrations
- [Phase Ordering](phase-ordering.md) -- dependency ordering for tables, views, and projections
- [Projection Operations](projection-operations.md) -- custom migration operations for projections
- [Parameterized View Operations](parameterized-view-operations.md) -- custom migration operations for views
