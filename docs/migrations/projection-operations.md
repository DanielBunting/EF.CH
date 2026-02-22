# Projection Operations

Projections are pre-computed alternative data representations stored alongside a ClickHouse table. EF.CH provides three custom migration operations for managing projections: `AddProjection`, `DropProjection`, and `MaterializeProjection`.

## AddProjection

Adds a projection to an existing table. By default, existing data is materialized into the projection.

```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    migrationBuilder.AddProjection(
        table: "Orders",
        name: "prj_by_status",
        selectSql: "SELECT * ORDER BY (\"Status\", \"OrderDate\")"
    );
}
```

Generated DDL:

```sql
ALTER TABLE "Orders" ADD PROJECTION "prj_by_status"
(
    SELECT * ORDER BY ("Status", "OrderDate")
);

ALTER TABLE "Orders" MATERIALIZE PROJECTION "prj_by_status";
```

### Without Materialization

Skip materializing existing data by setting `materialize: false`. The projection will only apply to newly inserted data.

```csharp
migrationBuilder.AddProjection(
    table: "Orders",
    name: "prj_by_status",
    selectSql: "SELECT * ORDER BY (\"Status\", \"OrderDate\")",
    materialize: false
);
```

Generated DDL:

```sql
ALTER TABLE "Orders" ADD PROJECTION "prj_by_status"
(
    SELECT * ORDER BY ("Status", "OrderDate")
);
```

### Aggregation Projection

Projections can also define aggregations.

```csharp
migrationBuilder.AddProjection(
    table: "Orders",
    name: "prj_daily_totals",
    selectSql: @"SELECT
        toDate(""OrderDate"") AS ""Date"",
        ""Category"",
        sum(""Amount"") AS ""Total"",
        count() AS ""Count""
    GROUP BY ""Date"", ""Category"""
);
```

Generated DDL:

```sql
ALTER TABLE "Orders" ADD PROJECTION "prj_daily_totals"
(
    SELECT
        toDate("OrderDate") AS "Date",
        "Category",
        sum("Amount") AS "Total",
        count() AS "Count"
    GROUP BY "Date", "Category"
);

ALTER TABLE "Orders" MATERIALIZE PROJECTION "prj_daily_totals";
```

## DropProjection

Removes a projection from a table.

```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    migrationBuilder.DropProjection(
        table: "Orders",
        name: "prj_old"
    );
}
```

Generated DDL:

```sql
ALTER TABLE "Orders" DROP PROJECTION IF EXISTS "prj_old";
```

The `ifExists` parameter defaults to `true`. Set it to `false` to generate DDL without the `IF EXISTS` clause:

```csharp
migrationBuilder.DropProjection(
    table: "Orders",
    name: "prj_old",
    ifExists: false
);
```

## MaterializeProjection

Materializes a projection for existing data. Use this when a projection was added without materialization, or when you need to re-materialize after a schema change.

```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    migrationBuilder.MaterializeProjection(
        table: "Orders",
        name: "prj_by_status"
    );
}
```

Generated DDL:

```sql
ALTER TABLE "Orders" MATERIALIZE PROJECTION "prj_by_status";
```

### Partition-Specific Materialization

Materialize a projection for a specific partition only, which is faster for large tables.

```csharp
migrationBuilder.MaterializeProjection(
    table: "Orders",
    name: "prj_by_status",
    inPartition: "202401"
);
```

Generated DDL:

```sql
ALTER TABLE "Orders" MATERIALIZE PROJECTION "prj_by_status" IN PARTITION '202401';
```

## Phase Ordering

Projection operations are handled in specific phases during [migration splitting](split-migrations.md):

- **DropProjection**: Phase 1 (dropped before any table modifications)
- **AddProjection**: Phase 9 (added after all table and column changes)
- **MaterializeProjection**: Phase 9 (materialized after projection is added)

This ordering ensures projections are not affected by column changes and that the table structure is finalized before projections are created.

## Model-Based Projections

Projections can also be defined in `OnModelCreating` using the fluent API, and EF.CH will generate the appropriate migration operations automatically.

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.HasProjection("prj_by_category")
        .OrderBy(x => x.Category)
        .ThenBy(x => x.CreatedAt);

    entity.HasProjection("prj_daily_totals")
        .GroupBy(x => x.Category)
        .Select(g => new
        {
            Category = g.Key,
            Total = g.Sum(x => x.Amount),
            UniqueUsers = g.UniqExact(x => x.UserId)
        });
});
```

When you run `Add-Migration`, EF.CH detects projection changes and generates `AddProjection`/`DropProjection` operations in the migration.

> **Note:** Materialization of existing data can be a heavy operation on large tables. For tables with billions of rows, consider materializing partition by partition using the `inPartition` parameter.

## See Also

- [Split Migrations](split-migrations.md) -- how projection operations are split into individual steps
- [Phase Ordering](phase-ordering.md) -- why projections are ordered in phase 9
- [Migrations Overview](overview.md) -- general migration workflow
