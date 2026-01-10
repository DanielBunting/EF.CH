# Projections

Projections are table-level optimizations that store pre-sorted or pre-aggregated data alongside the main table. The query optimizer automatically selects the best projection for each query.

## Projections vs Materialized Views

| Aspect | Projections | Materialized Views |
|--------|-------------|-------------------|
| Storage | Inside the table | Separate table |
| Queryable | No (optimizer auto-selects) | Yes (via DbSet) |
| Multiple per table | Yes | N/A (one MV per target table) |
| Added via | `ALTER TABLE ... ADD PROJECTION` | `CREATE MATERIALIZED VIEW` |
| Use case | Query optimization | Separate aggregate table |

**Key insight:** If you need to query the aggregated data directly, use a materialized view. If you just want queries against the main table to be faster, use a projection.

## How They Work

```
Query: SELECT * FROM Orders WHERE CustomerId = 'ABC' ORDER BY OrderDate

                                    ┌─────────────────────────────┐
                                    │     Query Optimizer         │
                                    │                             │
                                    │  "This query orders by      │
                                    │   CustomerId, OrderDate...  │
                                    │   Projection matches!"      │
                                    └──────────────┬──────────────┘
                                                   │
                    ┌──────────────────────────────┼──────────────────────────────┐
                    │                              │                              │
                    ▼                              ▼                              ▼
        ┌───────────────────┐        ┌───────────────────┐        ┌───────────────────┐
        │   Main Table      │        │   Projection 1    │        │   Projection 2    │
        │   (ORDER BY Id)   │        │ (ORDER BY         │        │ (GROUP BY Date)   │
        │                   │        │  CustomerId,      │   ✓    │                   │
        │                   │        │  OrderDate)       │ USED   │                   │
        └───────────────────┘        └───────────────────┘        └───────────────────┘
```

The optimizer evaluates which projection (if any) best matches the query's access pattern and uses it automatically.

## Configuration Methods

### Sort-Order Projection (Auto-Named)

The projection name is automatically generated from the table name and column names.

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.OrderDate, x.Id });

    // Auto-generated name: orders__prj_ord__customer_id__order_date
    entity.HasProjection()
        .OrderBy(x => x.CustomerId)
        .ThenBy(x => x.OrderDate)
        .Build();
});
```

### Sort-Order Projection (Explicit Name)

```csharp
entity.HasProjection("prj_by_customer")
    .OrderBy(x => x.CustomerId)
    .ThenBy(x => x.OrderDate)
    .Build();
```

### Aggregation Projection (Auto-Named)

Use anonymous types for the SELECT clause. The name is generated from the table name and selected fields.

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.OrderDate, x.Id });

    // Auto-generated name: orders__prj_agg__date__total_amount__order_count
    entity.HasProjection()
        .GroupBy(o => o.OrderDate.Date)
        .Select(g => new {
            Date = g.Key,
            TotalAmount = g.Sum(o => o.Amount),
            OrderCount = g.Count()
        })
        .Build();
});
```

### Aggregation Projection (Explicit Name)

```csharp
entity.HasProjection("daily_stats")
    .GroupBy(o => o.OrderDate.Date)
    .Select(g => new {
        Date = g.Key,
        TotalAmount = g.Sum(o => o.Amount),
        OrderCount = g.Count()
    })
    .Build();
```

### ClickHouse Aggregate Functions

Beyond standard LINQ aggregates (`Sum`, `Count`, `Average`, `Min`, `Max`), you can use ClickHouse-specific aggregate functions via the `ClickHouseAggregates` class:

```csharp
using EF.CH.Extensions;

entity.HasProjection()
    .GroupBy(o => o.OrderDate.Date)
    .Select(g => new {
        Date = g.Key,
        // Approximate unique count (fast, ~2% variance)
        UniqueCustomers = ClickHouseAggregates.Uniq(g, o => o.CustomerId),
        // Exact unique count
        ExactCustomers = ClickHouseAggregates.UniqExact(g, o => o.CustomerId),
        // Value at maximum - get customer with highest order
        TopCustomer = ClickHouseAggregates.ArgMax(g, o => o.CustomerId, o => o.Amount),
        // 95th percentile
        P95Amount = ClickHouseAggregates.Quantile(g, 0.95, o => (double)o.Amount),
        // Median
        MedianAmount = ClickHouseAggregates.Median(g, o => (double)o.Amount),
        // Standard LINQ still works
        TotalAmount = g.Sum(o => o.Amount)
    })
    .Build();
```

**Available Aggregates:**

| Method | ClickHouse SQL | Description |
|--------|---------------|-------------|
| `Uniq(g, x => x.Col)` | `uniq(Col)` | Approximate unique count (~2% variance) |
| `UniqExact(g, x => x.Col)` | `uniqExact(Col)` | Exact unique count |
| `ArgMax(g, x => x.Arg, x => x.Val)` | `argMax(Arg, Val)` | Arg value at maximum Val |
| `ArgMin(g, x => x.Arg, x => x.Val)` | `argMin(Arg, Val)` | Arg value at minimum Val |
| `AnyValue(g, x => x.Col)` | `any(Col)` | Any value from group |
| `AnyLastValue(g, x => x.Col)` | `anyLast(Col)` | Last value from group |
| `Quantile(g, level, x => x.Col)` | `quantile(level)(Col)` | Percentile (0-1) |
| `Median(g, x => x.Col)` | `median(Col)` | 50th percentile |
| `StddevPop(g, x => x.Col)` | `stddevPop(Col)` | Population std dev |
| `StddevSamp(g, x => x.Col)` | `stddevSamp(Col)` | Sample std dev |
| `VarPop(g, x => x.Col)` | `varPop(Col)` | Population variance |
| `VarSamp(g, x => x.Col)` | `varSamp(Col)` | Sample variance |
| `GroupArray(g, x => x.Col)` | `groupArray(Col)` | Collect into array |
| `GroupArray(g, n, x => x.Col)` | `groupArray(n)(Col)` | First n values |
| `GroupUniqArray(g, x => x.Col)` | `groupUniqArray(Col)` | Unique values array |
| `TopK(g, k, x => x.Col)` | `topK(k)(Col)` | Top k frequent values |

### Raw SQL (Escape Hatch)

For complex projections that can't be expressed in LINQ. Explicit name is required.

```csharp
entity.HasProjection(
    "prj_by_region",
    "SELECT * ORDER BY (\"Region\", \"OrderDate\")");
```

### Remove Projection

```csharp
entity.RemoveProjection("orders__prj_ord__customer_id__order_date");
```

## Naming Convention

Auto-generated names follow the pattern: `{table_name}__prj_{type}__{fields}` with snake_case and double underscores as separators.

| Projection Type | Pattern | Example |
|-----------------|---------|---------|
| Sort-order | `{table}__prj_ord__{col1}__{col2}` | `orders__prj_ord__customer_id__order_date` |
| Aggregation | `{table}__prj_agg__{field1}__{field2}` | `orders__prj_agg__date__total_amount` |

This naming convention:
- Uses snake_case for ClickHouse compatibility
- Uses `__` (double underscore) as separator between logical components
- Includes `prj` to identify projection names
- Is parseable: split on `__` gives `[table, prj_type, field1, field2, ...]`

## Migration Builder Extensions

For adding projections to existing tables via migrations:

```csharp
public partial class AddOrderProjections : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        // Add projection
        migrationBuilder.AddProjection(
            table: "Orders",
            name: "prj_by_status",
            selectSql: "SELECT * ORDER BY (\"Status\", \"OrderDate\")",
            materialize: true);

        // Materialize for specific partition only
        migrationBuilder.MaterializeProjection(
            table: "Orders",
            name: "prj_by_status",
            inPartition: "202401");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropProjection(
            table: "Orders",
            name: "prj_by_status");
    }
}
```

## Generated DDL

### Sort-Order Projection

```sql
-- From: entity.HasProjection().OrderBy(x => x.CustomerId).ThenBy(x => x.OrderDate).Build()
ALTER TABLE "orders" ADD PROJECTION "orders__prj_ord__customer_id__order_date" (
    SELECT * ORDER BY ("CustomerId", "OrderDate")
);
ALTER TABLE "orders" MATERIALIZE PROJECTION "orders__prj_ord__customer_id__order_date";
```

### Aggregation Projection

```sql
-- From: entity.HasProjection().GroupBy(...).Select(...).Build()
ALTER TABLE "orders" ADD PROJECTION "orders__prj_agg__date__total_amount__order_count" (
    SELECT
        toDate("OrderDate") AS "Date",
        sum("Amount") AS "TotalAmount",
        count() AS "OrderCount"
    GROUP BY "Date"
);
ALTER TABLE "orders" MATERIALIZE PROJECTION "orders__prj_agg__date__total_amount__order_count";
```

## Complete Example

```csharp
// Entity
public class Order
{
    public Guid Id { get; set; }
    public DateTime OrderDate { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

// DbContext
public class MyDbContext : DbContext
{
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });
            entity.HasPartitionByMonth(x => x.OrderDate);

            // Sort-order projection for customer queries
            // Auto-name: orders__prj_ord__customer_id__order_date
            entity.HasProjection()
                .OrderBy(x => x.CustomerId)
                .ThenBy(x => x.OrderDate)
                .Build();

            // Sort-order projection for region queries
            // Auto-name: orders__prj_ord__region__order_date
            entity.HasProjection()
                .OrderBy(x => x.Region)
                .ThenBy(x => x.OrderDate)
                .Build();

            // Aggregation projection for daily summaries
            // Explicit name for clarity
            entity.HasProjection("daily_sales")
                .GroupBy(o => o.OrderDate.Date)
                .Select(g => new {
                    Date = g.Key,
                    TotalAmount = g.Sum(o => o.Amount),
                    OrderCount = g.Count()
                })
                .Build();
        });
    }
}
```

## Limitations

- **No ASC/DESC support** - ClickHouse projections always sort ascending. Descending order is not supported.

- **FINAL compatibility** - Queries using `FINAL` won't use projections.

- **Lightweight delete restrictions** - Tables with projections may have restrictions when using lightweight deletes.

- **Storage overhead** - Each projection stores a copy of the data in the specified order, increasing disk usage.

- **MATERIALIZE cost** - Materializing projections on large existing tables can be slow and resource-intensive. Consider using `materialize: false` and running `MATERIALIZE PROJECTION` during off-peak hours.

- **Not queryable** - Unlike materialized views, you cannot query projections directly. They are only used by the optimizer automatically.

## Best Practices

1. **Use fluent builder API** - Provides type safety and automatic naming from table and column names.

2. **Let names auto-generate** - The `{Table}_{prjtype}_{fields}` pattern is descriptive and consistent.

3. **Match query patterns** - Create projections that match your most common query ORDER BY and GROUP BY patterns.

4. **Consider storage trade-offs** - Each projection duplicates data. Only create projections for frequently-used query patterns.

5. **Test optimizer selection** - Use `EXPLAIN` in ClickHouse to verify your projections are being used:
   ```sql
   EXPLAIN SELECT * FROM Orders WHERE CustomerId = 'ABC' ORDER BY OrderDate;
   ```

6. **Materialize strategically** - For large tables, consider `materialize: false` during initial creation, then materialize partition-by-partition during maintenance windows.
