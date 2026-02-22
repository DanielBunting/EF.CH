# Temporary Tables

Session-scoped temporary tables for multi-step query workflows. Temp tables use the Memory engine, are automatically assigned unique names, and are dropped when their handle is disposed. Use them to materialize intermediate results, stage data for complex joins, or break apart queries that are too complex for a single statement.

All extension methods live in the `EF.CH.Extensions` namespace.

```csharp
using EF.CH.Extensions;
```

## Create from Query Results

Materialize a query's results into a temp table in a single call:

```csharp
await using var tempTable = await context.Events
    .Where(e => e.Timestamp >= startOfDay)
    .ToTempTableAsync(context);

var results = await tempTable.Query()
    .Where(e => e.Amount > 100)
    .OrderByDescending(e => e.Amount)
    .ToListAsync();
```

Generated SQL (creation):

```sql
CREATE TABLE "_temp_Events_a1b2c3d4" ENGINE = Memory AS
SELECT ...
FROM "Events" AS e
WHERE e."Timestamp" >= '2024-01-15 00:00:00'
```

The `await using` ensures the temp table is dropped when the variable goes out of scope.

## Query a Temp Table

The `Query()` method returns a fully composable `IQueryable<T>`:

```csharp
await using var tempTable = await query.ToTempTableAsync(context);

// Filter
var filtered = await tempTable.Query()
    .Where(e => e.EventType == "click")
    .ToListAsync();

// Aggregate
var stats = await tempTable.Query()
    .GroupBy(e => e.EventType)
    .Select(g => new { Type = g.Key, Count = g.Count() })
    .ToListAsync();
```

## Create Empty, Then Populate

Create a temp table with the schema of an entity type, then insert data separately:

```csharp
await using var tempTable = await context.CreateTempTableAsync<Event>();

// Insert from a query (server-side)
await context.Events
    .Where(e => e.EventType == "click")
    .InsertIntoTempTableAsync(tempTable);

// Or insert entities directly (client-side)
await tempTable.InsertAsync(new[]
{
    new Event { Id = Guid.NewGuid(), EventType = "manual", Amount = 42 }
});

var results = await tempTable.Query().ToListAsync();
```

Server-side `InsertIntoTempTableAsync` generates `INSERT INTO ... SELECT` and is preferred for large datasets. Client-side `InsertAsync` generates `INSERT INTO ... VALUES` and is useful for small collections or programmatically generated data.

## Custom Table Name

By default, temp tables are assigned unique names. Override this if you need a predictable name:

```csharp
await using var tempTable = await context.CreateTempTableAsync<Event>(
    tableName: "staging_events");
```

## Scoped Management

`BeginTempTableScope` manages multiple temp tables with LIFO disposal order. All tables created within the scope are dropped when the scope is disposed:

```csharp
await using var scope = context.BeginTempTableScope();

var clickEvents = await scope.CreateFromQueryAsync(
    context.Events.Where(e => e.EventType == "click"));

var purchaseEvents = await scope.CreateFromQueryAsync(
    context.Events.Where(e => e.EventType == "purchase"));

// Use both temp tables
var clickCount = await clickEvents.Query().CountAsync();
var purchaseCount = await purchaseEvents.Query().CountAsync();

// Both tables are dropped when `scope` is disposed
```

The scope's `CreateAsync` and `CreateFromQueryAsync` methods mirror the top-level extensions but track handles internally for cleanup.

## Multi-Step Workflows

Temp tables enable complex analytics that would be impractical in a single query:

```csharp
// Step 1: Identify high-value users
await using var highValueUsers = await context.Orders
    .GroupBy(o => o.UserId)
    .Where(g => g.Sum(o => o.Amount) > 10_000)
    .Select(g => new UserSummary { UserId = g.Key, Total = g.Sum(o => o.Amount) })
    .ToTempTableAsync(context);

// Step 2: Get recent events for those users
await using var recentEvents = await context.Events
    .Where(e => e.Timestamp >= startOfWeek)
    .ToTempTableAsync(context);

// Step 3: Query across both temp tables using raw SQL
var results = await context.Database
    .SqlQueryRaw<UserEventSummary>(@$"
        SELECT u.UserId, u.Total, count() as EventCount
        FROM {highValueUsers.QuotedTableName} AS u
        JOIN {recentEvents.QuotedTableName} AS e ON u.UserId = e.UserId
        GROUP BY u.UserId, u.Total
        ORDER BY u.Total DESC")
    .ToListAsync();
```

## TempTableHandle Properties

| Property | Type | Description |
|----------|------|-------------|
| `TableName` | `string` | The unquoted table name |
| `QuotedTableName` | `string` | The quoted table name, safe for SQL interpolation |

## Lifetime

Temp tables are bound to the ClickHouse session (connection). They are dropped in two scenarios:

1. **Explicit disposal** via `await using` or calling `DisposeAsync()` directly.
2. **Connection close** -- if the connection is closed or returned to the pool before disposal, the table is lost.

Always use `await using` to ensure deterministic cleanup.

## See Also

- [INSERT...SELECT](insert-select.md) -- Server-side data movement into temp tables
- [Bulk Insert](bulk-insert.md) -- Populate temp tables with large datasets
- [Export](export.md) -- Export temp table contents to CSV, JSON, or Parquet
