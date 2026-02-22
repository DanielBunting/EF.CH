# Parameterized Views

Parameterized views are ClickHouse views that accept parameters at query time. They act as reusable query templates with typed placeholders substituted when the view is queried.

## Creating a View

### Runtime API

Create a parameterized view at runtime using `CreateParameterizedViewAsync` on the `DatabaseFacade`.

```csharp
await context.Database.CreateParameterizedViewAsync(
    "user_events_view",
    @"SELECT event_id, event_type, user_id, timestamp
      FROM events
      WHERE user_id = {user_id:UInt64}
        AND timestamp >= {start_date:DateTime}"
);
```

Generated DDL:

```sql
CREATE VIEW "user_events_view" AS
SELECT event_id, event_type, user_id, timestamp
FROM events
WHERE user_id = {user_id:UInt64}
  AND timestamp >= {start_date:DateTime}
```

The `{name:Type}` syntax is ClickHouse's native parameter placeholder format. Common types include `UInt64`, `String`, `DateTime`, `Date`, `UUID`, `Float64`.

### Idempotent Creation

Pass `ifNotExists: true` to avoid errors if the view already exists.

```csharp
await context.Database.CreateParameterizedViewAsync(
    "user_events_view",
    "SELECT * FROM events WHERE user_id = {user_id:UInt64}",
    ifNotExists: true
);
```

## Dropping a View

```csharp
await context.Database.DropParameterizedViewAsync("user_events_view");
```

The `ifExists` parameter defaults to `true`, so dropping a non-existent view does not throw.

```csharp
// Explicit control
await context.Database.DropParameterizedViewAsync("user_events_view", ifExists: false);
```

## Querying a View

Query a parameterized view by providing an anonymous object with properties matching the parameter names.

```csharp
public class EventView
{
    public long EventId { get; set; }
    public string EventType { get; set; }
    public ulong UserId { get; set; }
    public DateTime Timestamp { get; set; }
}
```

Register the result entity as keyless in `OnModelCreating`:

```csharp
modelBuilder.Entity<EventView>().HasNoKey();
```

Execute the query:

```csharp
var events = await context.FromParameterizedView<EventView>(
    "user_events_view",
    new { user_id = 123UL, start_date = new DateTime(2024, 1, 1) }
).ToListAsync();
```

Generated SQL:

```sql
SELECT * FROM "user_events_view"(user_id = 123, start_date = '2024-01-01 00:00:00')
```

### Dictionary Parameters

You can also pass parameters as a dictionary for dynamic parameter sets.

```csharp
var parameters = new Dictionary<string, object?>
{
    ["user_id"] = 123UL,
    ["start_date"] = new DateTime(2024, 1, 1)
};

var events = await context.FromParameterizedView<EventView>(
    "user_events_view",
    parameters
).ToListAsync();
```

### Composing with LINQ

The result of `FromParameterizedView` is an `IQueryable<T>` that supports further LINQ composition.

```csharp
var clickEvents = await context.FromParameterizedView<EventView>(
    "user_events_view",
    new { user_id = 123UL, start_date = new DateTime(2024, 1, 1) }
)
.Where(e => e.EventType == "click")
.OrderByDescending(e => e.Timestamp)
.Take(100)
.ToListAsync();
```

## Bulk View Management

Use `EnsureParameterizedViewsAsync` to create all parameterized views configured in the model at startup.

```csharp
await context.Database.EnsureParameterizedViewsAsync();
```

This scans the EF Core model for entities configured via `AsParameterizedView` and issues `CREATE VIEW IF NOT EXISTS` for each one.

## Parameter Type Mapping

Properties on the anonymous object are automatically converted to ClickHouse literals.

| C# Type | ClickHouse Literal |
|---------|-------------------|
| `string` | `'value'` (escaped) |
| `int`, `long`, `ulong` | Numeric literal |
| `DateTime` | `'yyyy-MM-dd HH:mm:ss'` |
| `DateTimeOffset` | `'yyyy-MM-dd HH:mm:ss'` (UTC) |
| `DateOnly` | `'yyyy-MM-dd'` |
| `bool` | `1` or `0` |
| `Guid` | `'guid-value'` |
| `decimal`, `double`, `float` | Invariant culture numeric |
| `Enum` | Integer value |
| `null` | `NULL` |

> **Note:** Property names on the anonymous object are automatically converted from PascalCase to snake_case to match ClickHouse parameter naming conventions. `UserId` becomes `user_id`.

## See Also

- [Parameterized View Migration Operations](../migrations/parameterized-view-operations.md) -- managing views in migrations
- [Materialized Views](materialized-views.md) -- views that store data
- [Table Functions](table-functions.md) -- ad-hoc data source access
