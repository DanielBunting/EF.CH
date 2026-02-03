# Parameterized Views

Parameterized views in ClickHouse allow you to create views with parameters that are substituted at query time. Unlike regular views where the query is fixed, parameterized views accept runtime arguments, making them ideal for common filtered queries.

## How They Work

1. You create a view with parameter placeholders using `{name:Type}` syntax
2. When querying, you provide parameter values in the call: `view_name(param=value, ...)`
3. ClickHouse substitutes the values and executes the query

```
View Definition                          Query Call
     |                                        |
     |  {user_id:UInt64}                     |  user_id = 123
     |  {start_date:DateTime}                |  start_date = '2024-01-01'
     └──────────────────────────────────────►|
                                              |
                               SELECT * FROM view(user_id = 123, start_date = '...')
```

## ClickHouse Syntax

### View Definition

```sql
CREATE VIEW user_events_view AS
SELECT event_id, event_type, timestamp, user_id
FROM events
WHERE user_id = {user_id:UInt64}
  AND timestamp >= {start_date:DateTime}
  AND timestamp < {end_date:DateTime}
```

### Query Syntax

```sql
SELECT * FROM user_events_view(user_id = 123, start_date = '2024-01-01 00:00:00', end_date = '2024-02-01 00:00:00')
WHERE event_type = 'click'
ORDER BY timestamp DESC
```

## EF.CH Configuration

### 1. Define the Result Entity

Create a class representing the view's output schema:

```csharp
public class UserEventView
{
    public ulong EventId { get; set; }
    public string EventType { get; set; } = string.Empty;
    public ulong UserId { get; set; }
    public DateTime Timestamp { get; set; }
}
```

### 2. Configure in DbContext

Use `HasParameterizedView` to mark the entity as a parameterized view result type:

```csharp
public class AppDbContext : DbContext
{
    public DbSet<UserEventView> UserEventViews => Set<UserEventView>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<UserEventView>(entity =>
        {
            entity.HasParameterizedView("user_events_view");
        });
    }
}
```

This configures the entity as:
- Keyless (parameterized views don't have primary keys)
- Marked with annotations for documentation/scaffolding purposes

### 3. Create the View (Migration)

Use `CreateParameterizedView` in a migration to create the view:

```csharp
public partial class AddUserEventsView : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateParameterizedView(
            viewName: "user_events_view",
            selectSql: @"
                SELECT event_id, event_type, timestamp, user_id
                FROM events
                WHERE user_id = {user_id:UInt64}
                  AND timestamp >= {start_date:DateTime}
            ",
            parameters: new[]
            {
                ("user_id", "UInt64"),
                ("start_date", "DateTime")
            });
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropParameterizedView("user_events_view");
    }
}
```

## Querying Parameterized Views

### Basic Query

Use `FromParameterizedView` to query with parameters:

```csharp
var events = await context.FromParameterizedView<UserEventView>(
    "user_events_view",
    new { user_id = 123UL, start_date = new DateTime(2024, 1, 1) })
    .ToListAsync();
```

### Strongly-Typed View Access (Recommended)

For type-safe access, use `ClickHouseParameterizedView<T>` in your DbContext:

```csharp
using EF.CH.ParameterizedViews;

public class AnalyticsDbContext : DbContext
{
    // Strongly-typed view accessor
    private ClickHouseParameterizedView<UserEventView>? _userEventsView;
    public ClickHouseParameterizedView<UserEventView> UserEventsView
        => _userEventsView ??= new ClickHouseParameterizedView<UserEventView>(this);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<UserEventView>(entity =>
        {
            entity.HasParameterizedView("user_events_view");
        });
    }
}

// Usage
var events = await context.UserEventsView
    .Query(new { user_id = 123UL, start_date = DateTime.Today.AddDays(-7) })
    .Where(e => e.EventType == "purchase")
    .OrderByDescending(e => e.Timestamp)
    .ToListAsync();

// Convenience methods
var count = await context.UserEventsView.CountAsync(new { user_id = 123UL, ... });
var first = await context.UserEventsView.FirstOrDefaultAsync(new { user_id = 123UL, ... });
var exists = await context.UserEventsView.AnyAsync(new { user_id = 123UL, ... });
```

### With LINQ Composition

You can chain LINQ operations after binding parameters:

```csharp
var clickEvents = await context.FromParameterizedView<UserEventView>(
    "user_events_view",
    new { user_id = 123UL, start_date = DateTime.Today.AddDays(-7) })
    .Where(e => e.EventType == "click")
    .OrderByDescending(e => e.Timestamp)
    .Take(100)
    .ToListAsync();
```

### With Dictionary Parameters

For dynamic parameter scenarios, use a dictionary:

```csharp
var parameters = new Dictionary<string, object?>
{
    ["user_id"] = userId,
    ["start_date"] = startDate
};

var events = await context.FromParameterizedView<UserEventView>(
    "user_events_view",
    parameters)
    .ToListAsync();
```

### Aggregation Queries

LINQ GroupBy works after parameter binding:

```csharp
var dailyCounts = await context.FromParameterizedView<UserEventView>(
    "user_events_view",
    new { user_id = 123UL, start_date = DateTime.Today.AddDays(-30) })
    .GroupBy(e => e.Timestamp.Date)
    .Select(g => new { Date = g.Key, Count = g.Count() })
    .OrderBy(x => x.Date)
    .ToListAsync();
```

## Fluent View Configuration

For a fully type-safe approach, you can configure parameterized views with a fluent builder that generates the CREATE VIEW DDL automatically:

### Configuration

```csharp
public class AnalyticsDbContext : DbContext
{
    public DbSet<Event> Events => Set<Event>();
    public DbSet<UserEventView> UserEventViews => Set<UserEventView>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Source entity (with explicit column names)
        modelBuilder.Entity<Event>(entity =>
        {
            entity.ToTable("events");
            entity.HasKey(e => e.EventId);
            entity.UseMergeTree(e => new { e.UserId, e.Timestamp });
            entity.Property(e => e.EventId).HasColumnName("event_id");
            entity.Property(e => e.EventType).HasColumnName("event_type");
            entity.Property(e => e.UserId).HasColumnName("user_id");
            entity.Property(e => e.Timestamp).HasColumnName("timestamp");
            entity.Property(e => e.Value).HasColumnName("value");
        });

        // Fluent view configuration
        modelBuilder.Entity<UserEventView>(entity =>
        {
            entity.AsParameterizedView<UserEventView, Event>(cfg => cfg
                .HasName("user_events_view")
                .FromTable()
                .Select(e => new UserEventView
                {
                    EventId = e.EventId,
                    EventType = e.EventType,
                    UserId = e.UserId,
                    Timestamp = e.Timestamp,
                    Value = e.Value
                })
                .Parameter<ulong>("user_id")
                .Parameter<DateTime>("start_date")
                .Where((e, p) => e.UserId == p.Get<ulong>("user_id"))
                .Where((e, p) => e.Timestamp >= p.Get<DateTime>("start_date")));
        });
    }
}
```

### Creating Views at Runtime

Use `EnsureParameterizedViewsAsync` to create all configured views:

```csharp
// On application startup
await context.Database.EnsureCreatedAsync();  // Create tables
await context.Database.EnsureParameterizedViewsAsync();  // Create views

// Or create a specific view
await context.Database.EnsureParameterizedViewAsync<UserEventView>();
```

### Inspecting Generated SQL

```csharp
var sql = context.Database.GetParameterizedViewSql<UserEventView>();
Console.WriteLine(sql);
// Output:
// CREATE VIEW "user_events_view" AS
// SELECT "event_id" AS "EventId",
//        "event_type" AS "EventType",
//        "user_id" AS "UserId",
//        "timestamp" AS "Timestamp",
//        "value" AS "Value"
// FROM "events"
// WHERE ("user_id" = {user_id:UInt64})
//   AND ("timestamp" >= {start_date:DateTime})
```

## Type Mappings

### CLR to ClickHouse Parameter Types

| CLR Type | ClickHouse Type | Notes |
|----------|-----------------|-------|
| `bool` | `UInt8` | Formatted as 1/0 |
| `byte` | `UInt8` | |
| `sbyte` | `Int8` | |
| `short` | `Int16` | |
| `ushort` | `UInt16` | |
| `int` | `Int32` | |
| `uint` | `UInt32` | |
| `long` | `Int64` | |
| `ulong` | `UInt64` | |
| `float` | `Float32` | |
| `double` | `Float64` | |
| `decimal` | `Decimal(18, 4)` | Invariant culture |
| `string` | `String` | Escaped with backslash |
| `DateTime` | `DateTime` | Format: yyyy-MM-dd HH:mm:ss |
| `DateTimeOffset` | `DateTime` | UTC converted |
| `DateOnly` | `Date` | Format: yyyy-MM-dd |
| `TimeOnly` | `String` | Format: HH:mm:ss |
| `Guid` | `UUID` | String format |
| `byte[]` | Hex string | Via unhex() |
| `enum` | Int64 | Underlying value |

### Parameter Name Conversion

Property names are converted from PascalCase to snake_case:

| C# Property | Parameter Name |
|-------------|----------------|
| `UserId` | `user_id` |
| `StartDate` | `start_date` |
| `XMLParser` | `x_m_l_parser` |

If your parameters are already snake_case, they remain unchanged.

## Complete Example

### Entities

```csharp
// Source table entity
public class Event
{
    public ulong EventId { get; set; }
    public string EventType { get; set; } = string.Empty;
    public ulong UserId { get; set; }
    public DateTime Timestamp { get; set; }
    public decimal Value { get; set; }
}

// Parameterized view result entity
public class UserEventView
{
    public ulong EventId { get; set; }
    public string EventType { get; set; } = string.Empty;
    public ulong UserId { get; set; }
    public DateTime Timestamp { get; set; }
    public decimal Value { get; set; }
}
```

### DbContext with Strongly-Typed Access

```csharp
using EF.CH.ParameterizedViews;

public class AnalyticsDbContext : DbContext
{
    public DbSet<Event> Events => Set<Event>();
    public DbSet<UserEventView> UserEventViews => Set<UserEventView>();

    // Strongly-typed view accessor
    private ClickHouseParameterizedView<UserEventView>? _userEventsView;
    public ClickHouseParameterizedView<UserEventView> UserEventsView
        => _userEventsView ??= new ClickHouseParameterizedView<UserEventView>(this);

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=analytics");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Source table
        modelBuilder.Entity<Event>(entity =>
        {
            entity.HasKey(e => e.EventId);
            entity.UseMergeTree(x => new { x.UserId, x.Timestamp });
            entity.HasPartitionByMonth(x => x.Timestamp);
        });

        // Parameterized view result
        modelBuilder.Entity<UserEventView>(entity =>
        {
            entity.HasParameterizedView("user_events_view");
        });
    }
}
```

### Migration

```csharp
public partial class CreateUserEventsView : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateParameterizedView(
            viewName: "user_events_view",
            selectSql: @"
                SELECT event_id AS EventId,
                       event_type AS EventType,
                       user_id AS UserId,
                       timestamp AS Timestamp,
                       value AS Value
                FROM events
                WHERE user_id = {user_id:UInt64}
                  AND timestamp >= {start_date:DateTime}
                  AND timestamp < {end_date:DateTime}
            ",
            parameters: new[]
            {
                ("user_id", "UInt64"),
                ("start_date", "DateTime"),
                ("end_date", "DateTime")
            });
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropParameterizedView("user_events_view");
    }
}
```

### Usage

```csharp
await using var context = new AnalyticsDbContext();

// Get last week's events for a user using strongly-typed accessor
var events = await context.UserEventsView
    .Query(new
    {
        user_id = 12345UL,
        start_date = DateTime.Today.AddDays(-7),
        end_date = DateTime.Today
    })
    .Where(e => e.EventType == "purchase")
    .OrderByDescending(e => e.Timestamp)
    .ToListAsync();

// Use convenience methods
var count = await context.UserEventsView.CountAsync(new
{
    user_id = 12345UL,
    start_date = DateTime.Today.AddMonths(-1),
    end_date = DateTime.Today
});

// Or use the traditional FromParameterizedView method
var summary = await context.FromParameterizedView<UserEventView>(
    "user_events_view",
    new
    {
        user_id = 12345UL,
        start_date = DateTime.Today.AddMonths(-1),
        end_date = DateTime.Today
    })
    .GroupBy(e => e.EventType)
    .Select(g => new
    {
        EventType = g.Key,
        Count = g.Count(),
        TotalValue = g.Sum(e => e.Value)
    })
    .ToListAsync();
```

## Limitations

1. **Fluent Configuration Requires Column Mappings**: When using `AsParameterizedView<TView, TSource>()`, ensure the source entity has explicit column name mappings if your database uses snake_case columns.

2. **Parameter Escaping**: String parameters are escaped with backslash. For complex strings with unusual characters, test carefully.

3. **NULL Handling**: NULL parameters require special handling in the view definition. ClickHouse NULL comparisons follow SQL semantics where `NULL = NULL` is NULL (not true).

4. **No Nested Parameters**: Parameters cannot be used in nested subqueries within the view definition.

## Best Practices

1. **Use Appropriate Types**: Match CLR types to ClickHouse types. Use `ulong` for UInt64, `long` for Int64, etc.

2. **Index Your Source Tables**: Ensure the source table has appropriate ORDER BY and indexes for filtered columns used in parameters.

3. **Partition Alignment**: If your view filters by date, ensure the source table is partitioned accordingly for efficient pruning.

4. **Parameter Naming**: Use snake_case in view definitions to match the automatic conversion from C# property names.

5. **Avoid Wide Result Sets**: If the view could return large amounts of data, add reasonable defaults or require date range parameters.

6. **Use Strongly-Typed Accessors**: Prefer `ClickHouseParameterizedView<T>` over raw `FromParameterizedView` for better IntelliSense and type safety.

## See Also

- [Materialized Views](materialized-views.md) - For INSERT-triggered aggregation
- [Query Modifiers](query-modifiers.md) - For FINAL, SAMPLE, PREWHERE hints
- [ClickHouse Views Documentation](https://clickhouse.com/docs/en/sql-reference/statements/create/view)
