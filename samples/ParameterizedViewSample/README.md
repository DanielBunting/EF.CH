# Parameterized View Sample

This sample demonstrates how to use ClickHouse parameterized views with EF.CH.

## What Are Parameterized Views?

Parameterized views in ClickHouse allow you to create views with parameters that are substituted at query time. They use the `{name:Type}` syntax:

```sql
CREATE VIEW user_events_view AS
SELECT *
FROM events
WHERE user_id = {user_id:UInt64}
  AND timestamp >= {start_date:DateTime}
```

Query with parameters:
```sql
SELECT * FROM user_events_view(user_id = 123, start_date = '2024-01-01 00:00:00')
```

## Running the Sample

1. Ensure ClickHouse is running on localhost:8123
2. Run the sample:
   ```bash
   dotnet run
   ```

## What This Sample Demonstrates

1. **Creating a Parameterized View** - Via raw SQL (views aren't managed by EF migrations)

2. **Basic Queries** - Using `FromParameterizedView` with anonymous object parameters

3. **LINQ Composition** - Chaining Where, OrderBy, and Take after parameter binding

4. **Aggregation** - Using GroupBy with Sum and Count

5. **Dictionary Parameters** - For dynamic parameter scenarios

## Key Code Patterns

### Configure the Result Entity

```csharp
modelBuilder.Entity<UserEventView>(entity =>
{
    entity.HasParameterizedView("user_events_view");
});
```

### Query with Parameters

```csharp
var events = await context.FromParameterizedView<UserEventView>(
    "user_events_view",
    new { user_id = 123UL, start_date = DateTime.Today })
    .Where(e => e.EventType == "click")
    .ToListAsync();
```

### Use Dictionary for Dynamic Parameters

```csharp
var parameters = new Dictionary<string, object?>
{
    ["user_id"] = userId,
    ["start_date"] = startDate
};

var events = await context.FromParameterizedView<UserEventView>(
    "user_events_view", parameters)
    .ToListAsync();
```

## Parameter Type Mapping

| C# Type | ClickHouse Type |
|---------|-----------------|
| `ulong` | UInt64 |
| `long` | Int64 |
| `int` | Int32 |
| `DateTime` | DateTime |
| `DateOnly` | Date |
| `string` | String |
| `decimal` | Decimal |
| `bool` | UInt8 (1/0) |

## Notes

- Property names are automatically converted from PascalCase to snake_case
- String values are properly escaped
- The result entity must be configured as keyless (done automatically by `HasParameterizedView`)
