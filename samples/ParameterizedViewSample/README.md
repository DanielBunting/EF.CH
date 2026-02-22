# Parameterized View Sample

Demonstrates ClickHouse parameterized views using EF.CH.

## What This Sample Shows

1. **Create parameterized views** - `CreateParameterizedViewAsync` with typed parameters using `{name:Type}` syntax
2. **Query parameterized views** - `FromParameterizedView<T>` with anonymous objects or dictionaries, composable with further LINQ operations
3. **Drop parameterized views** - `DropParameterizedViewAsync` with optional `IF EXISTS` safety
4. **Idempotent creation** - `EnsureParameterizedViewsAsync` for startup-safe view creation

## Prerequisites

- .NET 8.0 SDK
- Docker (for Testcontainers)

## Running

```bash
dotnet run --project samples/ParameterizedViewSample/
```

## Key Concepts

### Creating Views

Parameterized views use ClickHouse's `{name:Type}` syntax for parameters:

```csharp
await context.Database.CreateParameterizedViewAsync(
    "user_events_view",
    @"SELECT * FROM events
      WHERE user_id = {user_id:UInt64}
        AND timestamp >= {start_date:DateTime}");
```

### Querying Views

Pass parameters via anonymous objects (property names are converted to snake_case):

```csharp
var events = await context.FromParameterizedView<EventView>(
        "user_events_view",
        new { UserId = 123UL, StartDate = new DateTime(2024, 1, 1) })
    .Where(e => e.EventType == "click")    // Further LINQ composition
    .OrderByDescending(e => e.Timestamp)
    .ToListAsync();
```

Or pass parameters via dictionary:

```csharp
var parameters = new Dictionary<string, object?>
{
    ["user_id"] = 123UL,
    ["start_date"] = new DateTime(2024, 1, 1)
};
var events = await context.FromParameterizedView<EventView>(
    "user_events_view", parameters).ToListAsync();
```

### View Result Entities

View result types must be configured as keyless entities with no table mapping:

```csharp
modelBuilder.Entity<EventView>(entity =>
{
    entity.HasNoKey();
    entity.ToTable((string?)null);
});
```

### Startup Pattern

```csharp
await context.Database.EnsureCreatedAsync();
await context.Database.EnsureParameterizedViewsAsync();
```
