# MapTypeSample

Demonstrates using Map (Dictionary) columns in ClickHouse with EF.CH.

## What This Shows

- Defining entities with Dictionary properties
- Accessing map values by key in LINQ
- Filtering by map key existence (`ContainsKey`)
- Grouping and aggregating by map values

## LINQ to ClickHouse Translation

| LINQ | ClickHouse SQL |
|------|----------------|
| `dict["key"]` | `"dict"['key']` |
| `dict.ContainsKey("key")` | `mapContains("dict", 'key')` |
| `dict.Keys` | `mapKeys("dict")` |
| `dict.Values` | `mapValues("dict")` |

## Prerequisites

- .NET 10.0+
- ClickHouse server running on localhost:8123

## Running

```bash
dotnet run
```

## Expected Output

```
Map Type Sample
===============

Creating database and tables...
Inserting events with metadata and counters...

Inserted 5 events.

--- URLs from all events (Map key access) ---
  [page_view] /products/laptop
  [button_click] /products/laptop
  [page_view] /checkout
  [purchase] /checkout/success
  [error] /api/users

--- Page views from Google (Filter by map value) ---
  /products/laptop

--- Events with error_code (ContainsKey) ---
  [error] 500: Internal server error

--- Events with scroll depth > 50% ---
  /products/laptop: 75% scroll, 45s
  /checkout: 100% scroll, 120s

--- Events by URL ---
  /products/laptop: 2 event(s)
  /checkout: 1 event(s)
  /checkout/success: 1 event(s)
  /api/users: 1 event(s)

--- Purchase events with order details ---
  Order ORD-12345: 2 items, $1499.00 via credit_card

Done!
```

## Key Code

### Entity with Maps

```csharp
public class AnalyticsEvent
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;
    public Dictionary<string, string> Metadata { get; set; } = new();
    public Dictionary<string, int> Counters { get; set; } = new();
}
```

### Querying Maps

```csharp
// Access by key
var urls = await context.Events
    .Select(e => e.Metadata["url"])
    .ToListAsync();

// Check key existence
var errors = await context.Events
    .Where(e => e.Metadata.ContainsKey("error_code"))
    .ToListAsync();

// Filter by value
var highScroll = await context.Events
    .Where(e => e.Counters["scroll_depth"] > 50)
    .ToListAsync();
```

## Use Cases

- Flexible event metadata
- Request headers/query params
- Configuration key-value pairs
- Metrics with labels/tags

## Learn More

- [Maps Documentation](../../docs/types/maps.md)
- [Type Mappings](../../docs/types/overview.md)
