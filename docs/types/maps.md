# Map Types

Maps in ClickHouse store key-value pairs. EF.CH maps .NET `Dictionary<K, V>` to ClickHouse `Map(K, V)`.

## Type Mappings

| .NET Type | ClickHouse Type |
|-----------|-----------------|
| `Dictionary<K, V>` | `Map(K, V)` |
| `IDictionary<K, V>` | `Map(K, V)` |

## Entity Definition

```csharp
public class Event
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;

    // Map types
    public Dictionary<string, string> Metadata { get; set; } = new();  // Map(String, String)
    public Dictionary<string, int> Counters { get; set; } = new();     // Map(String, Int32)
    public Dictionary<int, string> ErrorCodes { get; set; } = new();   // Map(Int32, String)
}
```

**Important:** Initialize dictionaries to avoid null reference issues:

```csharp
public Dictionary<string, string> Metadata { get; set; } = new();  // Good
public Dictionary<string, string>? Metadata { get; set; }          // Nullable
```

## Configuration

Maps work without special configuration:

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });
    // Map properties just work
});
```

## LINQ Operations

### Key Access → bracket notation

```csharp
// Get a specific metadata value
var userAgents = await context.Events
    .Where(e => e.EventType == "request")
    .Select(e => new { e.Id, UserAgent = e.Metadata["user_agent"] })
    .ToListAsync();
// SQL: ... "Metadata"['user_agent']
```

### ContainsKey → mapContains()

```csharp
// Find events with a specific metadata key
var withReferrer = await context.Events
    .Where(e => e.Metadata.ContainsKey("referrer"))
    .ToListAsync();
// SQL: ... WHERE mapContains("Metadata", 'referrer')
```

### Keys → mapKeys()

```csharp
// Get all metadata keys
var keys = await context.Events
    .Select(e => new { e.Id, Keys = e.Metadata.Keys })
    .ToListAsync();
// SQL: ... mapKeys("Metadata")
```

### Values → mapValues()

```csharp
// Get all metadata values
var values = await context.Events
    .Select(e => new { e.Id, Values = e.Metadata.Values })
    .ToListAsync();
// SQL: ... mapValues("Metadata")
```

## Inserting Data

```csharp
context.Events.Add(new Event
{
    Id = Guid.NewGuid(),
    Timestamp = DateTime.UtcNow,
    EventType = "page_view",
    Metadata = new Dictionary<string, string>
    {
        ["url"] = "/products/123",
        ["referrer"] = "https://google.com",
        ["user_agent"] = "Mozilla/5.0..."
    },
    Counters = new Dictionary<string, int>
    {
        ["scroll_depth"] = 75,
        ["time_on_page"] = 120
    }
});
await context.SaveChangesAsync();
```

## Querying Examples

### Filter by Map Value

```csharp
// Events from a specific URL
var pageViews = await context.Events
    .Where(e => e.Metadata["url"] == "/products/123")
    .ToListAsync();
```

### Filter by Key Existence

```csharp
// Events with error information
var errors = await context.Events
    .Where(e => e.Metadata.ContainsKey("error"))
    .ToListAsync();
```

### Combine Conditions

```csharp
// High scroll depth from Google
var engaged = await context.Events
    .Where(e => e.Metadata.ContainsKey("referrer"))
    .Where(e => e.Metadata["referrer"].Contains("google"))
    .Where(e => e.Counters["scroll_depth"] > 50)
    .ToListAsync();
```

## Generated DDL

```csharp
public class Event
{
    public Guid Id { get; set; }
    public Dictionary<string, string> Metadata { get; set; } = new();
    public Dictionary<string, int> Counters { get; set; } = new();
}
```

Generates:

```sql
CREATE TABLE "Events" (
    "Id" UUID NOT NULL,
    "Metadata" Map(String, String) NOT NULL,
    "Counters" Map(String, Int32) NOT NULL
)
ENGINE = MergeTree
ORDER BY ("Id")
```

## Supported Key/Value Types

### Keys

Keys must be comparable types:
- `string` (most common)
- `int`, `long`, `short`, etc.
- `Guid`
- `DateTime`

### Values

Values can be any supported ClickHouse type:
- Primitives (`string`, `int`, `bool`, etc.)
- `DateTime`, `DateOnly`
- `Guid`
- Arrays (nested: `Map(String, Array(Int32))`)

## Scaffolding

When reverse-engineering a ClickHouse database:

| ClickHouse Type | Generated .NET Type |
|-----------------|---------------------|
| `Map(String, String)` | `Dictionary<string, string>` |
| `Map(String, Int32)` | `Dictionary<string, int>` |
| `Map(Int32, String)` | `Dictionary<int, string>` |

## Limitations

- **No Count in LINQ**: `Metadata.Count` may not translate
- **Key Must Exist**: Accessing a non-existent key returns default value, not exception
- **No Complex Predicates**: Can't do `e.Metadata.Any(kv => kv.Value == "x")` in LINQ

## Best Practices

### Use String Keys

```csharp
// Most flexible and common
public Dictionary<string, string> Metadata { get; set; } = new();
```

### Initialize Empty

```csharp
// Good: Non-null default
public Dictionary<string, string> Metadata { get; set; } = new();

// Avoid: Null default
public Dictionary<string, string> Metadata { get; set; } = null!;
```

### Consider Alternatives

Maps are great for:
- Variable metadata fields
- Configuration key-value pairs
- Sparse data with many optional fields

Consider regular columns for:
- Always-present fields
- Fields you frequently filter on
- Fields that need indexing

## Use Cases

### Request Logging

```csharp
public class ApiRequest
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Endpoint { get; set; } = string.Empty;
    public Dictionary<string, string> Headers { get; set; } = new();
    public Dictionary<string, string> QueryParams { get; set; } = new();
}
```

### Feature Flags

```csharp
public class UserSession
{
    public Guid UserId { get; set; }
    public Dictionary<string, bool> FeatureFlags { get; set; } = new();
    public Dictionary<string, int> AbTestVariants { get; set; } = new();
}
```

### Metrics with Labels

```csharp
public class Metric
{
    public DateTime Timestamp { get; set; }
    public string MetricName { get; set; } = string.Empty;
    public double Value { get; set; }
    public Dictionary<string, string> Labels { get; set; } = new();
}
```

## See Also

- [Type Mappings Overview](overview.md)
- [Arrays](arrays.md) - For list data
- [Nested Types](nested.md) - For structured data
