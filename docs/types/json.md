# JSON Types

ClickHouse has native JSON support for semi-structured data. EF.CH maps `System.Text.Json` types to ClickHouse's `JSON` type.

## Type Mappings

| .NET Type | ClickHouse Type |
|-----------|-----------------|
| `JsonElement` | `JSON` |
| `JsonDocument` | `JSON` |

## Entity Definition

```csharp
using System.Text.Json;

public class Event
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;
    public JsonElement Payload { get; set; }        // Semi-structured data
    public JsonElement? Metadata { get; set; }      // Nullable JSON
}
```

## Configuration

JSON types work without special configuration:

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });
    // JSON properties just work
});
```

## Generated DDL

```sql
CREATE TABLE "Events" (
    "Id" UUID NOT NULL,
    "Timestamp" DateTime64(3) NOT NULL,
    "EventType" String NOT NULL,
    "Payload" JSON NOT NULL,
    "Metadata" Nullable(JSON)
)
ENGINE = MergeTree
ORDER BY ("Timestamp", "Id")
```

## Inserting Data

### From Anonymous Object

```csharp
var payload = JsonSerializer.SerializeToElement(new
{
    userId = "user-123",
    action = "click",
    properties = new { buttonId = "submit", page = "/checkout" }
});

context.Events.Add(new Event
{
    Id = Guid.NewGuid(),
    Timestamp = DateTime.UtcNow,
    EventType = "user_interaction",
    Payload = payload
});
await context.SaveChangesAsync();
```

### From Dictionary

```csharp
var data = new Dictionary<string, object>
{
    ["level"] = "error",
    ["message"] = "Connection timeout",
    ["details"] = new { host = "db.example.com", port = 5432 }
};

var payload = JsonSerializer.SerializeToElement(data);
```

### From JSON String

```csharp
var jsonString = """{"key": "value", "count": 42}""";
var payload = JsonDocument.Parse(jsonString).RootElement;
```

## Querying

### Basic Queries

JSON columns can be retrieved in queries:

```csharp
var events = await context.Events
    .Where(e => e.EventType == "user_interaction")
    .Select(e => new { e.Id, e.Payload })
    .ToListAsync();
```

### Accessing JSON Properties

Access JSON properties after materialization:

```csharp
var events = await context.Events
    .Where(e => e.EventType == "purchase")
    .ToListAsync();

foreach (var evt in events)
{
    if (evt.Payload.TryGetProperty("amount", out var amount))
    {
        Console.WriteLine($"Amount: {amount.GetDecimal()}");
    }
}
```

### Raw SQL for JSON Functions

For server-side JSON operations, use raw SQL:

```csharp
var results = await context.Database
    .SqlQuery<JsonQueryResult>($"""
        SELECT
            "Id",
            JSONExtractString("Payload", 'userId') AS UserId,
            JSONExtractFloat("Payload", 'amount') AS Amount
        FROM "Events"
        WHERE JSONExtractString("Payload", 'status') = 'completed'
        """)
    .ToListAsync();
```

## Real-World Examples

### Event Tracking

```csharp
public class AnalyticsEvent
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventName { get; set; } = string.Empty;
    public string UserId { get; set; } = string.Empty;
    public JsonElement Properties { get; set; }  // Flexible schema
}

// Track page view
context.Events.Add(new AnalyticsEvent
{
    Id = Guid.NewGuid(),
    Timestamp = DateTime.UtcNow,
    EventName = "page_view",
    UserId = "user-123",
    Properties = JsonSerializer.SerializeToElement(new
    {
        page = "/products",
        referrer = "https://google.com",
        duration_ms = 1500
    })
});

// Track purchase
context.Events.Add(new AnalyticsEvent
{
    Id = Guid.NewGuid(),
    Timestamp = DateTime.UtcNow,
    EventName = "purchase",
    UserId = "user-123",
    Properties = JsonSerializer.SerializeToElement(new
    {
        order_id = "ORD-456",
        amount = 99.99,
        items = new[] { "SKU-001", "SKU-002" }
    })
});
```

### API Request Logging

```csharp
public class ApiRequestLog
{
    public DateTime Timestamp { get; set; }
    public string Method { get; set; } = string.Empty;
    public string Path { get; set; } = string.Empty;
    public int StatusCode { get; set; }
    public JsonElement RequestHeaders { get; set; }
    public JsonElement? RequestBody { get; set; }
    public JsonElement? ResponseBody { get; set; }
}
```

### Configuration Storage

```csharp
public class TenantConfig
{
    public Guid TenantId { get; set; }
    public DateTime UpdatedAt { get; set; }
    public JsonElement Settings { get; set; }  // Flexible per-tenant settings
}
```

## Working with JsonElement

### Creating JsonElement

```csharp
// From object
var element = JsonSerializer.SerializeToElement(new { key = "value" });

// From string
var element = JsonDocument.Parse("""{"key": "value"}""").RootElement;

// Empty object
var empty = JsonSerializer.SerializeToElement(new { });

// Empty array
var emptyArray = JsonSerializer.SerializeToElement(Array.Empty<object>());
```

### Reading JsonElement

```csharp
JsonElement payload = event.Payload;

// Get property
if (payload.TryGetProperty("userId", out var userId))
{
    string value = userId.GetString()!;
}

// Get nested property
var nested = payload.GetProperty("details").GetProperty("host").GetString();

// Get array
foreach (var item in payload.GetProperty("items").EnumerateArray())
{
    Console.WriteLine(item.GetString());
}

// Safe access with ValueKind check
if (payload.ValueKind == JsonValueKind.Object)
{
    // Handle object
}
```

### Deserializing

```csharp
// To typed object
var details = payload.Deserialize<PurchaseDetails>();

// To dictionary
var dict = payload.Deserialize<Dictionary<string, JsonElement>>();
```

## JsonDocument vs JsonElement

| Type | Memory | Use Case |
|------|--------|----------|
| `JsonElement` | Lighter, value type | Entity properties |
| `JsonDocument` | Disposable, owns memory | Parsing JSON strings |

Prefer `JsonElement` for entity properties:

```csharp
// Good: JsonElement for entity property
public JsonElement Payload { get; set; }

// Avoid: JsonDocument requires disposal management
public JsonDocument Payload { get; set; }  // Memory leak risk
```

## Limitations

### No Server-Side JSON Path in LINQ

JSON path expressions aren't translated to LINQ:

```csharp
// Won't work - no LINQ translation
var filtered = await context.Events
    .Where(e => e.Payload.GetProperty("status").GetString() == "active")
    .ToListAsync();
```

Use raw SQL for server-side JSON filtering:

```csharp
// Works - raw SQL
var filtered = await context.Database
    .SqlQuery<Event>($"""
        SELECT * FROM "Events"
        WHERE JSONExtractString("Payload", 'status') = 'active'
        """)
    .ToListAsync();
```

### Schema Flexibility Trade-offs

JSON columns offer schema flexibility but:
- No compile-time type checking
- Less efficient than typed columns for filtering
- Consider typed columns for frequently queried fields

```csharp
// Better for frequent filtering
public class Event
{
    public string EventType { get; set; }  // Typed column - fast filtering
    public string UserId { get; set; }      // Typed column - fast filtering
    public JsonElement Properties { get; set; }  // JSON for flexible extras
}
```

## Best Practices

### Extract Common Fields

```csharp
// Good: Common fields as typed columns
public class Event
{
    public string EventType { get; set; }  // Fast to filter/group
    public decimal? Amount { get; set; }    // Fast to aggregate
    public JsonElement Properties { get; set; }  // Flexible extras
}

// Avoid: Everything in JSON
public class Event
{
    public JsonElement Data { get; set; }  // Hard to query efficiently
}
```

### Use Nullable for Optional JSON

```csharp
public class Event
{
    public JsonElement Payload { get; set; }       // Required
    public JsonElement? Metadata { get; set; }     // Optional
}
```

### Validate Before Insert

```csharp
// Validate JSON structure before insert
if (!ValidateEventPayload(payload))
{
    throw new ArgumentException("Invalid payload structure");
}

context.Events.Add(new Event { Payload = payload });
```

## See Also

- [Type Mappings Overview](overview.md)
- [ClickHouse JSON Docs](https://clickhouse.com/docs/en/sql-reference/data-types/json)
