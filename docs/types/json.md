# JSON Types

ClickHouse has native JSON support for semi-structured data. EF.CH provides full LINQ integration with ClickHouse 24.8+ native JSON subcolumn syntax.

## Overview

- **Native JSON type** with subcolumn access (`"column"."path"."field"`)
- **Extension method API** for type-safe LINQ queries
- **Typed POCO support** with automatic serialization
- **Configurable parameters** (max_dynamic_paths, max_dynamic_types)

**Requirements:** ClickHouse 24.8+ for native JSON subcolumn syntax.

## Type Mappings

| .NET Type | ClickHouse Type | Notes |
|-----------|-----------------|-------|
| `JsonElement` | `JSON` | Recommended for flexible schema |
| `JsonDocument` | `JSON` | Disposable, use with care |
| `T` (POCO class) | `JSON` | Via value converter, snake_case naming |

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

### Fluent API

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });

    // Basic JSON column
    entity.Property(e => e.Payload)
        .HasColumnType("JSON");

    // With parameters (controls ClickHouse storage optimization)
    entity.Property(e => e.Metadata)
        .HasColumnType("JSON")
        .HasMaxDynamicPaths(2048)      // Default: 1024
        .HasMaxDynamicTypes(64);        // Default: 32
});
```

### Attribute Configuration

```csharp
public class Event
{
    public Guid Id { get; set; }

    [ClickHouseJson(MaxDynamicPaths = 1024, MaxDynamicTypes = 32)]
    public JsonElement Payload { get; set; }

    [ClickHouseJson(IsTyped = true)]
    public OrderMetadata OrderInfo { get; set; } = new();
}
```

### JSON Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_dynamic_paths` | 1024 | Max paths stored as separate subcolumns |
| `max_dynamic_types` | 32 | Max data types per path |

Higher values = more flexibility, more memory. Lower values = better compression.

## Generated DDL

```sql
CREATE TABLE "Events" (
    "Id" UUID,
    "Timestamp" DateTime64(3),
    "EventType" String,
    "Payload" JSON,
    "Metadata" JSON(max_dynamic_paths=2048, max_dynamic_types=64)
) ENGINE = MergeTree()
ORDER BY ("Timestamp", "Id")
```

**Note:** ClickHouse doesn't support `Nullable(JSON)`. JSON columns handle null values internally, so nullable C# properties (`JsonElement?`) still generate `JSON` without the Nullable wrapper.

## Querying with Extension Methods

EF.CH provides extension methods that translate to ClickHouse's native JSON subcolumn syntax.

### Available Methods

| Method | ClickHouse SQL | Description |
|--------|---------------|-------------|
| `GetPath<T>(path)` | `"col"."path"."field"` | Extract typed value at path |
| `GetPathOrDefault<T>(path, default)` | `ifNull("col"."path", default)` | Extract with fallback |
| `HasPath(path)` | `"col"."path" IS NOT NULL` | Check if path exists |
| `GetArray<T>(path)` | `"col"."path"` | Extract array at path |
| `GetObject(path)` | `"col"."path"` | Extract nested object |

### Path Syntax

```
user.email           → "Payload"."user"."email"
tags[0]              → "Payload"."tags"[1]           (auto-converts to 1-based)
order.items[0].name  → "Payload"."order"."items"[1]."name"
```

### Query Examples

```csharp
using EF.CH.Extensions;

// Filter by JSON path value
var activeUsers = await context.Events
    .Where(e => e.Payload.GetPath<string>("user.status") == "active")
    .ToListAsync();

// Generated SQL:
// SELECT ... FROM "Events" WHERE "Payload"."user"."status" = 'active'

// Check path existence
var premiumEvents = await context.Events
    .Where(e => e.Payload.HasPath("premium.features"))
    .ToListAsync();

// Generated SQL:
// SELECT ... FROM "Events" WHERE "Payload"."premium"."features" IS NOT NULL

// Project JSON values
var userEmails = await context.Events
    .Select(e => new {
        e.Id,
        Email = e.Payload.GetPath<string>("user.email"),
        Score = e.Payload.GetPathOrDefault<int>("metrics.score", 0),
        FirstTag = e.Payload.GetPath<string>("tags[0]"),
        AllTags = e.Payload.GetArray<string>("tags")
    })
    .ToListAsync();

// Generated SQL:
// SELECT "Id",
//        "Payload"."user"."email",
//        ifNull("Payload"."metrics"."score", 0),
//        "Payload"."tags"[1],
//        "Payload"."tags"
// FROM "Events"

// Combine with other filters
var recentHighValue = await context.Events
    .Where(e => e.Timestamp > DateTime.UtcNow.AddDays(-7))
    .Where(e => e.Payload.GetPath<decimal>("amount") > 1000)
    .Where(e => e.Payload.HasPath("verified"))
    .OrderByDescending(e => e.Payload.GetPath<decimal>("amount"))
    .Take(100)
    .ToListAsync();
```

### Nested Object Access

```csharp
// Access deeply nested properties
var shippingCities = await context.Orders
    .Select(o => new {
        o.Id,
        City = o.Metadata.GetPath<string>("shipping.address.city"),
        Country = o.Metadata.GetPath<string>("shipping.address.country")
    })
    .ToListAsync();

// Get nested object as JsonElement for further processing
var addresses = await context.Orders
    .Select(o => new {
        o.Id,
        Address = o.Metadata.GetObject("shipping.address")
    })
    .ToListAsync();
```

## Typed POCO Support

Map C# classes directly to JSON columns with automatic serialization.

### Configuration

```csharp
public class OrderMetadata
{
    public string CustomerName { get; set; } = string.Empty;
    public string CustomerEmail { get; set; } = string.Empty;
    public ShippingAddress? ShippingAddress { get; set; }
    public List<string> Tags { get; set; } = [];
}

public class ShippingAddress
{
    public string Street { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string Country { get; set; } = string.Empty;
}

public class Order
{
    public Guid Id { get; set; }

    [ClickHouseJson(IsTyped = true)]
    public OrderMetadata Metadata { get; set; } = new();
}

// Or via fluent API
entity.Property(e => e.Metadata)
    .HasColumnType("JSON")
    .HasTypedJson();
```

### Serialization

Typed POCOs use `System.Text.Json` with snake_case naming:

```csharp
// C# property
public string CustomerName { get; set; }

// Stored in ClickHouse as
{"customer_name": "John Doe"}
```

### Querying Typed POCOs

Use extension methods for LINQ queries:

```csharp
// Query using extension methods
var seattleOrders = await context.Orders
    .Where(o => o.Metadata.GetPath<string>("shipping_address.city") == "Seattle")
    .Select(o => new {
        o.Id,
        Customer = o.Metadata.GetPath<string>("customer_name")
    })
    .ToListAsync();
```

**Note:** Direct property access (`o.Metadata.ShippingAddress.City`) is not translated to SQL. Use `GetPath<T>()` for server-side filtering.

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

### From JSON String

```csharp
var jsonString = """{"key": "value", "count": 42}""";
var payload = JsonDocument.Parse(jsonString).RootElement;
```

### Typed POCO

```csharp
context.Orders.Add(new Order
{
    Id = Guid.NewGuid(),
    Metadata = new OrderMetadata
    {
        CustomerName = "John Doe",
        CustomerEmail = "john@example.com",
        ShippingAddress = new ShippingAddress
        {
            City = "Seattle",
            Country = "USA"
        }
    }
});
await context.SaveChangesAsync();
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
```

### Reading JsonElement (Client-Side)

```csharp
JsonElement payload = event.Payload;

// Get property
if (payload.TryGetProperty("userId", out var userId))
{
    string value = userId.GetString()!;
}

// Get nested property
var nested = payload.GetProperty("details").GetProperty("host").GetString();

// Iterate array
foreach (var item in payload.GetProperty("items").EnumerateArray())
{
    Console.WriteLine(item.GetString());
}
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

// Good: Nullable JsonElement for optional JSON
public JsonElement? Metadata { get; set; }

// Avoid: JsonDocument requires disposal management and causes EF change tracking issues
public JsonDocument Payload { get; set; }  // Memory leak risk + EF snapshot issues
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
    public JsonElement Properties { get; set; }
}

// Query events with specific properties
var purchases = await context.Events
    .Where(e => e.EventName == "purchase")
    .Where(e => e.Properties.GetPath<decimal>("amount") > 100)
    .Select(e => new {
        e.UserId,
        Amount = e.Properties.GetPath<decimal>("amount"),
        ProductId = e.Properties.GetPath<string>("product_id")
    })
    .ToListAsync();
```

### API Request Logging

```csharp
public class ApiRequestLog
{
    public DateTime Timestamp { get; set; }
    public string Method { get; set; } = string.Empty;
    public string Path { get; set; } = string.Empty;
    public int StatusCode { get; set; }

    [ClickHouseJson(MaxDynamicPaths = 256)]
    public JsonElement RequestHeaders { get; set; }

    public JsonElement? RequestBody { get; set; }
}

// Find requests with specific header
var authRequests = await context.ApiLogs
    .Where(l => l.RequestHeaders.HasPath("Authorization"))
    .ToListAsync();
```

### Configuration Storage

```csharp
public class TenantConfig
{
    public Guid TenantId { get; set; }
    public DateTime UpdatedAt { get; set; }

    [ClickHouseJson(IsTyped = true)]
    public TenantSettings Settings { get; set; } = new();
}

public class TenantSettings
{
    public string Theme { get; set; } = "default";
    public int MaxUsers { get; set; } = 10;
    public List<string> EnabledFeatures { get; set; } = [];
}
```

## Best Practices

### Extract Frequently Filtered Fields

```csharp
// Good: Common fields as typed columns for fast filtering
public class Event
{
    public string EventType { get; set; }     // Fast to filter/group
    public string UserId { get; set; }         // Fast to filter/group
    public decimal? Amount { get; set; }       // Fast to aggregate
    public JsonElement Properties { get; set; } // Flexible extras
}

// Avoid: Everything in JSON (slow filtering)
public class Event
{
    public JsonElement Data { get; set; }
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

### Choose Appropriate Parameters

```csharp
// High cardinality, many dynamic paths
entity.Property(e => e.FlexibleData)
    .HasColumnType("JSON")
    .HasMaxDynamicPaths(4096);

// Low cardinality, known structure
entity.Property(e => e.Config)
    .HasColumnType("JSON")
    .HasMaxDynamicPaths(64);
```

## Known Limitations (ClickHouse 24.8)

The JSON type in ClickHouse 24.8 is experimental and uses a "Dynamic" type system:

1. **Experimental flag required**: Set `allow_experimental_json_type = 1` in server config
2. **Aggregate functions** on JSON paths may require explicit type casting
3. **Array index access** (`items[0]`) may need additional handling for Dynamic type
4. **Numeric types**: JSON integers return as `Int64`, floats as `Float64` - use `long` and `double` in C#

These limitations may be resolved in future ClickHouse versions as the JSON type matures.

## See Also

- [Type Mappings Overview](overview.md)
- [ClickHouse JSON Docs](https://clickhouse.com/docs/en/sql-reference/data-types/json)
- [ClickHouse JSON Subcolumn Syntax](https://clickhouse.com/docs/en/sql-reference/data-types/newjson)
