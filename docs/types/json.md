# JSON Type

> **Note:** The native JSON type requires ClickHouse 24.8 or later.

## CLR to ClickHouse Mapping

```
JsonElement   --> JSON
JsonDocument  --> JSON
```

## Entity Definition

```csharp
using System.Text.Json;

public class Event
{
    public uint Id { get; set; }
    public JsonElement Metadata { get; set; }
}
```

```sql
CREATE TABLE "Events" (
    "Id" UInt32,
    "Metadata" JSON
) ENGINE = MergeTree() ORDER BY ("Id")
```

## JSON Column Configuration

### max_dynamic_paths

Controls the maximum number of paths stored as subcolumns (ClickHouse default: 1024):

```csharp
entity.Property(x => x.Metadata)
    .HasColumnType("JSON")
    .HasMaxDynamicPaths(2048);
```

```sql
"Metadata" JSON(max_dynamic_paths=2048)
```

### max_dynamic_types

Controls the maximum number of types per path (ClickHouse default: 32):

```csharp
entity.Property(x => x.Metadata)
    .HasColumnType("JSON")
    .HasMaxDynamicTypes(64);
```

```sql
"Metadata" JSON(max_dynamic_types=64)
```

### Combined Configuration

```csharp
entity.Property(x => x.Metadata)
    .HasColumnType("JSON")
    .HasJsonOptions(maxDynamicPaths: 1024, maxDynamicTypes: 32);
```

```sql
"Metadata" JSON(max_dynamic_paths=1024, max_dynamic_types=32)
```

### Typed POCO Mapping

Map a JSON column to a strongly-typed POCO for compile-time safety:

```csharp
public class OrderMetadata
{
    public string CustomerName { get; set; }
    public int Priority { get; set; }
}

entity.Property(x => x.Metadata)
    .HasTypedJson<OrderMetadata>();
```

When marked as typed JSON, property navigation on the POCO translates to ClickHouse subcolumn access syntax in LINQ queries.

## Subcolumn Access Methods

All JSON access methods are defined in `ClickHouseJsonFunctions` and are LINQ translation stubs -- they throw if invoked outside a query.

### GetPath&lt;T&gt;

Extract a typed value at a dot-separated path:

```csharp
context.Events
    .Select(e => new {
        Email = e.Metadata.GetPath<string>("user.email"),
        Score = e.Metadata.GetPath<int>("metrics.score")
    })
```

```sql
SELECT "Metadata"."user"."email", "Metadata"."metrics"."score" FROM "Events"
```

Array index access is also supported:

```csharp
context.Events.Select(e => e.Metadata.GetPath<string>("tags[0]"))
```

```sql
SELECT "Metadata"."tags"[1] FROM "Events"
```

> **Note:** Array indices in path expressions are automatically converted from C# 0-based to ClickHouse 1-based indexing.

### GetPathOrDefault&lt;T&gt;

Extract a value with a fallback default:

```csharp
context.Events.Select(e => e.Metadata.GetPathOrDefault<int>("metrics.score", 0))
```

```sql
SELECT ifNull("Metadata"."metrics"."score", 0) FROM "Events"
```

### HasPath

Check if a path exists and is not null:

```csharp
context.Events.Where(e => e.Metadata.HasPath("premium.features"))
```

```sql
SELECT * FROM "Events" WHERE "Metadata"."premium"."features" IS NOT NULL
```

### GetArray&lt;T&gt;

Extract an array at a path:

```csharp
context.Events.Select(e => e.Metadata.GetArray<string>("tags"))
```

```sql
SELECT "Metadata"."tags" FROM "Events"
```

### GetObject

Extract a nested JSON object as `JsonElement`:

```csharp
context.Events.Select(e => e.Metadata.GetObject("user"))
```

```sql
SELECT "Metadata"."user" FROM "Events"
```

## SQL Literal Format

JSON literals are single-quoted JSON strings with internal escaping:

```sql
'{"user": {"email": "test@example.com"}, "score": 42}'
```

The provider serializes POCOs using `snake_case` naming and excludes null values by default.

## Working with JsonDocument

`JsonDocument` works identically to `JsonElement`:

```csharp
public class Log
{
    public uint Id { get; set; }
    public JsonDocument Payload { get; set; }
}

context.Logs.Select(l => l.Payload.GetPath<string>("level"))
```

```sql
SELECT "Payload"."level" FROM "Logs"
```

## Translation Reference

| C# Expression | ClickHouse SQL |
|----------------|----------------|
| `json.GetPath<T>("a.b")` | `"column"."a"."b"` |
| `json.GetPathOrDefault<T>("a.b", val)` | `ifNull("column"."a"."b", val)` |
| `json.HasPath("a.b")` | `"column"."a"."b" IS NOT NULL` |
| `json.GetArray<T>("a")` | `"column"."a"` |
| `json.GetObject("a")` | `"column"."a"` |

## See Also

- [Type System Overview](overview.md)
