# JSON Type Sample

Demonstrates native JSON type support with ClickHouse 24.8+ using extension methods that translate to native subcolumn syntax.

## Requirements

- ClickHouse 24.8+ (for native JSON subcolumn syntax)
- Server setting: `allow_experimental_json_type = 1` (JSON is experimental in 24.8)

## What This Sample Shows

1. **JSON column configuration** with optional parameters (max_dynamic_paths, max_dynamic_types)
2. **GetPath<T>()** - Extract typed values from JSON paths
3. **GetPathOrDefault<T>()** - Extract with fallback for missing values
4. **HasPath()** - Check if a path exists
5. **Array index access** - Access array elements with `[n]` syntax
6. **Filtering by JSON values** - WHERE clauses on JSON paths
7. **Ordering by JSON values** - ORDER BY on JSON paths
8. **Aggregation on JSON values** - SUM, AVG, etc. on numeric JSON paths

## Key Extension Methods

| Method | Generated SQL | Description |
|--------|---------------|-------------|
| `GetPath<T>("user.email")` | `"Payload"."user"."email"` | Extract typed value |
| `GetPathOrDefault<int>("score", 0)` | `ifNull("Payload"."score", 0)` | With fallback |
| `HasPath("shipping")` | `"Payload"."shipping" IS NOT NULL` | Check existence |
| `GetPath<string>("items[0]")` | `"Payload"."items"[1]` | Array access (auto-converts to 1-based) |

## Running the Sample

```bash
# Start ClickHouse 24.8+
docker run -d -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server:24.8

# Run the sample
dotnet run
```

## Expected Output

```
JSON Type Sample
================

Creating database and tables...
Inserting events with JSON payloads...

Inserted 5 events.

--- User emails from all events (GetPath) ---
  user_signup: alice@example.com
  purchase: alice@example.com
  user_signup: bob@example.com
  purchase: bob@example.com
  page_view: alice@example.com

--- Premium tier signups (filter by JSON path) ---
  Alice Smith (alice@example.com)

--- Events with promo codes (HasPath) ---
  purchase: WELCOME10

--- Metrics scores (GetPath with HasPath filter) ---
  user_signup: score = 85

--- Shipping cities from purchases (nested path) ---
  alice@example.com: Seattle, USA

--- Purchases ordered by amount (descending) ---
  alice@example.com: $149.99
  bob@example.com: $29.99

Done!
```

## Typed POCO Alternative

For structured JSON with known schema, use typed POCOs:

```csharp
public class OrderMetadata
{
    public string CustomerName { get; set; } = string.Empty;
    public ShippingAddress? ShippingAddress { get; set; }
}

public class Order
{
    public Guid Id { get; set; }

    [ClickHouseJson(IsTyped = true)]
    public OrderMetadata Metadata { get; set; } = new();
}
```

Query using extension methods with snake_case paths (matching ClickHouse storage):

```csharp
context.Orders
    .Where(o => o.Metadata.GetPath<string>("shipping_address.city") == "Seattle")
    .ToListAsync();
```

## Generated SQL Examples

**Filter by JSON path:**
```sql
SELECT "Id", "Payload"."user"."email"
FROM "events"
WHERE "Payload"."user"."tier" = 'premium'
```

**Check path existence:**
```sql
SELECT "Id", "Payload"."promo_code"
FROM "events"
WHERE "Payload"."promo_code" IS NOT NULL
```

**Default value:**
```sql
SELECT "EventType", ifNull("Payload"."metrics"."score", 0)
FROM "events"
```

## Known Limitations (ClickHouse 24.8)

ClickHouse 24.8's JSON type uses a "Dynamic" type system for dynamic paths, which has some limitations:

1. **Aggregate functions** on JSON paths (SUM, AVG, etc.) require explicit type casting
2. **Array index access** (`items[0]`) may require additional handling for Dynamic type
3. **Numeric types**: JSON integers return as `Int64`, floats as `Float64` - use `long` and `double` in C#

These limitations may be resolved in future ClickHouse versions as the JSON type matures.

## See Also

- [JSON Types Documentation](../../docs/types/json.md)
- [ClickHouse JSON Docs](https://clickhouse.com/docs/en/sql-reference/data-types/json)
