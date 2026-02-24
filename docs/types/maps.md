# Map(K, V)

## CLR to ClickHouse Mapping

```
Dictionary<K, V>         --> Map(K, V)
IDictionary<K, V>        --> Map(K, V)
IReadOnlyDictionary<K, V> --> Map(K, V)
```

## Entity Definition

```csharp
public class Product
{
    public uint Id { get; set; }
    public Dictionary<string, string> Attributes { get; set; } = new();
    public Dictionary<string, int> Metrics { get; set; } = new();
}
```

```sql
CREATE TABLE "Products" (
    "Id" UInt32,
    "Attributes" Map(String, String),
    "Metrics" Map(String, Int32)
) ENGINE = MergeTree() ORDER BY ("Id")
```

> **Note:** ClickHouse map keys cannot be `Nullable`. Attempting to use a nullable key type (e.g., `Dictionary<string?, int>`) will result in an error. Values can be nullable.

## LINQ Translations

### ContainsKey

```csharp
context.Products.Where(p => p.Attributes.ContainsKey("color"))
```

```sql
SELECT * FROM "Products" WHERE mapContains("Attributes", 'color')
```

This works on `Dictionary<K,V>`, `IDictionary<K,V>`, and `IReadOnlyDictionary<K,V>`.

### Keys

```csharp
context.Products.Select(p => p.Attributes.Keys)
```

```sql
SELECT mapKeys("Attributes") FROM "Products"
```

### Values

```csharp
context.Products.Select(p => p.Attributes.Values)
```

```sql
SELECT mapValues("Attributes") FROM "Products"
```

### Count

```csharp
context.Products.Select(p => p.Attributes.Count)
```

```sql
SELECT length(mapKeys("Attributes")) FROM "Products"
```

ClickHouse does not have a direct `mapLength` function. The provider translates `Count` to `length(mapKeys(map))` -- extracting the keys array first, then taking its length.

## SQL Literal Format

Map literals use brace-delimited key-value pairs:

```sql
{'color': 'red', 'size': 'large'}     -- Map(String, String)
{'clicks': 42, 'views': 1000}         -- Map(String, Int32)
{}                                      -- Empty map
```

## Internal Storage

ClickHouse stores maps internally as `Array(Tuple(K, V))`. This means a `Map(String, Int32)` is effectively `Array(Tuple(String, Int32))` on disk. The map syntax provides ergonomic access patterns on top of this storage format.

## Translation Reference

| C# Expression | ClickHouse SQL |
|----------------|----------------|
| `dict.ContainsKey(key)` | `mapContains(map, key)` |
| `dict.Keys` | `mapKeys(map)` |
| `dict.Values` | `mapValues(map)` |
| `dict.Count` | `length(mapKeys(map))` |

## See Also

- [Type System Overview](overview.md)
- [Arrays](arrays.md)
- [Tuples](tuples.md)
