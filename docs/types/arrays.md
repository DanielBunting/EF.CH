# Array(T)

## CLR to ClickHouse Mapping

```
T[]                    --> Array(T)
List<T>                --> Array(T)
IList<T>               --> Array(T)
ICollection<T>         --> Array(T)
IEnumerable<T>         --> Array(T)
IReadOnlyList<T>       --> Array(T)
IReadOnlyCollection<T> --> Array(T)
```

Any CLR array or generic list type maps to `Array(T)` where `T` is the ClickHouse mapping of the element type.

## Entity Definition

```csharp
public class Event
{
    public uint Id { get; set; }
    public string[] Tags { get; set; } = [];
    public List<int> Scores { get; set; } = [];
}
```

```sql
CREATE TABLE "Events" (
    "Id" UInt32,
    "Tags" Array(String),
    "Scores" Array(Int32)
) ENGINE = MergeTree() ORDER BY ("Id")
```

## LINQ Translations

### Contains

```csharp
context.Events.Where(e => e.Tags.Contains("error"))
```

```sql
SELECT * FROM "Events" WHERE has("Tags", 'error')
```

The static `Enumerable.Contains` overload also translates:

```csharp
context.Events.Where(e => Enumerable.Contains(e.Tags, "error"))
```

```sql
SELECT * FROM "Events" WHERE has("Tags", 'error')
```

### Length / Count

```csharp
context.Events.Select(e => e.Tags.Length)
context.Events.Select(e => e.Scores.Count)
context.Events.Select(e => Enumerable.Count(e.Tags))
```

All three produce:

```sql
SELECT length("Tags") FROM "Events"
SELECT length("Scores") FROM "Events"
SELECT length("Tags") FROM "Events"
```

### Any (notEmpty)

```csharp
context.Events.Where(e => e.Tags.Any())
```

```sql
SELECT * FROM "Events" WHERE notEmpty("Tags")
```

### First / FirstOrDefault

```csharp
context.Events.Select(e => e.Tags.First())
context.Events.Select(e => e.Tags.FirstOrDefault())
```

```sql
SELECT arrayElement("Tags", 1) FROM "Events"
SELECT arrayElement("Tags", 1) FROM "Events"
```

> **Note:** ClickHouse arrays are 1-based. The provider automatically converts C# 0-based semantics to ClickHouse 1-based indexing. `First()` becomes `arrayElement(arr, 1)`, not `arrayElement(arr, 0)`.

### Last / LastOrDefault

```csharp
context.Events.Select(e => e.Tags.Last())
context.Events.Select(e => e.Tags.LastOrDefault())
```

```sql
SELECT arrayElement("Tags", -1) FROM "Events"
SELECT arrayElement("Tags", -1) FROM "Events"
```

ClickHouse supports negative indexing: `-1` is the last element, `-2` is second to last, and so on.

## Array Aggregate Combinators

Array-level aggregations are available through `ClickHouseAggregates` extension methods:

```csharp
using EF.CH.Extensions;

context.Events.Select(e => e.Scores.ArraySum())    // arraySum("Scores")
context.Events.Select(e => e.Scores.ArrayAvg())    // arrayAvg("Scores")
context.Events.Select(e => e.Scores.ArrayMin())    // arrayMin("Scores")
context.Events.Select(e => e.Scores.ArrayMax())    // arrayMax("Scores")
context.Events.Select(e => e.Scores.ArrayCount())  // length("Scores")
```

## SQL Literal Format

Array literals are generated as bracket-delimited lists:

```sql
[1, 2, 3]                -- Array(Int32)
['hello', 'world']       -- Array(String)
[NULL, 'a', 'b']         -- Array(Nullable(String))
```

## Translation Reference

| C# Expression | ClickHouse SQL |
|----------------|----------------|
| `array.Contains(item)` | `has(array, item)` |
| `Enumerable.Contains(array, item)` | `has(array, item)` |
| `array.Length` | `length(array)` |
| `list.Count` | `length(array)` |
| `Enumerable.Count(array)` | `length(array)` |
| `array.Any()` | `notEmpty(array)` |
| `array.First()` | `arrayElement(array, 1)` |
| `array.FirstOrDefault()` | `arrayElement(array, 1)` |
| `array.Last()` | `arrayElement(array, -1)` |
| `array.LastOrDefault()` | `arrayElement(array, -1)` |
| `array.ArraySum()` | `arraySum(array)` |
| `array.ArrayAvg()` | `arrayAvg(array)` |
| `array.ArrayMin()` | `arrayMin(array)` |
| `array.ArrayMax()` | `arrayMax(array)` |
| `array.ArrayCount()` | `length(array)` |

## See Also

- [Type System Overview](overview.md)
- [Maps](maps.md)
- [Nested](nested.md)
