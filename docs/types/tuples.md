# Tuple(T1, T2, ...)

## CLR to ClickHouse Mapping

```
ValueTuple<T1, T2, ...>  --> Tuple(T1, T2, ...)
Tuple<T1, T2, ...>       --> Tuple(T1, T2, ...)
```

Both C# value tuples (`ValueTuple`) and reference tuples (`Tuple`) are supported with 1 to 8+ elements.

## Entity Definition

```csharp
public class GeoEvent
{
    public uint Id { get; set; }
    public (double Latitude, double Longitude) Location { get; set; }
    public (string Name, int Age, bool Active) UserInfo { get; set; }
}
```

```sql
CREATE TABLE "GeoEvents" (
    "Id" UInt32,
    "Location" Tuple(Float64, Float64),
    "UserInfo" Tuple(String, Int32, Bool)
) ENGINE = MergeTree() ORDER BY ("Id")
```

## Named Tuples

ClickHouse supports named tuples where each element has a label:

```sql
-- Unnamed tuple
Tuple(Int32, String)

-- Named tuple
Tuple(id Int32, name String)
```

The provider generates named tuples when element names are provided through the type mapping configuration.

## Value Tuples (C# 7+)

C# value tuples are the recommended approach. They use `Item1`, `Item2`, etc. fields internally, and support destructuring:

```csharp
public class Measurement
{
    public uint Id { get; set; }
    public (DateTime Timestamp, double Value) Reading { get; set; }
}

// Usage
var reading = (DateTime.UtcNow, 42.5);
```

## Reference Tuples

Classic `System.Tuple<T1, T2, ...>` types are also supported:

```csharp
public Tuple<string, int> Pair { get; set; }
// DDL: "Pair" Tuple(String, Int32)
```

## Large Tuples (8+ Elements)

For tuples with more than 7 elements, .NET uses a nested `TRest` field (for `ValueTuple`) or `Rest` property (for `Tuple`). The provider handles this nesting transparently, flattening the elements into a single ClickHouse tuple:

```csharp
public (int A, int B, int C, int D, int E, int F, int G, int H) Data { get; set; }
// DDL: "Data" Tuple(Int32, Int32, Int32, Int32, Int32, Int32, Int32, Int32)
```

## SQL Literal Format

Tuple literals are generated as parenthesized, comma-separated values:

```sql
(42, 'hello')                -- Tuple(Int32, String)
(1.5, 2.5)                  -- Tuple(Float64, Float64)
(1, 'name', NULL)            -- Tuple with nullable element
```

## See Also

- [Type System Overview](overview.md)
- [Arrays](arrays.md)
- [Maps](maps.md)
- [Nested](nested.md)
