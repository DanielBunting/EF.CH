# Nested Type

## CLR to ClickHouse Mapping

```
List<TRecord>  --> Nested(field1 T1, field2 T2, ...)
TRecord[]      --> Nested(field1 T1, field2 T2, ...)
```

The record type's public properties become parallel arrays in ClickHouse.

## Entity Definition

```csharp
public class Goal
{
    public uint Id { get; set; }
    public DateTime EventTime { get; set; }
}

public class Match
{
    public uint MatchId { get; set; }
    public List<Goal> Goals { get; set; } = [];
}
```

```sql
CREATE TABLE "Matches" (
    "MatchId" UInt32,
    "Goals" Nested(
        "Id" UInt32,
        "EventTime" DateTime64(3)
    )
) ENGINE = MergeTree() ORDER BY ("MatchId")
```

## How Nested Works in ClickHouse

ClickHouse Nested types are syntactic sugar for parallel arrays. A `Nested(Id UInt32, EventTime DateTime64(3))` column is stored on disk as two separate arrays:

```
Goals.Id        Array(UInt32)
Goals.EventTime Array(DateTime64(3))
```

All parallel arrays within a Nested column must always have the same length.

## Insert Format

When inserting data, ClickHouse expects parallel arrays, not row-oriented data:

```sql
-- Parallel array format
INSERT INTO "Matches" VALUES (1, ([10, 20], ['2024-01-15 10:30:00.000', '2024-01-15 11:00:00.000']))
```

The provider handles the conversion from `List<TRecord>` to parallel arrays automatically.

## LINQ Translations

### Count

```csharp
context.Matches.Select(m => m.Goals.Count)
context.Matches.Select(m => Enumerable.Count(m.Goals))
```

```sql
SELECT length("Goals.Id") FROM "Matches"
SELECT length("Goals.Id") FROM "Matches"
```

> **Note:** Since all parallel arrays in a Nested column have the same length, the provider uses the first field's array for `length()` operations. In this example, `Goals.Id` is used because `Id` is the first property of the `Goal` record.

### Any (notEmpty)

```csharp
context.Matches.Where(m => m.Goals.Any())
```

```sql
SELECT * FROM "Matches" WHERE notEmpty("Goals.Id")
```

## Eligible Record Types

A type is eligible for Nested mapping if it:

- Is a class or record (not a primitive type)
- Is not a collection type itself
- Is not a known special type (`Guid`, `DateTime`, `DateTimeOffset`, etc.)
- Has at least one public readable property
- All properties are simple, mappable types (primitives, `string`, `decimal`, `Guid`, `DateTime`, enums, large integers, etc.)

Types that do not meet these criteria will not be mapped as Nested and may be mapped as JSON or require explicit column type configuration.

## SQL Literal Format

Nested literals use the parallel array format:

```sql
-- Two goals: (Id=10, EventTime=...), (Id=20, EventTime=...)
([10, 20], ['2024-01-15 10:30:00.000', '2024-01-15 11:00:00.000'])

-- Empty nested
([], [])
```

## Value Conversion

The provider uses a built-in value converter that transforms between the CLR list format and ClickHouse's tuple array format:

- **Write path**: `List<TRecord>` is converted to `Tuple<T1, T2, ...>[]` where each tuple contains one record's field values.
- **Read path**: `Tuple<T1, T2, ...>[]` is converted back to `List<TRecord>` by reconstructing record instances from tuple elements.

This conversion supports up to 7 fields per record (the limit of `System.Tuple<>` generic arity).

## Translation Reference

| C# Expression | ClickHouse SQL |
|----------------|----------------|
| `nested.Count` | `length("Column.FirstField")` |
| `Enumerable.Count(nested)` | `length("Column.FirstField")` |
| `nested.Any()` | `notEmpty("Column.FirstField")` |
| `Enumerable.Any(nested)` | `notEmpty("Column.FirstField")` |

## See Also

- [Type System Overview](overview.md)
- [Arrays](arrays.md)
- [Tuples](tuples.md)
