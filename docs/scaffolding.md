# Scaffolding

Scaffolding (reverse engineering) generates C# entity classes and a `DbContext` from an existing ClickHouse database. EF.CH extends the standard EF Core scaffolding pipeline to detect ClickHouse-specific features including engine types, enum definitions, and Nested types.

## Running the Scaffolder

Use the standard EF Core command to scaffold from a ClickHouse database.

```bash
dotnet ef dbcontext scaffold "Host=localhost;Port=8123;Database=mydb" EF.CH
```

Or in the Package Manager Console:

```powershell
Scaffold-DbContext "Host=localhost;Port=8123;Database=mydb" EF.CH
```

### Options

```bash
# Scaffold specific tables only
dotnet ef dbcontext scaffold "Host=localhost;Port=8123;Database=mydb" EF.CH \
    --table Orders --table Products

# Specify output directory
dotnet ef dbcontext scaffold "Host=localhost;Port=8123;Database=mydb" EF.CH \
    --output-dir Models

# Specify context name
dotnet ef dbcontext scaffold "Host=localhost;Port=8123;Database=mydb" EF.CH \
    --context AnalyticsDbContext
```

## Engine Detection

The scaffolder queries `system.tables` to detect the engine type and parameters for each table. Engine metadata is preserved as annotations on the generated entity configuration.

| Detected | Generated Configuration |
|----------|------------------------|
| Engine name | `UseMergeTree(...)`, `UseReplacingMergeTree(...)`, etc. |
| ORDER BY columns | Columns passed to the engine method |
| PARTITION BY expression | `HasPartitionBy(...)` or `HasPartitionByMonth(...)` |
| PRIMARY KEY (if different from ORDER BY) | `HasPrimaryKey(...)` |
| SAMPLE BY | `HasSampleBy(...)` |
| Version column (ReplacingMergeTree) | Version parameter in engine method |
| Sign column (CollapsingMergeTree) | Sign parameter in engine method |

### Example

Given a ClickHouse table:

```sql
CREATE TABLE events
(
    event_id UInt64,
    user_id UInt64,
    event_type Enum8('click' = 1, 'view' = 2, 'purchase' = 3),
    timestamp DateTime64(3),
    amount Nullable(Decimal(18,4))
)
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY (user_id, event_id)
PARTITION BY toYYYYMM(timestamp);
```

The scaffolder generates:

```csharp
public class Event
{
    public ulong EventId { get; set; }
    public ulong UserId { get; set; }
    public EventsEventType EventType { get; set; }
    public DateTime Timestamp { get; set; }
    public decimal? Amount { get; set; }
}
```

With `OnModelCreating` configuration:

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.UseReplacingMergeTree(x => x.Timestamp, x => new { x.UserId, x.EventId });
    entity.HasPartitionBy("toYYYYMM(Timestamp)");
});
```

## C# Enum Generation

When the scaffolder encounters a ClickHouse `Enum8` or `Enum16` column, it generates a corresponding C# enum type with the same member names and values.

Given:

```sql
event_type Enum8('click' = 1, 'view' = 2, 'purchase' = 3)
```

Generated enum:

```csharp
public enum EventsEventType
{
    click = 1,
    view = 2,
    purchase = 3
}
```

### Enum Naming Convention

The enum type name is derived from the table and column names:

- If the column name already ends with `Status`, `Type`, `Kind`, `State`, or `Mode`, the column name is used directly (for example, `EventType` becomes the enum name `EventType`)
- Otherwise, the table name is prepended: `Events` + `Category` becomes `EventsCategory`

Both names are converted to PascalCase from any snake_case or kebab-case source.

## Nested Type Detection

ClickHouse stores `Nested` types as parallel arrays with dotted column names. The scaffolder detects these patterns and generates:

1. A virtual column with the `Nested(...)` store type
2. A comment containing a suggested record class definition

Given columns in `system.columns`:

```
Goals.ID    Array(UInt32)
Goals.Name  Array(String)
```

The scaffolder generates a property with a documentation comment:

```csharp
/// <summary>
/// ClickHouse Nested type with fields: ID (uint), Name (string)
///
/// TODO: Replace List<object> with a custom record type:
///
/// public record EventsGoal
/// {
///     public uint ID { get; set; }
///     public string Name { get; set; }
/// }
///
/// Then change this property to: public List<EventsGoal> ...
/// </summary>
public List<object> Goals { get; set; }
```

## Column Type Mapping

The scaffolder maps ClickHouse types to CLR types during reverse engineering.

| ClickHouse Type | C# Type |
|----------------|---------|
| `UInt8` | `byte` |
| `UInt16` | `ushort` |
| `UInt32` | `uint` |
| `UInt64` | `ulong` |
| `Int8` | `sbyte` |
| `Int16` | `short` |
| `Int32` | `int` |
| `Int64` | `long` |
| `Float32` | `float` |
| `Float64` | `double` |
| `Decimal(P,S)` | `decimal` |
| `String` | `string` |
| `FixedString(N)` | `string` |
| `Bool` | `bool` |
| `UUID` | `Guid` |
| `DateTime64(P)` | `DateTime` |
| `Date` | `DateOnly` |
| `Enum8(...)` | Generated C# enum |
| `Enum16(...)` | Generated C# enum |
| `Nullable(T)` | Nullable CLR type |
| `LowCardinality(T)` | Same as inner type T |
| `Array(T)` | Array of mapped type |

## Compression Codec Detection

If a column has a compression codec configured, the scaffolder stores the codec as an annotation. This information can be used to generate `HasCodec(...)` calls in the entity configuration.

## System Table Queries

The scaffolder reads metadata from these ClickHouse system tables:

| System Table | Data Retrieved |
|-------------|----------------|
| `system.tables` | Table names, engine types, sorting keys, partition keys, comments |
| `system.columns` | Column names, types, default expressions, compression codecs, key membership |

Materialized views, dictionaries, and temporary tables are excluded from scaffolding.

## See Also

- [Migrations Overview](migrations/overview.md) -- forward migration workflow after scaffolding
- [Split Migrations](migrations/split-migrations.md) -- how EF.CH handles multi-operation migrations
