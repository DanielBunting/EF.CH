# QuickStartSample

The simplest possible EF.CH application - insert and query events in ClickHouse.

## What This Shows

- Connecting to ClickHouse with `UseClickHouse()`
- Configuring a table with `UseMergeTree()`
- Adding partitioning with `HasPartitionByMonth()`
- Inserting data via `SaveChangesAsync()`
- Querying with LINQ

## Prerequisites

- .NET 10.0+
- ClickHouse server running on localhost:8123

## Running

```bash
dotnet run
```

## Expected Output

```
EF.CH Quick Start Sample
========================

Creating database and tables...
Inserting sample data...
Inserted 3 events.

Querying recent events...
Found 3 events:

  [14:32:15] page_view by user-002
  [14:30:15] button_click by user-001
  [14:28:15] page_view by user-001

Event counts by type:
  page_view: 2
  button_click: 1

Done!
```

## Key Code

### Entity

```csharp
public class Event
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string UserId { get; set; } = string.Empty;
    public string? Data { get; set; }
}
```

### Configuration

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });  // Required!
    entity.HasPartitionByMonth(x => x.Timestamp);         // Optional
});
```

## Generated SQL

The configuration generates this DDL:

```sql
CREATE TABLE "Events" (
    "Id" UUID NOT NULL,
    "Timestamp" DateTime64(3) NOT NULL,
    "EventType" String NOT NULL,
    "UserId" String NOT NULL,
    "Data" Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM("Timestamp")
ORDER BY ("Timestamp", "Id")
```

## Learn More

- [Getting Started](../../docs/getting-started.md) - Full setup guide
- [ClickHouse Concepts](../../docs/clickhouse-concepts.md) - Key differences from RDBMS
- [MergeTree Engine](../../docs/engines/mergetree.md) - Engine details
