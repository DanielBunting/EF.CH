# Bulk Insert

High-throughput insert operations that bypass EF Core change tracking. Bulk insert sends data directly to ClickHouse in batches, providing significantly higher throughput than `SaveChangesAsync()` for large datasets.

All extension methods live in the `EF.CH.Extensions` namespace.

```csharp
using EF.CH.Extensions;
```

## Basic Bulk Insert

Insert a collection of entities via the `DbContext`:

```csharp
var events = Enumerable.Range(0, 100_000).Select(i => new Event
{
    Id = Guid.NewGuid(),
    Timestamp = DateTime.UtcNow,
    EventType = "click",
    Amount = i * 0.5m
});

var result = await context.BulkInsertAsync(events);

Console.WriteLine($"Inserted {result.RowsInserted} rows in {result.Elapsed.TotalSeconds:F2}s");
Console.WriteLine($"Throughput: {result.RowsPerSecond:F0} rows/sec");
```

Generated SQL (per batch):

```sql
INSERT INTO "Events" ("Id", "Timestamp", "EventType", "Amount")
VALUES (...), (...), ...
```

## Bulk Insert via DbSet

Insert through a `DbSet<T>` directly:

```csharp
var result = await context.Events.BulkInsertAsync(events);
```

This is equivalent to calling `context.BulkInsertAsync<Event>(events)` but reads more naturally when you already have a reference to the DbSet.

## Batch Size Configuration

The default batch size is 10,000 entities. Configure it via the options callback:

```csharp
var result = await context.BulkInsertAsync(events, options => options
    .WithBatchSize(50_000));
```

Larger batches reduce the number of round-trips to ClickHouse but consume more memory. Smaller batches are safer for entities with large columns (strings, arrays, JSON). Measure throughput with your actual data to find the optimal size.

## Streaming Insert

For datasets that do not fit in memory, use `BulkInsertStreamingAsync` with an `IAsyncEnumerable<T>`. Entities are consumed and inserted in batches without buffering the entire collection.

```csharp
async IAsyncEnumerable<Event> GenerateEvents()
{
    await foreach (var line in File.ReadLinesAsync("events.csv"))
    {
        yield return ParseEvent(line);
    }
}

var result = await context.BulkInsertStreamingAsync(GenerateEvents());
```

Streaming insert also works on a DbSet:

```csharp
var result = await context.Events.BulkInsertStreamingAsync(GenerateEvents());
```

The streaming API uses the same batching internally. Each batch is sent to ClickHouse as soon as it is filled, so memory usage stays bounded regardless of total dataset size.

## Insert Format

Two formats are available for generating the INSERT statement:

| Format | Method | Description |
|--------|--------|-------------|
| Values (default) | `WithFormat(ClickHouseBulkInsertFormat.Values)` | Standard SQL VALUES syntax |
| JSONEachRow | `WithFormat(ClickHouseBulkInsertFormat.JsonEachRow)` | One JSON object per line |

```csharp
var result = await context.BulkInsertAsync(events, options => options
    .WithFormat(ClickHouseBulkInsertFormat.JsonEachRow));
```

JSONEachRow can be useful for entities with complex types (arrays, maps, nested JSON) where JSON serialization handles escaping automatically. Values format is the default and works with all ClickHouse versions.

## Async Insert Mode

Enable ClickHouse's `async_insert` for write-heavy workloads. Inserts are buffered on the server and flushed in batches, reducing write amplification at the cost of slightly delayed durability.

```csharp
var result = await context.BulkInsertAsync(events, options => options
    .WithAsyncInsert(wait: true));
```

When `wait` is `false`, the call returns immediately after the server acknowledges receipt. When `true`, it waits for the data to be flushed to storage.

## Parallel Insertion

Send multiple batches in parallel using separate connections:

```csharp
var result = await context.BulkInsertAsync(events, options => options
    .WithBatchSize(25_000)
    .WithParallelism(4));
```

This sends up to 4 batches concurrently. Use this when network latency is the bottleneck rather than ClickHouse server throughput.

## Server-Side Insert Threads

Control how many threads ClickHouse uses to parse and insert the data:

```csharp
var result = await context.BulkInsertAsync(events, options => options
    .WithMaxInsertThreads(4));
```

## Progress Callback

Track progress during long-running inserts:

```csharp
var result = await context.BulkInsertAsync(events, options => options
    .WithBatchSize(10_000)
    .WithProgressCallback(rowsSoFar =>
        Console.WriteLine($"Inserted {rowsSoFar} rows...")));
```

The callback is invoked after each batch completes, with the cumulative count of rows inserted so far.

## Custom Settings and Timeout

Add arbitrary ClickHouse settings to the INSERT statement and control the command timeout:

```csharp
var result = await context.BulkInsertAsync(events, options => options
    .WithSetting("max_insert_block_size", 1_000_000)
    .WithSetting("min_insert_block_size_rows", 10_000)
    .WithTimeout(TimeSpan.FromMinutes(5)));
```

## Fluent Option Chaining

All options return `this` for fluent chaining:

```csharp
var result = await context.BulkInsertAsync(events, options => options
    .WithBatchSize(50_000)
    .WithFormat(ClickHouseBulkInsertFormat.JsonEachRow)
    .WithAsyncInsert(wait: true)
    .WithParallelism(2)
    .WithMaxInsertThreads(4)
    .WithTimeout(TimeSpan.FromMinutes(10))
    .WithProgressCallback(rows => Console.WriteLine($"{rows} rows inserted")));
```

## Result Object

`BulkInsertAsync` and `BulkInsertStreamingAsync` both return a `ClickHouseBulkInsertResult`:

| Property | Type | Description |
|----------|------|-------------|
| `RowsInserted` | `long` | Total number of rows inserted |
| `BatchesExecuted` | `int` | Number of batches sent |
| `Elapsed` | `TimeSpan` | Total wall-clock time |
| `RowsPerSecond` | `double` | Computed throughput |

## See Also

- [INSERT...SELECT](insert-select.md) -- Server-side data movement without client round-trips
- [Temporary Tables](temp-tables.md) -- Session-scoped staging tables
- [Export](export.md) -- Exporting query results in various formats
