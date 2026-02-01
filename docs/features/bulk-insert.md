# Bulk Insert

EF.CH provides high-performance bulk insert operations that bypass EF Core change tracking for maximum throughput. Use bulk insert when you need to insert thousands or millions of rows efficiently.

## Why Bulk Insert?

Standard EF Core inserts (`AddRange` + `SaveChangesAsync`) have significant overhead:

- Change tracking for every entity
- Individual INSERT statements or small batches
- Round-trip latency per batch
- Memory pressure from tracked entities

Bulk insert bypasses these bottlenecks:

- No change tracking overhead
- Optimized batch sizes (default 10,000 rows)
- Direct SQL generation
- Streaming support for memory efficiency
- Parallel insertion for maximum throughput

**Typical performance improvement: 50-100x faster than standard EF Core.**

## Basic Usage

### Simple Insert

```csharp
using EF.CH.Extensions;

var events = GenerateEvents(100_000);

var result = await context.BulkInsertAsync(events);

Console.WriteLine($"Inserted {result.RowsInserted} rows");
Console.WriteLine($"Throughput: {result.RowsPerSecond:N0} rows/sec");
```

### Via DbSet

```csharp
// Both approaches are equivalent
await context.BulkInsertAsync(events);
await context.Events.BulkInsertAsync(events);
```

## Options

Configure bulk insert behavior using the options callback:

```csharp
var result = await context.BulkInsertAsync(events, options => options
    .WithBatchSize(5_000)
    .WithFormat(ClickHouseBulkInsertFormat.Values)
    .WithParallelism(4)
    .WithProgressCallback(rows => Console.WriteLine($"Progress: {rows}"))
    .WithTimeout(TimeSpan.FromMinutes(10)));
```

### Batch Size

Controls how many rows are inserted per batch. Larger batches are more efficient but use more memory.

```csharp
// Default: 10,000 rows per batch
await context.BulkInsertAsync(events, options => options
    .WithBatchSize(50_000));  // Larger batches for better throughput
```

**Guidelines:**
- Small rows (few columns): 50,000-100,000 per batch
- Large rows (many columns, strings): 5,000-10,000 per batch
- Memory constrained: reduce batch size

### Insert Format

Choose between SQL VALUES format and JSONEachRow format:

```csharp
// Default: VALUES format
await context.BulkInsertAsync(events, options => options
    .WithFormat(ClickHouseBulkInsertFormat.Values));

// JSONEachRow format
await context.BulkInsertAsync(events, options => options
    .WithFormat(ClickHouseBulkInsertFormat.JsonEachRow));
```

| Format | Generated SQL |
|--------|---------------|
| `Values` | `INSERT INTO table VALUES (1, 'a'), (2, 'b'), ...` |
| `JsonEachRow` | `INSERT INTO table FORMAT JSONEachRow {"col1":1,"col2":"a"}...` |

**When to use each:**
- `Values` (default): Fastest, works for most types
- `JsonEachRow`: Better for complex nested types, easier to debug

### Async Insert

Enable ClickHouse async insert mode for higher throughput at the cost of immediate durability:

```csharp
// Fire-and-forget (fastest, but data may be buffered)
await context.BulkInsertAsync(events, options => options
    .WithAsyncInsert());

// Wait for async insert to complete
await context.BulkInsertAsync(events, options => options
    .WithAsyncInsert(wait: true));
```

Async insert buffers data on the server before writing to storage, which can improve throughput for high-frequency inserts.

### Parallelism

Use multiple connections to insert batches in parallel:

```csharp
await context.BulkInsertAsync(events, options => options
    .WithBatchSize(10_000)
    .WithParallelism(4));  // 4 concurrent connections
```

**Guidelines:**
- Set to number of CPU cores for CPU-bound workloads
- For network-bound: 2-4 connections typically optimal
- Higher parallelism = more memory usage

### Progress Callbacks

Monitor progress for long-running inserts:

```csharp
await context.BulkInsertAsync(events, options => options
    .WithBatchSize(10_000)
    .WithProgressCallback(rowsInserted =>
    {
        var percent = (double)rowsInserted / totalRows * 100;
        Console.WriteLine($"Progress: {percent:F1}% ({rowsInserted:N0} rows)");
    }));
```

The callback receives the cumulative count of rows inserted after each batch.

### Timeouts

Set command timeout for large inserts:

```csharp
await context.BulkInsertAsync(events, options => options
    .WithTimeout(TimeSpan.FromMinutes(30)));
```

### Custom Settings

Apply any ClickHouse setting to the INSERT statement:

```csharp
await context.BulkInsertAsync(events, options => options
    .WithMaxInsertThreads(8)  // Shorthand for max_insert_threads
    .WithSetting("insert_quorum", 0)
    .WithSettings(new Dictionary<string, object>
    {
        ["max_memory_usage"] = 10_000_000_000,
        ["insert_distributed_sync"] = 1
    }));
```

## Streaming Large Datasets

For datasets too large to fit in memory, use `BulkInsertStreamingAsync` with `IAsyncEnumerable`:

```csharp
// Generator function - data is never fully loaded into memory
async IAsyncEnumerable<Event> ReadEventsFromFileAsync(string path)
{
    await foreach (var line in File.ReadLinesAsync(path))
    {
        yield return ParseEvent(line);
    }
}

// Stream directly to ClickHouse
var result = await context.BulkInsertStreamingAsync(
    ReadEventsFromFileAsync("events.csv"),
    options => options.WithBatchSize(10_000));
```

### Memory Management

Streaming inserts buffer only one batch at a time, making it possible to insert billions of rows with constant memory usage:

```csharp
// Even with 1 billion rows, memory usage stays constant
var result = await context.BulkInsertStreamingAsync(
    GenerateInfiniteEvents(),  // Yields events one at a time
    options => options.WithBatchSize(50_000));
```

### Streaming from External Sources

```csharp
// From Kafka
async IAsyncEnumerable<Event> ConsumeKafkaAsync(
    IConsumer<string, Event> consumer,
    [EnumeratorCancellation] CancellationToken ct)
{
    while (!ct.IsCancellationRequested)
    {
        var result = consumer.Consume(ct);
        yield return result.Message.Value;
    }
}

// From HTTP stream
async IAsyncEnumerable<Event> ReadFromApiAsync(HttpClient client)
{
    var stream = await client.GetStreamAsync("/events");
    await foreach (var evt in JsonSerializer.DeserializeAsyncEnumerable<Event>(stream))
    {
        if (evt != null) yield return evt;
    }
}
```

## Complex Types

Bulk insert supports ClickHouse complex types including Arrays and Maps:

### Arrays

```csharp
public class Product
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    public string[] Tags { get; set; }  // Array(String)
    public int[] Quantities { get; set; }  // Array(Int32)
}

modelBuilder.Entity<Product>(entity =>
{
    entity.Property(e => e.Tags).HasColumnType("Array(String)");
    entity.Property(e => e.Quantities).HasColumnType("Array(Int32)");
});

var products = new List<Product>
{
    new()
    {
        Id = Guid.NewGuid(),
        Name = "Widget",
        Tags = new[] { "electronics", "gadgets" },
        Quantities = new[] { 10, 20, 30 }
    }
};

await context.BulkInsertAsync(products);
```

### Maps

```csharp
public class Event
{
    public Guid Id { get; set; }
    public Dictionary<string, string> Properties { get; set; }  // Map(String, String)
    public Dictionary<string, int> Metrics { get; set; }  // Map(String, Int32)
}

modelBuilder.Entity<Event>(entity =>
{
    entity.Property(e => e.Properties).HasColumnType("Map(String, String)");
    entity.Property(e => e.Metrics).HasColumnType("Map(String, Int32)");
});

var events = new List<Event>
{
    new()
    {
        Id = Guid.NewGuid(),
        Properties = new() { ["source"] = "web", ["browser"] = "chrome" },
        Metrics = new() { ["duration_ms"] = 150, ["retry_count"] = 0 }
    }
};

await context.BulkInsertAsync(events);
```

## Result Statistics

`BulkInsertAsync` returns a `ClickHouseBulkInsertResult` with performance metrics:

```csharp
var result = await context.BulkInsertAsync(events);

Console.WriteLine($"Rows: {result.RowsInserted}");
Console.WriteLine($"Batches: {result.BatchesExecuted}");
Console.WriteLine($"Time: {result.Elapsed.TotalSeconds:F2}s");
Console.WriteLine($"Throughput: {result.RowsPerSecond:N0} rows/sec");
```

| Property | Type | Description |
|----------|------|-------------|
| `RowsInserted` | `long` | Total rows inserted |
| `BatchesExecuted` | `int` | Number of batches executed |
| `Elapsed` | `TimeSpan` | Total operation time |
| `RowsPerSecond` | `double` | Calculated throughput |

## Performance Comparison

Typical performance for inserting 100,000 rows:

| Method | Time | Throughput |
|--------|------|------------|
| `AddRange` + `SaveChangesAsync` | ~50s | ~2,000 rows/sec |
| `BulkInsertAsync` (sequential) | ~0.5s | ~200,000 rows/sec |
| `BulkInsertAsync` (parallel=4) | ~0.15s | ~650,000 rows/sec |

*Results vary by hardware, network, and row complexity.*

## Complete Example

```csharp
using EF.CH.BulkInsert;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// High-performance bulk insert with all options
var result = await context.BulkInsertAsync(events, options => options
    .WithBatchSize(25_000)
    .WithParallelism(4)
    .WithFormat(ClickHouseBulkInsertFormat.Values)
    .WithAsyncInsert(wait: true)
    .WithMaxInsertThreads(8)
    .WithTimeout(TimeSpan.FromMinutes(30))
    .WithProgressCallback(rows =>
    {
        var pct = (double)rows / events.Count * 100;
        Console.WriteLine($"Progress: {pct:F1}%");
    }));

Console.WriteLine($"Inserted {result.RowsInserted:N0} rows in {result.Elapsed.TotalSeconds:F1}s");
Console.WriteLine($"Throughput: {result.RowsPerSecond:N0} rows/sec");
```

## Limitations

- **Not tracked by DbContext**: Entities are not added to the change tracker
- **MATERIALIZED columns**: Skipped (ClickHouse computes them)
- **ALIAS columns**: Skipped (virtual columns)
- **No relationships**: Foreign keys are inserted as values, not navigated
- **No identity columns**: ClickHouse doesn't support auto-increment; use GUIDs

## Options Reference

| Method | Description | Default |
|--------|-------------|---------|
| `WithBatchSize(int)` | Rows per INSERT statement | 10,000 |
| `WithFormat(format)` | Values or JsonEachRow | Values |
| `WithAsyncInsert(wait)` | Enable async_insert mode | false |
| `WithParallelism(int)` | Concurrent connections | 1 |
| `WithMaxInsertThreads(int)` | ClickHouse insert threads | null |
| `WithSetting(key, value)` | Custom ClickHouse setting | - |
| `WithSettings(dict)` | Multiple ClickHouse settings | - |
| `WithTimeout(timespan)` | Command timeout | null |
| `WithProgressCallback(fn)` | Progress callback | null |

## See Also

- [Bulk Insert Sample](../../samples/BulkInsertSample/README.md)
- [ClickHouse INSERT Docs](https://clickhouse.com/docs/en/sql-reference/statements/insert-into)
- [ClickHouse Async Inserts](https://clickhouse.com/docs/en/cloud/bestpractices/asynchronous-inserts)
