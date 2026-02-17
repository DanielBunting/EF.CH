# BulkInsertSample

Demonstrates high-performance bulk insert operations that bypass EF Core change tracking for maximum throughput.

## What This Shows

- `BulkInsertAsync()` for inserting collections of entities
- `BulkInsertStreamingAsync()` for inserting IAsyncEnumerable streams
- Batch size configuration for memory control
- Progress callbacks for monitoring large inserts
- Values vs JSONEachRow format options
- Async insert mode for buffered inserts
- Parallel insertion using multiple connections
- Complex types: Arrays and Maps
- Performance comparison vs standard EF Core

## API Reference

| Method | Purpose | Example |
|--------|---------|---------|
| `BulkInsertAsync()` | Insert a collection | `context.BulkInsertAsync(entities)` |
| `BulkInsertStreamingAsync()` | Insert async stream | `context.BulkInsertStreamingAsync(asyncStream)` |
| `DbSet.BulkInsertAsync()` | Insert via DbSet | `context.Events.BulkInsertAsync(events)` |

## Options

| Option | Description | Default |
|--------|-------------|---------|
| `WithBatchSize(n)` | Rows per batch | 10,000 |
| `WithFormat(format)` | Values or JSONEachRow | Values |
| `WithAsyncInsert(wait)` | Enable async_insert | false |
| `WithParallelism(n)` | Parallel connections | 1 |
| `WithMaxInsertThreads(n)` | ClickHouse threads | null |
| `WithProgressCallback(fn)` | Progress reporting | null |
| `WithTimeout(timespan)` | Command timeout | null |
| `WithSetting(k, v)` | Custom ClickHouse setting | - |

## Prerequisites

- .NET 8.0+
- Docker (for running ClickHouse)

## Running

Start ClickHouse using docker-compose:

```bash
docker-compose up -d
```

Then run the sample:

```bash
dotnet run
```

Stop ClickHouse when done:

```bash
docker-compose down
```

## Expected Output

```
Bulk Insert Sample
==================

Creating database and tables...

--- Basic Bulk Insert ---
Inserted 10,000 rows
Batches: 1
Elapsed: 45ms
Throughput: 222,222 rows/sec

--- Bulk Insert with Batch Size ---
Inserted 25,000 rows in 5 batches
Throughput: 250,000 rows/sec

--- Progress Callback ---
  Progress: 10,000 rows inserted
  Progress: 20,000 rows inserted
  Progress: 30,000 rows inserted
Complete: 30,000 rows in 120ms

--- Streaming Insert (IAsyncEnumerable) ---
  Streamed: 10,000 rows
  Streamed: 20,000 rows
  Streamed: 30,000 rows
  Streamed: 40,000 rows
  Streamed: 50,000 rows
Complete: 50,000 rows in 200ms
Throughput: 250,000 rows/sec

--- JSONEachRow Format ---
Inserted 10,000 rows using JSONEachRow format
Throughput: 200,000 rows/sec

--- Async Insert Mode ---
Inserted 10,000 rows with async_insert=1
Throughput: 180,000 rows/sec

--- Parallel Insertion ---
  Progress: 10,000 rows
  Progress: 20,000 rows
  Progress: 30,000 rows
  ...
Complete: 100,000 rows in 300ms
Throughput: 333,333 rows/sec (4 parallel connections)

--- Complex Types (Arrays, Maps) ---
Inserted 5,000 products with arrays and maps
Throughput: 100,000 rows/sec

--- Custom ClickHouse Settings ---
Inserted 10,000 rows with custom settings

--- Performance Comparison ---

Standard EF Core (AddRange + SaveChangesAsync) - 5,000 rows:
  Elapsed: 2500ms
  Throughput: 2,000 rows/sec

Bulk Insert - 5,000 rows:
  Elapsed: 25ms
  Throughput: 200,000 rows/sec

Bulk insert is 100.0x faster!

--- Via DbSet ---
Inserted 2,000 products via DbSet.BulkInsertAsync()

Done!
```

## Key Code

### Basic Bulk Insert

```csharp
var events = GenerateEvents(10_000);
var result = await context.BulkInsertAsync(events);

Console.WriteLine($"Inserted {result.RowsInserted} rows");
Console.WriteLine($"Throughput: {result.RowsPerSecond:N0} rows/sec");
```

### With Options

```csharp
var result = await context.BulkInsertAsync(events, options => options
    .WithBatchSize(5_000)
    .WithProgressCallback(rows => Console.WriteLine($"Progress: {rows}"))
    .WithTimeout(TimeSpan.FromMinutes(5)));
```

### Streaming Large Datasets

```csharp
// Generator function - data is never fully loaded into memory
async IAsyncEnumerable<Event> ReadFromFileAsync(string path)
{
    await foreach (var line in File.ReadLinesAsync(path))
    {
        yield return ParseEvent(line);
    }
}

// Stream directly to ClickHouse
var result = await context.BulkInsertStreamingAsync(
    ReadFromFileAsync("events.csv"),
    options => options.WithBatchSize(10_000));
```

### Parallel Insertion

```csharp
// Uses 4 separate connections for parallel batch insertion
var result = await context.BulkInsertAsync(events, options => options
    .WithBatchSize(10_000)
    .WithParallelism(4));
```

### Complex Types

```csharp
public class Product
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    public string[] Tags { get; set; }  // Array(String)
    public Dictionary<string, string> Metadata { get; set; }  // Map(String, String)
}

var products = new List<Product>
{
    new()
    {
        Id = Guid.NewGuid(),
        Name = "Widget",
        Tags = new[] { "electronics", "gadgets" },
        Metadata = new() { ["sku"] = "W-001", ["warehouse"] = "NYC" }
    }
};

await context.BulkInsertAsync(products);
```

### Via DbSet

```csharp
// Both approaches are equivalent
await context.BulkInsertAsync(events);
await context.Events.BulkInsertAsync(events);
```

## Result Statistics

The `ClickHouseBulkInsertResult` provides:

| Property | Description |
|----------|-------------|
| `RowsInserted` | Total rows inserted |
| `BatchesExecuted` | Number of batches |
| `Elapsed` | Total time taken |
| `RowsPerSecond` | Calculated throughput |

## When to Use Each Format

| Format | Best For |
|--------|----------|
| `Values` (default) | Simple types, maximum speed |
| `JsonEachRow` | Complex nested types, debugging |

## Learn More

- [Bulk Insert Documentation](../../docs/features/bulk-insert.md)
- [ClickHouse INSERT Docs](https://clickhouse.com/docs/en/sql-reference/statements/insert-into)
