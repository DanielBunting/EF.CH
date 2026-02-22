# Bulk Operations Sample

Demonstrates high-throughput data operations supported by EF.CH for ClickHouse.

## Features Covered

1. **BulkInsertAsync** - Insert 10,000 records with configurable batch size and progress callbacks. Bypasses EF Core change tracking for maximum throughput.

2. **BulkInsertStreamingAsync** - Stream records from an `IAsyncEnumerable<T>` without loading all data into memory. Ideal for large datasets or data pipeline scenarios.

3. **INSERT...SELECT** - Server-side data movement between tables using `ExecuteInsertFromQueryAsync`. The data never leaves ClickHouse, making this efficient for archiving or ETL operations.

4. **OPTIMIZE TABLE** - Force background merges to consolidate data parts. Supports fluent configuration with `WithFinal()`, `WithPartition()`, and `WithDeduplicate()`.

5. **Export** - Export query results as CSV (`ToCsvAsync`), JSON (`ToJsonAsync`), or JSON Lines (`ToJsonLinesAsync`). Uses direct HTTP requests to ClickHouse for native format support.

## Prerequisites

- Docker (for ClickHouse)
- .NET 8.0 SDK

## Running

```bash
# Start ClickHouse
docker run -d --name clickhouse -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server:latest

# Run the sample
dotnet run --project samples/BulkOperationsSample/

# Or with a custom connection string
dotnet run --project samples/BulkOperationsSample/ -- "Host=localhost;Port=8123;Database=default"
```

## Key Concepts

- `BulkInsertAsync` bypasses EF Core change tracking entirely for insert performance
- `BulkInsertStreamingAsync` processes records as they arrive, keeping memory usage constant
- INSERT...SELECT operates entirely server-side -- no data serialization or network transfer for the moved rows
- `await Task.Delay(500)` after OPTIMIZE TABLE allows ClickHouse time to complete background merges
- Export methods use ClickHouse's native FORMAT clause via HTTP interface
