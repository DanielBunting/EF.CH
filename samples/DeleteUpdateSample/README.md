# Delete/Update Sample

Demonstrates DELETE and UPDATE operations in ClickHouse via EF.CH, including both delete strategies.

## Features Covered

1. **Lightweight DELETE** - The default strategy. Uses `DELETE FROM ... WHERE ...` syntax. Rows are marked as deleted immediately and filtered from subsequent queries. Physical deletion occurs during background merges. Returns the affected row count.

2. **Mutation DELETE** - Uses `ALTER TABLE ... DELETE WHERE ...` syntax. An asynchronous operation that rewrites data parts in the background. Does not return an accurate affected row count. Best for bulk maintenance operations.

3. **ExecuteUpdateAsync** - Uses `ALTER TABLE ... UPDATE ... SET ... WHERE ...` syntax. Supports setting multiple columns in a single operation. Like mutation deletes, updates are asynchronous.

4. **Strategy Comparison** - Side-by-side demonstration of both delete strategies on identical data, showing the behavioral differences.

## Delete Strategy Configuration

```csharp
// Lightweight (default) -- synchronous, returns affected count
options.UseClickHouse(connectionString, o =>
    o.UseDeleteStrategy(ClickHouseDeleteStrategy.Lightweight));

// Mutation -- asynchronous, does not return accurate count
options.UseClickHouse(connectionString, o =>
    o.UseDeleteStrategy(ClickHouseDeleteStrategy.Mutation));
```

## Prerequisites

- Docker (for ClickHouse)
- .NET 8.0 SDK

## Running

```bash
# Start ClickHouse
docker run -d --name clickhouse -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server:latest

# Run the sample
dotnet run --project samples/DeleteUpdateSample/

# Or with a custom connection string
dotnet run --project samples/DeleteUpdateSample/ -- "Host=localhost;Port=8123;Database=default"
```

## Key Concepts

- ClickHouse mutations (ALTER TABLE UPDATE/DELETE) are asynchronous background operations
- `await Task.Delay(500)` after mutations allows ClickHouse time to process them before querying
- In production, check `system.mutations` table for mutation completion status
- Lightweight DELETE is preferred for normal application use; Mutation DELETE is for bulk maintenance
- ClickHouse does not support traditional transactions -- mutations cannot be rolled back
- UPDATE operations always use ALTER TABLE UPDATE internally (there is no lightweight variant)
