# QuickStart Sample

A minimal example showing how to get started with EF.CH, the Entity Framework Core provider for ClickHouse.

## What it demonstrates

- Defining an entity (`Event`) and a `DbContext` with ClickHouse configuration
- Configuring the **MergeTree** engine with `UseMergeTree` for ORDER BY
- Monthly partitioning with `HasPartitionByMonth`
- Creating tables via `EnsureCreatedAsync`
- Inserting rows with `AddRange` + `SaveChangesAsync`
- Aggregation queries using `GroupBy`, `Count`, and `Sum`
- Filtered and ordered queries with standard LINQ

## Prerequisites

- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)
- A running ClickHouse instance (Docker is the easiest option)

## How to run

1. Start a ClickHouse server:

```bash
docker run -d --name clickhouse -p 8123:8123 clickhouse/clickhouse-server:latest
```

2. Run the sample:

```bash
dotnet run
```

## Expected output

```
=== EF.CH QuickStart Sample ===

[1] Creating table...
    Table 'Events' created.

[2] Inserting sample data...
    Inserted 8 events.

[3] Aggregating events by type...
    PageView     Count=3  TotalAmount=0.00
    Purchase     Count=3  TotalAmount=259.48
    SignUp       Count=2  TotalAmount=0.00

[4] Querying purchases over $50...
    2025-01-01 12:00:00  Amount=129.99
    2025-01-01 10:00:00  Amount=79.50

=== Done ===
```

Note: Exact timestamps and ordering may vary depending on when you run the sample.
