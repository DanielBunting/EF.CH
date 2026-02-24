# Table Functions

Table functions provide server-side access to external data sources without creating a persistent table. EF.CH wraps ClickHouse table functions as `IQueryable<T>` extension methods, enabling LINQ composition over S3 files, URLs, remote servers, local files, and cluster nodes.

All table function extensions require the entity type `T` to be registered in the EF Core model. Column structure is automatically inferred from the model when not specified explicitly.

## FromS3

Query data from S3-compatible object storage.

```csharp
var data = await context.FromS3<LogEntry>(
    path: "https://my-bucket.s3.amazonaws.com/logs/2024/*.parquet",
    format: "Parquet"
).Where(x => x.Level == "ERROR")
 .ToListAsync();
```

Generated SQL:

```sql
SELECT * FROM s3(
    'https://my-bucket.s3.amazonaws.com/logs/2024/*.parquet',
    'Parquet',
    'Id UInt64, Level String, Message String, Timestamp DateTime64(3)'
)
```

With authentication:

```csharp
var data = await context.FromS3<LogEntry>(
    path: "https://my-bucket.s3.amazonaws.com/logs/*.parquet",
    format: "Parquet",
    accessKeyId: "AKIAIOSFODNN7EXAMPLE",
    secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
).ToListAsync();
```

## FromUrl

Query data from any HTTP/HTTPS URL.

```csharp
var data = await context.FromUrl<PriceData>(
    url: "https://api.example.com/prices.csv",
    format: "CSVWithNames"
).Where(x => x.Price > 100)
 .ToListAsync();
```

Generated SQL:

```sql
SELECT * FROM url(
    'https://api.example.com/prices.csv',
    'CSVWithNames',
    'Symbol String, Price Float64, Timestamp DateTime64(3)'
)
```

## FromRemote

Query a table on a remote ClickHouse server.

```csharp
var data = await context.FromRemote<Order>(
    addresses: "remote-clickhouse:9000",
    database: "production",
    table: "orders"
).Where(x => x.Amount > 1000)
 .ToListAsync();
```

Generated SQL:

```sql
SELECT * FROM remote(
    'remote-clickhouse:9000',
    'production',
    'orders'
)
```

With authentication:

```csharp
var data = await context.FromRemote<Order>(
    addresses: "dc2-clickhouse:9000",
    database: "production",
    table: "orders",
    user: "readonly",
    password: "secret"
).ToListAsync();
```

## FromFile

Query a local file on the ClickHouse server.

```csharp
var data = await context.FromFile<ImportRecord>(
    path: "/var/data/import.csv",
    format: "CSVWithNames"
).ToListAsync();
```

Generated SQL:

```sql
SELECT * FROM file(
    '/var/data/import.csv',
    'CSVWithNames',
    'Id UInt64, Name String, Value Float64'
)
```

> **Note:** The file path is resolved on the ClickHouse server, not on the application host. The ClickHouse server process must have read access to the file.

## FromCluster

Query a table across all nodes in a ClickHouse cluster.

```csharp
var data = await context.FromCluster<Event>(
    clusterName: "analytics_cluster",
    database: "events_db",
    table: "events"
).Where(x => x.EventDate >= new DateOnly(2024, 1, 1))
 .ToListAsync();
```

Generated SQL:

```sql
SELECT * FROM cluster(
    'analytics_cluster',
    'events_db',
    'events'
)
```

## Structure Inference

When the `structure` parameter is omitted (the default), EF.CH infers the column structure from the entity's EF Core model configuration. Column names and ClickHouse types are read from the model metadata.

To override the inferred structure, pass a custom structure string:

```csharp
var data = await context.FromS3<LogEntry>(
    path: "s3://bucket/data.parquet",
    format: "Parquet",
    structure: "id UInt64, message String, ts DateTime64(3)"
).ToListAsync();
```

## Composing with LINQ

All table function results are `IQueryable<T>` and support standard LINQ operations, ClickHouse query modifiers, and aggregations.

```csharp
var summary = await context.FromS3<LogEntry>(
    "s3://bucket/logs/*.parquet", "Parquet"
)
.Where(x => x.Level == "ERROR")
.GroupBy(x => x.Source)
.Select(g => new { Source = g.Key, Count = g.Count() })
.OrderByDescending(x => x.Count)
.Take(10)
.ToListAsync();
```

## See Also

- [External Entities](external-entities.md) -- persistent entity mappings for external databases
- [Parameterized Views](parameterized-views.md) -- reusable query templates
- [Clustering Overview](../clustering/overview.md) -- cluster-level data access
