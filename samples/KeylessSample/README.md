# KeylessSample

Demonstrates `UseKeylessEntitiesByDefault()` for append-only analytics workloads.

## What This Shows

- Enabling keyless entities by default with `UseKeylessEntitiesByDefault()`
- Keyless entities for append-only data (PageView, ApiRequest)
- Overriding keyless default with `HasKey()` (User)
- Combining keyless with partitioning and TTL

## Why Keyless?

ClickHouse is optimized for append-only analytics workloads where:
- Data is write-once, read-many
- You don't need to update or delete individual rows
- Tables don't need primary keys for entity tracking

With `UseKeylessEntitiesByDefault()`, all entities are keyless unless you explicitly call `HasKey()`.

## Prerequisites

- .NET 10.0+
- ClickHouse server running on localhost:8123
- EF Core tools: `dotnet tool install --global dotnet-ef`

## Running

```bash
dotnet run
```

To see generated SQL:

```bash
dotnet ef migrations add InitialCreate
dotnet ef migrations script
```

## Entities

### PageView (Keyless)

Append-only page view tracking - no key needed:

```csharp
public class PageView
{
    public DateTime Timestamp { get; set; }
    public string PageUrl { get; set; }
    public string? UserId { get; set; }
    public int DurationMs { get; set; }
}
```

Configuration (no `HasKey()` needed):
```csharp
entity.UseMergeTree("Timestamp", "PageUrl");
entity.HasPartitionBy("toYYYYMMDD(\"Timestamp\")");
```

### ApiRequest (Keyless with TTL)

API logging with automatic expiration:

```csharp
public class ApiRequest
{
    public DateTime Timestamp { get; set; }
    public string Endpoint { get; set; }
    public int StatusCode { get; set; }
    public int ResponseTimeMs { get; set; }
}
```

Configuration:
```csharp
entity.UseMergeTree("Timestamp", "Endpoint");
entity.HasPartitionBy("toYYYYMM(\"Timestamp\")");
entity.HasTtl("\"Timestamp\" + INTERVAL 90 DAY");
```

### User (With Key)

User profile that needs update semantics:

```csharp
public class User
{
    public Guid Id { get; set; }
    public string Email { get; set; }
    public DateTime UpdatedAt { get; set; }
}
```

Configuration (explicit key overrides default):
```csharp
entity.HasKey(e => e.Id);  // Override keyless default
entity.UseReplacingMergeTree("UpdatedAt", "Id");
```

## Enabling Keyless by Default

```csharp
protected override void OnConfiguring(DbContextOptionsBuilder options)
{
    options.UseClickHouse(
        "Host=localhost;Database=keyless_sample",
        o => o.UseKeylessEntitiesByDefault());
}
```

## Generated DDL

### PageViews (Keyless)

```sql
CREATE TABLE "PageViews" (
    "Timestamp" DateTime64(3) NOT NULL,
    "PageUrl" String NOT NULL,
    "UserId" String NULL,
    "Referrer" String NULL,
    "UserAgent" String NULL,
    "DurationMs" Int32 NOT NULL
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD("Timestamp")
ORDER BY ("Timestamp", "PageUrl")
```

### ApiRequests (Keyless with TTL)

```sql
CREATE TABLE "ApiRequests" (
    "Timestamp" DateTime64(3) NOT NULL,
    "Endpoint" String NOT NULL,
    "Method" String NOT NULL,
    "StatusCode" Int32 NOT NULL,
    "ResponseTimeMs" Int32 NOT NULL,
    "RequestBody" String NULL
)
ENGINE = MergeTree
PARTITION BY toYYYYMM("Timestamp")
ORDER BY ("Timestamp", "Endpoint")
TTL "Timestamp" + INTERVAL 90 DAY
```

### Users (With Key)

```sql
CREATE TABLE "Users" (
    "Id" UUID NOT NULL,
    "Email" String NOT NULL,
    "Name" String NOT NULL,
    "CreatedAt" DateTime64(3) NOT NULL,
    "UpdatedAt" DateTime64(3) NOT NULL
)
ENGINE = ReplacingMergeTree("UpdatedAt")
ORDER BY ("Id")
```

## Keyless vs Keyed Behavior

| Operation | Keyless | With Key |
|-----------|---------|----------|
| Insert | ✅ Works | ✅ Works |
| Query | ✅ Works | ✅ Works |
| Find() | ❌ Returns null | ✅ Works |
| Update tracking | ❌ Not tracked | ✅ Tracked |
| Remove tracking | ❌ Not tracked | ✅ Tracked |
| ExecuteDeleteAsync | ✅ Works | ✅ Works |

## Learn More

- [Keyless Entities Documentation](../../docs/features/keyless-entities.md)
- [ReplacingMergeTree](../../docs/engines/replacing-mergetree.md)
- [TTL](../../docs/features/ttl.md)
