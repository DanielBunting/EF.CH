# Getting Started

## Installation

```bash
dotnet add package EF.CH
```

EF.CH targets .NET 8.0 and depends on:

- `Microsoft.EntityFrameworkCore.Relational` 8.0
- `ClickHouse.Driver` (ADO.NET driver for ClickHouse)

## Running ClickHouse

You need a ClickHouse server to connect to. There are three common ways to run one locally.

### Option 1: Docker (single instance)

```bash
docker run -d \
  --name clickhouse \
  -p 8123:8123 \
  -p 9000:9000 \
  clickhouse/clickhouse-server:latest
```

Port 8123 is the HTTP interface. Port 9000 is the native TCP interface used by the ClickHouse.Driver ADO.NET client.

### Option 2: Docker Compose

```yaml
# docker-compose.yml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - clickhouse-data:/var/lib/clickhouse
    ulimits:
      nofile:
        soft: 262144
        hard: 262144

volumes:
  clickhouse-data:
```

```bash
docker compose up -d
```

### Option 3: Testcontainers (for tests)

For integration tests, use `Testcontainers.ClickHouse` to spin up a disposable ClickHouse instance per test class.

```bash
dotnet add package Testcontainers.ClickHouse
```

```csharp
using Testcontainers.ClickHouse;

public class MyTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    public async Task InitializeAsync() => await _container.StartAsync();
    public async Task DisposeAsync() => await _container.DisposeAsync();
}
```

> **Note:** Testcontainers requires Docker running on the host machine. The container is created fresh for each test class and destroyed afterward.

## Connection Strings

The connection string format uses semicolon-separated key-value pairs:

```
Host=localhost;Port=8123;Database=default
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `Host` | `localhost` | ClickHouse server hostname |
| `Port` | `8123` | HTTP interface port |
| `Database` | `default` | Target database name |
| `Username` | `default` | ClickHouse user |
| `Password` | (empty) | ClickHouse password |

Example with authentication:

```
Host=clickhouse.example.com;Port=8123;Database=analytics;Username=app_user;Password=secret
```

## First Project Walkthrough

This walkthrough creates a simple analytics table, inserts data, and queries it.

### 1. Define an entity

```csharp
public class PageView
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Url { get; set; } = string.Empty;
    public string UserAgent { get; set; } = string.Empty;
    public int ResponseTimeMs { get; set; }
}
```

### 2. Create a DbContext

```csharp
using Microsoft.EntityFrameworkCore;
using EF.CH.Extensions;

public class AnalyticsDbContext : DbContext
{
    public DbSet<PageView> PageViews { get; set; }

    public AnalyticsDbContext(DbContextOptions<AnalyticsDbContext> options)
        : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PageView>(entity =>
        {
            entity.HasNoKey();

            entity.UseMergeTree(x => new { x.Timestamp, x.Url })
                .HasPartitionByMonth(x => x.Timestamp);
        });
    }
}
```

> **Note:** ClickHouse tables always require an ENGINE. `UseMergeTree()` configures the MergeTree engine and its ORDER BY key. The ORDER BY key determines how data is physically sorted on disk -- choose columns that match your most common query filters.

### 3. Register the context

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<AnalyticsDbContext>(options =>
    options.UseClickHouse("Host=localhost;Port=8123;Database=default"));
```

Or in a console application:

```csharp
var options = new DbContextOptionsBuilder<AnalyticsDbContext>()
    .UseClickHouse("Host=localhost;Port=8123;Database=default")
    .Options;

using var context = new AnalyticsDbContext(options);
```

### 4. Create the table

EF.CH supports migrations, but for a quick start you can use `EnsureCreated`:

```csharp
await context.Database.EnsureCreatedAsync();
```

This generates and executes:

```sql
CREATE TABLE "PageViews" (
    "Id" UUID,
    "Timestamp" DateTime64(3),
    "Url" String,
    "UserAgent" String,
    "ResponseTimeMs" Int32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM("Timestamp")
ORDER BY ("Timestamp", "Url")
```

### 5. Insert data

For small batches, use `AddRange` and `SaveChanges`:

```csharp
context.PageViews.AddRange(
    new PageView
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow,
        Url = "/home",
        UserAgent = "Mozilla/5.0",
        ResponseTimeMs = 42
    },
    new PageView
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow,
        Url = "/api/data",
        UserAgent = "curl/7.88",
        ResponseTimeMs = 15
    }
);

await context.SaveChangesAsync();
```

For large volumes, use bulk insert:

```csharp
var pageViews = Enumerable.Range(0, 100_000).Select(i => new PageView
{
    Id = Guid.NewGuid(),
    Timestamp = DateTime.UtcNow.AddSeconds(-i),
    Url = $"/page/{i % 100}",
    UserAgent = "bot",
    ResponseTimeMs = Random.Shared.Next(5, 500)
});

await context.BulkInsertAsync(pageViews);
```

### 6. Query data

Standard LINQ queries translate to ClickHouse SQL:

```csharp
// Filter and project
var slowPages = await context.PageViews
    .Where(p => p.ResponseTimeMs > 200)
    .OrderByDescending(p => p.ResponseTimeMs)
    .Take(10)
    .ToListAsync();

// Aggregation
var stats = await context.PageViews
    .GroupBy(p => p.Url)
    .Select(g => new
    {
        Url = g.Key,
        AvgResponseTime = g.Average(p => p.ResponseTimeMs),
        Count = g.Count()
    })
    .OrderByDescending(x => x.Count)
    .ToListAsync();
```

The aggregation query generates:

```sql
SELECT "p"."Url", avgOrNull(CAST("p"."ResponseTimeMs" AS Float64)), count()
FROM "PageViews" AS "p"
GROUP BY "p"."Url"
ORDER BY count() DESC
```

> **Note:** EF.CH translates `Average()` to `avgOrNull()` and `Sum()` to `sumOrNull()` to avoid exceptions on empty result sets. This is a ClickHouse-specific safety measure.

## Complete Minimal Example

```csharp
using Microsoft.EntityFrameworkCore;
using EF.CH.Extensions;

// Entity
public class Event
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Category { get; set; } = string.Empty;
    public double Value { get; set; }
}

// DbContext
public class AppDbContext : DbContext
{
    public DbSet<Event> Events { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse("Host=localhost;Port=8123;Database=default");

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.Timestamp, x.Category });
        });
    }
}

// Usage
using var context = new AppDbContext();
await context.Database.EnsureCreatedAsync();

context.Events.Add(new Event
{
    Id = Guid.NewGuid(),
    Timestamp = DateTime.UtcNow,
    Category = "click",
    Value = 1.0
});
await context.SaveChangesAsync();

var events = await context.Events
    .Where(e => e.Category == "click")
    .ToListAsync();

Console.WriteLine($"Found {events.Count} events");
```

## Troubleshooting

### "Connection refused" on startup

ClickHouse is not running or not listening on the expected port. Verify the container is up:

```bash
docker ps | grep clickhouse
```

Check connectivity:

```bash
curl http://localhost:8123/ping
```

### "Table already exists" on EnsureCreated

`EnsureCreatedAsync()` does not drop existing tables. If you change your model, drop the table manually first:

```sql
DROP TABLE IF EXISTS "PageViews"
```

Or use migrations for schema evolution.

### Queries return empty results after insert

ClickHouse inserts are asynchronous at the storage level. Data should be available immediately for queries, but if you are using `ReplacingMergeTree` or other deduplicating engines, rows may not be merged yet. Use `.Final()` to get the deduplicated view:

```csharp
var results = await context.Events.Final().ToListAsync();
```

### "No data to insert" error

This occurs when calling `SaveChangesAsync()` with no tracked entities. Ensure you called `Add()` or `AddRange()` before saving.

### Mutations (UPDATE/DELETE) seem to do nothing

ClickHouse mutations via `ALTER TABLE UPDATE` and `ALTER TABLE DELETE` are asynchronous. They execute in the background and may take time to complete. Use `ExecuteUpdateAsync` / `ExecuteDeleteAsync` for lightweight operations, and allow time (or call `OPTIMIZE TABLE ... FINAL`) for the changes to materialize.

### Tests fail with "Docker is not running"

Testcontainers requires Docker. On macOS, ensure Docker Desktop is running. On Linux, ensure the Docker daemon is active:

```bash
systemctl status docker
```

## See Also

- [ClickHouse for EF Developers](clickhouse-for-ef-developers.md) -- mental model differences from traditional RDBMS
- [Limitations](limitations.md) -- unsupported EF Core features and workarounds
- [Engines Overview](engines/overview.md) -- choosing the right table engine
- [MergeTree Engine](engines/mergetree.md) -- detailed MergeTree configuration
