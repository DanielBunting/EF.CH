# Keyless Entities

ClickHouse is designed for append-only analytical workloads where most tables do not have a traditional primary key. EF.CH provides `UseKeylessEntitiesByDefault()` to align Entity Framework Core's model with this reality, making all entities keyless unless explicitly configured otherwise.

---

## Enabling Keyless by Default

```csharp
public class AnalyticsDbContext : DbContext
{
    public DbSet<PageView> PageViews => Set<PageView>();
    public DbSet<ClickEvent> ClickEvents => Set<ClickEvent>();
    public DbSet<UserSession> UserSessions => Set<UserSession>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse("Host=localhost;Port=8123;Database=analytics",
            o => o.UseKeylessEntitiesByDefault());
}
```

With this option, all entities default to `HasNoKey()`. EF Core treats them as read-only, with no change tracking or identity resolution.

### Overriding for Specific Entities

Entities that need keys can opt in with `HasKey()` in `OnModelCreating`. The convention runs early, so explicit `HasKey()` takes precedence.

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    // PageView and ClickEvent remain keyless (from convention)

    // UserSession gets an explicit key for change tracking
    modelBuilder.Entity<UserSession>(entity =>
    {
        entity.HasKey(x => x.SessionId);
        entity.UseReplacingMergeTree(x => x.Version, x => x.SessionId);
    });
}
```

---

## Per-Entity Keyless Configuration

Without the global option, individual entities can be configured as keyless using the standard EF Core API.

```csharp
modelBuilder.Entity<PageView>(entity =>
{
    entity.HasNoKey();
    entity.UseMergeTree(x => new { x.Timestamp, x.PagePath });
});
```

---

## Why Keyless in ClickHouse

In traditional OLTP databases, primary keys serve three purposes: uniqueness constraints, identity for UPDATE/DELETE operations, and relationship navigation. ClickHouse does not enforce uniqueness, does not use keys for mutations, and does not support foreign keys. This means:

1. **No uniqueness guarantee**: ClickHouse's ORDER BY defines physical sort order, not uniqueness. Duplicate rows are allowed and common.

2. **Append-only ingestion**: Data is typically bulk-inserted and never updated. ReplacingMergeTree provides eventual deduplication, but this happens during background merges, not at INSERT time.

3. **No foreign keys**: Joins are supported in queries but there are no FK constraints. Relationships are managed at the application level.

4. **Change tracking overhead**: EF Core's change tracker requires a key to identify entities. For read-only analytical queries, this overhead provides no benefit.

---

## When to Use Keys vs. Keyless

| Pattern | Use Key | Use Keyless |
|---|---|---|
| Append-only event logs | | Yes |
| Read-only analytics tables | | Yes |
| Sensor/metric time-series | | Yes |
| Tables with SaveChanges inserts | Yes | |
| ReplacingMergeTree with dedup | Yes | |
| Tables needing EF change tracking | Yes | |
| Materialized view targets | | Yes |

Use keys when you need EF Core's change tracking for insert operations via `SaveChangesAsync()`, or when using `ReplacingMergeTree` where a key defines the deduplication identity.

Use keyless when the table is append-only, read-only from EF Core's perspective, or when data is inserted via bulk insert or INSERT...SELECT.

---

## Querying Keyless Entities

Keyless entities support all read operations. They can be queried, filtered, projected, and aggregated normally.

```csharp
// Standard query
var recentViews = await context.PageViews
    .Where(v => v.Timestamp >= DateTime.UtcNow.AddDays(-1))
    .ToListAsync();

// Aggregation
var topPages = await context.PageViews
    .GroupBy(v => v.PagePath)
    .Select(g => new { Path = g.Key, Views = g.Count() })
    .OrderByDescending(x => x.Views)
    .Take(10)
    .ToListAsync();

// Bulk insert (does not require a key)
await context.BulkInsertAsync(newPageViews);
```

### Limitations

Keyless entities cannot use:
- `SaveChangesAsync()` for inserts (use `BulkInsertAsync()` instead)
- `ExecuteUpdateAsync()` or `ExecuteDeleteAsync()`
- EF Core change tracking (`Attach`, `Update`, `Remove`)
- Navigation properties with lazy/eager loading

---

## Complete Example

```csharp
public class PageView
{
    public DateTime Timestamp { get; set; }
    public string PagePath { get; set; } = "";
    public string UserAgent { get; set; } = "";
    public Guid? UserId { get; set; }
    public int ResponseTimeMs { get; set; }
}

public class ClickEvent
{
    public DateTime Timestamp { get; set; }
    public string ElementId { get; set; } = "";
    public string PagePath { get; set; } = "";
    public Guid? UserId { get; set; }
}

public class AnalyticsDbContext : DbContext
{
    public DbSet<PageView> PageViews => Set<PageView>();
    public DbSet<ClickEvent> ClickEvents => Set<ClickEvent>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse("Host=localhost;Port=8123;Database=analytics",
            o => o.UseKeylessEntitiesByDefault());

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PageView>(entity =>
        {
            entity.UseMergeTree(x => new { x.Timestamp, x.PagePath });
            entity.HasPartitionByMonth(x => x.Timestamp);
            entity.HasTtl(x => x.Timestamp, ClickHouseInterval.Months(6));

            entity.Property(x => x.Timestamp).HasTimestampCodec();
            entity.Property(x => x.PagePath).HasLowCardinality();
        });

        modelBuilder.Entity<ClickEvent>(entity =>
        {
            entity.UseMergeTree(x => new { x.Timestamp, x.ElementId });
            entity.HasPartitionByMonth(x => x.Timestamp);
            entity.HasTtl(x => x.Timestamp, ClickHouseInterval.Months(3));

            entity.Property(x => x.Timestamp).HasTimestampCodec();
            entity.Property(x => x.ElementId).HasLowCardinality();
        });
    }
}
```

Insert data using bulk insert:

```csharp
await using var context = new AnalyticsDbContext();
await context.Database.EnsureCreatedAsync();

var pageViews = Enumerable.Range(0, 10000).Select(i => new PageView
{
    Timestamp = DateTime.UtcNow.AddSeconds(-i),
    PagePath = $"/page/{i % 100}",
    UserAgent = "Mozilla/5.0",
    UserId = i % 3 == 0 ? null : Guid.NewGuid(),
    ResponseTimeMs = Random.Shared.Next(50, 500)
});

await context.BulkInsertAsync(pageViews);
```

---

## See Also

- [Column Features](column-features.md) -- compression codecs, computed columns, LowCardinality
- [TTL](ttl.md) -- automatic data expiration
- [Partitioning](partitioning.md) -- PARTITION BY strategies
