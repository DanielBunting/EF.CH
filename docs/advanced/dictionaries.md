# Dictionaries

ClickHouse dictionaries provide low-latency key-value lookups backed by external or internal data sources. EF.CH supports defining dictionaries in the EF Core model with full control over source, layout, lifetime, and defaults.

## From ClickHouse Table

The most common pattern sources a dictionary from a ClickHouse table using `AsDictionary` with a projection and optional filter.

```csharp
public class Country
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string IsoCode { get; set; }
    public bool IsActive { get; set; }
}

public class CountryLookup
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string IsoCode { get; set; }
}
```

```csharp
modelBuilder.Entity<CountryLookup>(entity =>
{
    entity.AsDictionary<CountryLookup, Country>(cfg => cfg
        .HasKey(x => x.Id)
        .FromTable(
            projection: c => new CountryLookup
            {
                Id = c.Id,
                Name = c.Name,
                IsoCode = c.IsoCode
            },
            filter: q => q.Where(c => c.IsActive)
        )
        .UseHashedLayout()
        .HasLifetime(minSeconds: 60, maxSeconds: 300)
        .HasDefault(x => x.Name, "Unknown")
    );
});
```

Generated DDL:

```sql
CREATE DICTIONARY "CountryLookup"
(
    "Id" UInt32,
    "Name" String DEFAULT 'Unknown',
    "IsoCode" String
)
PRIMARY KEY "Id"
SOURCE(CLICKHOUSE(TABLE 'Country' WHERE 'IsActive = 1'))
LIFETIME(MIN 60 MAX 300)
LAYOUT(HASHED());
```

## From PostgreSQL

```csharp
entity.AsDictionary<CountryLookup, Country>(cfg => cfg
    .HasKey(x => x.Id)
    .FromPostgreSql(pg => pg
        .FromTable("countries", schema: "public")
        .Connection(c => c
            .HostPort(env: "PG_HOST")
            .Credentials("PG_USER", "PG_PASSWORD")
        )
    )
    .UseHashedLayout()
    .HasLifetime(minSeconds: 60, maxSeconds: 300)
);
```

## From MySQL

```csharp
entity.AsDictionary<CountryLookup, Country>(cfg => cfg
    .HasKey(x => x.Id)
    .FromMySql(my => my
        .FromTable("countries")
        .Connection(c => c
            .HostPort(env: "MYSQL_HOST")
            .Credentials("MYSQL_USER", "MYSQL_PASSWORD")
        )
    )
    .UseHashedLayout()
);
```

## From HTTP

```csharp
entity.AsDictionary<CountryLookup, Country>(cfg => cfg
    .HasKey(x => x.Id)
    .FromHttp(http => http
        .Url("https://api.example.com/countries")
        .Format("JSONEachRow")
    )
    .UseHashedLayout()
    .HasLifetime(minSeconds: 300, maxSeconds: 600)
);
```

## From Redis

```csharp
entity.AsDictionary<CountryLookup, Country>(cfg => cfg
    .HasKey(x => x.Id)
    .FromRedis(r => r
        .Connection(c => c
            .HostPort(env: "REDIS_HOST")
            .DbIndex(0)
        )
    )
    .UseDirectLayout()
);
```

## Layouts

The layout determines how the dictionary is stored in memory.

| Method | Layout | Use Case |
|--------|--------|----------|
| `UseHashedLayout()` | `HASHED` | General-purpose; integer keys |
| `UseFlatLayout()` | `FLAT` | Small dictionaries with dense integer keys |
| `UseDirectLayout()` | `DIRECT` | No caching; queries source on every lookup |
| `UseCacheLayout()` | `CACHE` | LRU cache; good for large datasets with hot keys |
| `UseComplexKeyHashedLayout()` | `COMPLEX_KEY_HASHED` | Composite (multi-column) keys |

## Lifetime

The lifetime controls how often ClickHouse refreshes the dictionary from its source.

```csharp
// Fixed range: ClickHouse picks a random interval between min and max
.HasLifetime(minSeconds: 60, maxSeconds: 300)

// Single value: equivalent to HasLifetime(0, seconds)
.HasLifetime(300)
```

## Defaults

Specify fallback values returned when a key is not found.

```csharp
.HasDefault(x => x.Name, "Unknown")
.HasDefault(x => x.IsoCode, "??")
```

## Query API

Query dictionaries using the `dict.Get`, `dict.GetOrDefault`, and `dict.ContainsKey` extension methods inside LINQ expressions.

```csharp
// dictGet
var name = context.Set<CountryLookup>()
    .Select(d => d.Get<string>(key, x => x.Name))
    .FirstOrDefault();
```

```sql
-- Generated SQL
SELECT dictGet('CountryLookup', 'Name', key)
```

```csharp
// dictGetOrDefault
var name = context.Set<CountryLookup>()
    .Select(d => d.GetOrDefault<string>(key, x => x.Name, "N/A"))
    .FirstOrDefault();
```

```sql
-- Generated SQL
SELECT dictGetOrDefault('CountryLookup', 'Name', key, 'N/A')
```

```csharp
// dictHas
var exists = context.Set<CountryLookup>()
    .Select(d => d.ContainsKey(key))
    .FirstOrDefault();
```

```sql
-- Generated SQL
SELECT dictHas('CountryLookup', key)
```

## Runtime Management

External dictionaries (PostgreSQL, MySQL, HTTP, Redis) are created at runtime rather than through migrations to avoid storing credentials in migration files.

```csharp
// Create all external dictionaries (idempotent)
await context.EnsureDictionariesAsync();

// Recreate external dictionaries (DROP + CREATE)
await context.RecreateDictionariesAsync();

// Reload a specific dictionary from its source
await context.ReloadDictionaryAsync<CountryLookup>();

// Get DDL for debugging
var ddl = context.GetDictionaryDdl<CountryLookup>();
Console.WriteLine(ddl);
```

> **Note:** ClickHouse-sourced dictionaries (using `FromTable`) are created via EF Core migrations. External-sourced dictionaries require calling `EnsureDictionariesAsync` at application startup.

## See Also

- [Materialized Views](materialized-views.md) -- another advanced DDL feature
- [External Entities](external-entities.md) -- querying external databases directly
- [Phase Ordering](../migrations/phase-ordering.md) -- dictionary creation ordering in migrations
