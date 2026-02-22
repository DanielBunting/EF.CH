# Dictionary Sample

Demonstrates ClickHouse dictionary features using EF.CH.

## What This Sample Shows

1. **Table-backed dictionaries** - Create a dictionary from a ClickHouse source table using `AsDictionary<TDictionary, TSource>()` with `FromTable`, `HasKey`, `UseHashedLayout`, and `HasLifetime`
2. **Dictionary query API** - Use `Get<T>()`, `GetOrDefault<T>()`, and `ContainsKey()` for dictionary lookups
3. **External PostgreSQL source** - Configuration pattern for sourcing dictionaries from PostgreSQL via `FromPostgreSql`
4. **Layout comparison** - When to use `UseHashedLayout`, `UseFlatLayout`, `UseDirectLayout`, `UseCacheLayout`, and `UseComplexKeyHashedLayout`

## Prerequisites

- .NET 8.0 SDK
- Docker (for Testcontainers)

## Running

```bash
dotnet run --project samples/DictionarySample/
```

## Key Concepts

### Table-Backed Dictionaries

Table-backed dictionaries are sourced from existing ClickHouse tables. They provide fast key-value lookups using in-memory data structures. The source data is periodically refreshed based on the `HasLifetime` configuration.

```csharp
entity.AsDictionary<CountryLookup, Country>(cfg => cfg
    .HasKey(x => x.Id)
    .FromTable(
        projection: c => new CountryLookup { Id = c.Id, Name = c.Name },
        filter: q => q.Where(c => c.IsActive))
    .UseHashedLayout()
    .HasLifetime(minSeconds: 60, maxSeconds: 300));
```

### Dictionary Query API

The `ClickHouseDictionary<TDictionary, TKey>` class provides both LINQ-translatable methods (for use in queries) and async methods (for direct access):

- `Get<T>(key, x => x.Attr)` / `GetAsync<T>(...)` - translates to `dictGet()`
- `GetOrDefault<T>(key, x => x.Attr, default)` / `GetOrDefaultAsync<T>(...)` - translates to `dictGetOrDefault()`
- `ContainsKey(key)` / `ContainsKeyAsync(...)` - translates to `dictHas()`

### External Dictionaries

External dictionaries source data from PostgreSQL, MySQL, HTTP, or Redis. They are created at runtime (not during migrations) to avoid storing credentials:

```csharp
// At startup
await context.EnsureDictionariesAsync();
```
