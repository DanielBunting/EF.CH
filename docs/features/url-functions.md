# URL Parsing Functions

ClickHouse has built-in URL parsing functions optimized for web analytics. EF.CH exposes these as `EF.Functions` extensions, letting you decompose and manipulate URLs directly in LINQ queries.

## Available Functions

### Extraction

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `Domain(url)` | `domain(url)` | Extract domain (e.g. `www.example.com`) |
| `DomainWithoutWWW(url)` | `domainWithoutWWW(url)` | Domain without `www.` prefix |
| `TopLevelDomain(url)` | `topLevelDomain(url)` | TLD (e.g. `com`, `co.uk`) |
| `Protocol(url)` | `protocol(url)` | Protocol (e.g. `https`) |
| `UrlPath(url)` | `path(url)` | Path component (e.g. `/products/123`) |

### Query Parameters

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `ExtractURLParameter(url, name)` | `extractURLParameter(url, name)` | Get a specific query parameter value |
| `ExtractURLParameters(url)` | `extractURLParameters(url)` | Get all parameters as `string[]` |
| `CutURLParameter(url, name)` | `cutURLParameter(url, name)` | Return URL with parameter removed |

### Encoding

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `DecodeURLComponent(s)` | `decodeURLComponent(s)` | URL-decode a string |
| `EncodeURLComponent(s)` | `encodeURLComponent(s)` | URL-encode a string |

All functions return `string` except `ExtractURLParameters` which returns `string[]`.

## Usage Examples

### Web Analytics: Traffic by Domain

```csharp
using EF.CH.Extensions;

var traffic = await context.PageViews
    .GroupBy(p => EF.Functions.DomainWithoutWWW(p.Url))
    .Select(g => new
    {
        Domain = g.Key,
        Views = g.Count()
    })
    .OrderByDescending(x => x.Views)
    .Take(10)
    .ToListAsync();
```

Generates:
```sql
SELECT domainWithoutWWW("Url") AS "Domain", count() AS "Views"
FROM "PageViews"
GROUP BY domainWithoutWWW("Url")
ORDER BY count() DESC
LIMIT 10
```

### UTM Campaign Tracking

```csharp
// Extract UTM parameters from landing page URLs
var campaigns = await context.PageViews
    .Select(p => new
    {
        p.Url,
        Source = EF.Functions.ExtractURLParameter(p.Url, "utm_source"),
        Medium = EF.Functions.ExtractURLParameter(p.Url, "utm_medium"),
        Campaign = EF.Functions.ExtractURLParameter(p.Url, "utm_campaign")
    })
    .Where(x => x.Source != "")
    .ToListAsync();
```

### Content Analysis by Path

```csharp
// Count views per URL path (ignoring query strings)
var pathCounts = await context.PageViews
    .GroupBy(p => EF.Functions.UrlPath(p.Url))
    .Select(g => new { Path = g.Key, Count = g.Count() })
    .OrderByDescending(x => x.Count)
    .ToListAsync();
```

### Clean URLs for Display

```csharp
// Remove tracking parameters before displaying
var cleanUrls = await context.PageViews
    .Select(p => new
    {
        Original = p.Url,
        Clean = EF.Functions.CutURLParameter(
            EF.Functions.CutURLParameter(p.Url, "utm_source"),
            "utm_medium")
    })
    .ToListAsync();
```

### Protocol Distribution

```csharp
var protocols = await context.PageViews
    .GroupBy(p => EF.Functions.Protocol(p.Url))
    .Select(g => new { Protocol = g.Key, Count = g.Count() })
    .ToListAsync();
// Result: [{ Protocol: "https", Count: 9500 }, { Protocol: "http", Count: 500 }]
```

## Notes

- These functions are string-based — they parse the URL on every call. For frequently-queried URL components, consider using [computed columns](computed-columns.md) to materialize the extracted value.
- `UrlPath` is named differently from ClickHouse's `path()` to avoid collision with `System.IO.Path`.
- Empty strings are returned for missing components (not NULL).
- Functions handle malformed URLs gracefully — they return empty strings for unparseable parts.

## Learn More

- [ClickHouse URL Functions](https://clickhouse.com/docs/en/sql-reference/functions/url-functions)
