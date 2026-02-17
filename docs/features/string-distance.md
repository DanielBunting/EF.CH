# String Distance & Similarity Functions

ClickHouse provides functions for measuring how similar or different two strings are. These are useful for fuzzy matching, deduplication, and search relevance. EF.CH exposes them as `EF.Functions` extensions.

## Available Functions

### Edit Distance

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|---------------|-------------|-------------|
| `LevenshteinDistance(s1, s2)` | `levenshteinDistance(s1, s2)` | `ulong` | Minimum single-character edits |
| `LevenshteinDistanceUTF8(s1, s2)` | `levenshteinDistanceUTF8(s1, s2)` | `ulong` | UTF-8 aware edit distance |
| `DamerauLevenshteinDistance(s1, s2)` | `damerauLevenshteinDistance(s1, s2)` | `ulong` | Edits including transpositions |

### Similarity Scores

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|---------------|-------------|-------------|
| `JaroSimilarity(s1, s2)` | `jaroSimilarity(s1, s2)` | `double` | Jaro similarity (0 = different, 1 = identical) |
| `JaroWinklerSimilarity(s1, s2)` | `jaroWinklerSimilarity(s1, s2)` | `double` | Jaro-Winkler similarity (0..1, favors common prefixes) |

## Usage Examples

### Fuzzy Name Matching

```csharp
using EF.CH.Extensions;

// Find customers with names similar to a search term
var matches = await context.Customers
    .Where(c => EF.Functions.JaroWinklerSimilarity(c.Name, "johnson") > 0.85)
    .Select(c => new
    {
        c.Name,
        Similarity = EF.Functions.JaroWinklerSimilarity(c.Name, "johnson")
    })
    .OrderByDescending(x => x.Similarity)
    .ToListAsync();
```

Generates:
```sql
SELECT "Name", jaroWinklerSimilarity("Name", 'johnson') AS "Similarity"
FROM "Customers"
WHERE jaroWinklerSimilarity("Name", 'johnson') > 0.85
ORDER BY jaroWinklerSimilarity("Name", 'johnson') DESC
```

### Deduplication Candidates

```csharp
// Find products with very similar names (potential duplicates)
var dupes = await context.Products
    .SelectMany(
        p1 => context.Products.Where(p2 =>
            p2.Id != p1.Id &&
            EF.Functions.LevenshteinDistance(p1.Name, p2.Name) < 3),
        (p1, p2) => new { Name1 = p1.Name, Name2 = p2.Name })
    .ToListAsync();
```

### Typo Detection

```csharp
// Suggest corrections for misspelled search terms
var suggestions = await context.Products
    .Select(p => new
    {
        p.Name,
        Distance = EF.Functions.DamerauLevenshteinDistance(p.Name, searchTerm)
    })
    .Where(x => x.Distance <= 2)
    .OrderBy(x => x.Distance)
    .Take(5)
    .ToListAsync();
```

## Choosing a Function

| Function | Best For | Notes |
|----------|---------|-------|
| `LevenshteinDistance` | Exact edit count | Counts insertions, deletions, substitutions |
| `DamerauLevenshteinDistance` | Typo detection | Also counts transpositions ("ab" → "ba" = 1) |
| `JaroSimilarity` | Short strings | Good for names, codes |
| `JaroWinklerSimilarity` | Names with common prefixes | Boosts score when strings share a prefix |
| `LevenshteinDistanceUTF8` | Non-ASCII text | Counts Unicode characters, not bytes |

## Notes

- Distance functions return `ulong` (ClickHouse `UInt64`). Lower = more similar.
- Similarity functions return `double` (ClickHouse `Float64`). Higher = more similar.
- For large-scale fuzzy search, consider combining with [text search functions](../../samples/TextSearchSample/) (n-gram bloom filters) to pre-filter candidates before computing exact distances.

## Learn More

- [ClickHouse String Functions](https://clickhouse.com/docs/en/sql-reference/functions/string-functions)
