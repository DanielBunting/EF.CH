# Text Search

EF.CH translates 29 text search functions to their ClickHouse equivalents and provides convenience query extensions for common search patterns. Functions are available both as `EF.Functions.*` stubs (for use in LINQ Where clauses) and as `IQueryable<T>` extensions.

## DbFunctions API

All 29 functions are accessible via `EF.Functions.*` inside LINQ expressions. They throw at runtime if called directly -- they exist only for LINQ-to-SQL translation.

```csharp
using Microsoft.EntityFrameworkCore;
```

### Token Search

Token-based search splits strings on non-alphanumeric ASCII boundaries. Useful for log messages, URLs, and structured text.

| Method | ClickHouse Function | Returns |
|--------|---------------------|---------|
| `EF.Functions.HasToken(haystack, token)` | `hasToken()` | `bool` |
| `EF.Functions.HasTokenCaseInsensitive(haystack, token)` | `hasTokenCaseInsensitive()` | `bool` |
| `EF.Functions.HasAnyToken(haystack, tokens)` | `hasAnyToken()` | `bool` |
| `EF.Functions.HasAllTokens(haystack, tokens)` | `hasAllTokens()` | `bool` |

```csharp
var errors = await context.Logs
    .Where(l => EF.Functions.HasToken(l.Message, "ERROR"))
    .ToListAsync();
```

```sql
SELECT l."Id", l."Message"
FROM "Logs" AS l
WHERE hasToken(l."Message", 'ERROR')
```

### Multi-Search

Searches for multiple substrings simultaneously. Efficient for searching many terms at once.

| Method | ClickHouse Function | Returns |
|--------|---------------------|---------|
| `EF.Functions.MultiSearchAny(haystack, needles)` | `multiSearchAny()` | `bool` |
| `EF.Functions.MultiSearchAnyCaseInsensitive(haystack, needles)` | `multiSearchAnyCaseInsensitive()` | `bool` |
| `EF.Functions.MultiSearchAll(haystack, needles)` | `multiSearchAllPositions()` | `bool` |
| `EF.Functions.MultiSearchAllCaseInsensitive(haystack, needles)` | `multiSearchAllPositionsCaseInsensitive()` | `bool` |
| `EF.Functions.MultiSearchFirstPosition(haystack, needles)` | `multiSearchFirstPosition()` | `ulong` |
| `EF.Functions.MultiSearchFirstPositionCaseInsensitive(haystack, needles)` | `multiSearchFirstPositionCaseInsensitive()` | `ulong` |
| `EF.Functions.MultiSearchFirstIndex(haystack, needles)` | `multiSearchFirstIndex()` | `ulong` |
| `EF.Functions.MultiSearchFirstIndexCaseInsensitive(haystack, needles)` | `multiSearchFirstIndexCaseInsensitive()` | `ulong` |

```csharp
var terms = new[] { "error", "warning", "critical" };
var alerts = await context.Logs
    .Where(l => EF.Functions.MultiSearchAny(l.Message, terms))
    .ToListAsync();
```

```sql
SELECT l."Id", l."Message"
FROM "Logs" AS l
WHERE multiSearchAny(l."Message", ['error', 'warning', 'critical'])
```

### N-gram Search and Distance

N-gram functions compute similarity and distance scores using 3-gram comparison. Scores range from 0 (no match) to 1 (exact match for search, maximum distance for distance).

| Method | ClickHouse Function | Returns |
|--------|---------------------|---------|
| `EF.Functions.NgramSearch(haystack, needle)` | `ngramSearch()` | `float` |
| `EF.Functions.NgramSearchCaseInsensitive(haystack, needle)` | `ngramSearchCaseInsensitive()` | `float` |
| `EF.Functions.NgramDistance(haystack, needle)` | `ngramDistance()` | `float` |
| `EF.Functions.NgramDistanceCaseInsensitive(haystack, needle)` | `ngramDistanceCaseInsensitive()` | `float` |
| `EF.Functions.NgramSearchUTF8(haystack, needle)` | `ngramSearchUTF8()` | `float` |
| `EF.Functions.NgramSearchCaseInsensitiveUTF8(haystack, needle)` | `ngramSearchCaseInsensitiveUTF8()` | `float` |
| `EF.Functions.NgramDistanceUTF8(haystack, needle)` | `ngramDistanceUTF8()` | `float` |
| `EF.Functions.NgramDistanceCaseInsensitiveUTF8(haystack, needle)` | `ngramDistanceCaseInsensitiveUTF8()` | `float` |

Use the UTF8 variants when working with non-ASCII text to ensure correct character boundary handling.

```csharp
var similar = await context.Products
    .Where(p => EF.Functions.NgramSearch(p.Name, "laptop") > 0.3f)
    .OrderByDescending(p => EF.Functions.NgramSearch(p.Name, "laptop"))
    .ToListAsync();
```

```sql
SELECT p."Id", p."Name"
FROM "Products" AS p
WHERE ngramSearch(p."Name", 'laptop') > 0.3
ORDER BY ngramSearch(p."Name", 'laptop') DESC
```

### Subsequence

Checks whether the characters of the subsequence appear in order within the haystack (not necessarily contiguous).

| Method | ClickHouse Function | Returns |
|--------|---------------------|---------|
| `EF.Functions.HasSubsequence(haystack, subsequence)` | `hasSubsequence()` | `bool` |
| `EF.Functions.HasSubsequenceCaseInsensitive(haystack, subsequence)` | `hasSubsequenceCaseInsensitive()` | `bool` |

### Substring Counting

| Method | ClickHouse Function | Returns |
|--------|---------------------|---------|
| `EF.Functions.CountSubstrings(haystack, needle)` | `countSubstrings()` | `ulong` |
| `EF.Functions.CountSubstringsCaseInsensitive(haystack, needle)` | `countSubstringsCaseInsensitive()` | `ulong` |

### Regex (Hyperscan)

Multi-pattern regex matching powered by the Hyperscan library. Efficient for matching many patterns simultaneously.

| Method | ClickHouse Function | Returns |
|--------|---------------------|---------|
| `EF.Functions.MultiMatchAny(haystack, patterns)` | `multiMatchAny()` | `bool` |
| `EF.Functions.MultiMatchAnyIndex(haystack, patterns)` | `multiMatchAnyIndex()` | `ulong` |
| `EF.Functions.MultiMatchAllIndices(haystack, patterns)` | `multiMatchAllIndices()` | `ulong[]` |

```csharp
var patterns = new[] { @"\berror\b", @"\bfailed\b", @"timeout \d+ms" };
var matches = await context.Logs
    .Where(l => EF.Functions.MultiMatchAny(l.Message, patterns))
    .ToListAsync();
```

```sql
SELECT l."Id", l."Message"
FROM "Logs" AS l
WHERE multiMatchAny(l."Message", ['\\berror\\b', '\\bfailed\\b', 'timeout \\d+ms'])
```

### Extract and Split

| Method | ClickHouse Function | Returns |
|--------|---------------------|---------|
| `EF.Functions.ExtractAll(haystack, pattern)` | `extractAll()` | `string[]` |
| `EF.Functions.SplitByNonAlpha(s)` | `splitByNonAlpha()` | `string[]` |

## Query Extensions

Convenience extensions on `IQueryable<T>` that build the correct LINQ expression trees internally. These live in the `EF.CH.Extensions` namespace.

```csharp
using EF.CH.Extensions;
```

### ContainsToken

Filters to rows where the selected text column contains a specific token.

```csharp
// Case-sensitive
var results = await context.Logs
    .ContainsToken(l => l.Body, "error")
    .ToListAsync();

// Case-insensitive
var results = await context.Logs
    .ContainsToken(l => l.Body, "error", caseInsensitive: true)
    .ToListAsync();
```

Generated SQL:

```sql
-- Case-sensitive
WHERE hasToken("Body", 'error')

-- Case-insensitive
WHERE hasTokenCaseInsensitive("Body", 'error')
```

### ContainsAny

Filters to rows where the selected text column contains any of the given terms as substrings.

```csharp
var terms = new[] { "error", "warning", "critical" };

var results = await context.Logs
    .ContainsAny(l => l.Body, terms)
    .ToListAsync();

// Case-insensitive
var results = await context.Logs
    .ContainsAny(l => l.Body, terms, caseInsensitive: true)
    .ToListAsync();
```

Generated SQL:

```sql
-- Case-sensitive
WHERE multiSearchAny("Body", ['error', 'warning', 'critical'])

-- Case-insensitive
WHERE multiSearchAnyCaseInsensitive("Body", ['error', 'warning', 'critical'])
```

### FuzzyMatch

Filters to rows where the n-gram similarity exceeds a threshold, then orders results by descending similarity score. Returns an `IOrderedQueryable<T>`.

```csharp
var results = await context.Products
    .FuzzyMatch(p => p.Name, "laptap", threshold: 0.3f)
    .Take(20)
    .ToListAsync();

// Case-insensitive
var results = await context.Products
    .FuzzyMatch(p => p.Name, "laptap", threshold: 0.3f, caseInsensitive: true)
    .Take(20)
    .ToListAsync();
```

Generated SQL:

```sql
-- Case-sensitive
WHERE ngramSearch("Name", 'laptap') > 0.3
ORDER BY ngramSearch("Name", 'laptap') DESC
LIMIT 20

-- Case-insensitive
WHERE ngramSearchCaseInsensitive("Name", 'laptap') > 0.3
ORDER BY ngramSearchCaseInsensitive("Name", 'laptap') DESC
LIMIT 20
```

The default threshold is `0.3f`. Lower values return more results (more permissive matching); higher values require closer matches.

## Choosing the Right Function

| Use Case | Recommended Function | Index Support |
|----------|---------------------|---------------|
| Exact word in log line | `HasToken` / `ContainsToken` | TokenBF skip index |
| Any of several keywords | `MultiSearchAny` / `ContainsAny` | TokenBF skip index |
| Fuzzy name matching | `NgramSearch` / `FuzzyMatch` | NgramBF skip index |
| Typo-tolerant search | `NgramSearch` with low threshold | NgramBF skip index |
| Multi-pattern regex | `MultiMatchAny` | None (Hyperscan in-memory) |
| Character subsequence | `HasSubsequence` | None |

For best performance, pair text search functions with the corresponding skip index type on the searched column.

## See Also

- [Query Modifiers](query-modifiers.md) -- PREWHERE for filtering optimization
- [Raw SQL](raw-sql.md) -- Escape hatches for unsupported search patterns
