# TextSearchSample

Demonstrates ClickHouse full-text search functions translated from LINQ via `EF.Functions`.

## What This Shows

- **Token search**: `HasToken`, `HasTokenCaseInsensitive`, `HasAnyToken`, `HasAllTokens` — exact token matching
- **Multi-search**: `MultiSearchAny`, `MultiSearchFirstIndex` — substring search across multiple needles
- **N-gram similarity**: `NgramSearch`, `NgramDistance` — fuzzy matching with similarity scores
- **Subsequence**: `HasSubsequence` — ordered character matching
- **Substring counting**: `CountSubstrings` — count occurrences of a substring
- **Regex multi-match**: `MultiMatchAny`, `MultiMatchAnyIndex` — Hyperscan-powered regex matching
- **Extract**: `ExtractAll`, `SplitByNonAlpha` — extract or tokenize text
- **Fluent helpers**: `ContainsToken()`, `ContainsAny()`, `FuzzyMatch()` — convenience wrappers
- **Skip indices**: `UseTokenBF()`, `UseNgramBF()` — accelerate text search with bloom filters

## Function Reference

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|---------------|-------------|-------------|
| `HasToken(text, token)` | `hasToken(text, token)` | `bool` | Exact token match |
| `HasTokenCaseInsensitive(text, token)` | `hasTokenCaseInsensitive(text, token)` | `bool` | Case-insensitive token match |
| `HasAnyToken(text, tokens)` | `hasAnyToken(text, tokens)` | `bool` | Any token matches |
| `HasAllTokens(text, tokens)` | `hasAllTokens(text, tokens)` | `bool` | All tokens match |
| `MultiSearchAny(text, needles)` | `multiSearchAny(text, needles)` | `bool` | Any substring matches |
| `MultiSearchFirstIndex(text, needles)` | `multiSearchFirstIndex(text, needles)` | `int` | Index of first matching needle |
| `NgramSearch(text, query)` | `ngramSearch(text, query)` | `float` | Similarity score (0..1) |
| `NgramDistance(text, query)` | `ngramDistance(text, query)` | `float` | Distance score (0..1) |
| `HasSubsequence(text, subseq)` | `hasSubsequence(text, subseq)` | `bool` | Ordered character match |
| `CountSubstrings(text, needle)` | `countSubstrings(text, needle)` | `int` | Count occurrences |
| `MultiMatchAny(text, patterns)` | `multiMatchAny(text, patterns)` | `bool` | Any regex matches |
| `ExtractAll(text, pattern)` | `extractAll(text, pattern)` | `string[]` | Extract regex matches |
| `SplitByNonAlpha(text)` | `splitByNonAlpha(text)` | `string[]` | Split by non-alpha chars |

All functions also have case-insensitive and/or UTF-8 variants.

## Prerequisites

- .NET 8.0+
- ClickHouse server running on localhost:8123

## Running

```bash
dotnet run
```

## Key Code

### Token Search

```csharp
// Exact token match (splits on non-alphanumeric boundaries)
var errors = await context.Logs
    .Where(l => EF.Functions.HasToken(l.Message, "error"))
    .ToListAsync();

// Check for any of several tokens
var alerts = await context.Logs
    .Where(l => EF.Functions.HasAnyToken(l.Message, new[] { "timeout", "refused", "error" }))
    .ToListAsync();
```

### Fuzzy Matching with N-grams

```csharp
// Find similar messages with similarity score
var results = await context.Logs
    .Select(l => new
    {
        l.Message,
        Score = EF.Functions.NgramSearch(l.Message, "authentication failed")
    })
    .Where(x => x.Score > 0.3f)
    .OrderByDescending(x => x.Score)
    .ToListAsync();
```

### Regex Multi-Match (Hyperscan)

```csharp
// Match multiple regex patterns at once
var matches = await context.Logs
    .Where(l => EF.Functions.MultiMatchAny(l.Message, new[] { @"\d+ms", @"\d+\.\d+\.\d+\.\d+" }))
    .ToListAsync();
```

### Fluent Helpers

```csharp
// ContainsToken — shorthand for HasToken in a Where clause
var results = context.Logs.ContainsToken(l => l.Message, "error");

// ContainsAny — shorthand for MultiSearchAny in a Where clause
var results = context.Logs.ContainsAny(l => l.Message, new[] { "error", "warning" });

// FuzzyMatch — ngramSearch with threshold filter + ORDER BY score DESC
var results = context.Logs.FuzzyMatch(l => l.Message, "payment error", threshold: 0.3f);
```

### Skip Indices for Performance

```csharp
modelBuilder.Entity<LogEntry>(entity =>
{
    // Token bloom filter — accelerates hasToken/hasTokenAny
    entity.HasIndex(e => e.Message).UseTokenBF(size: 10240, hashes: 3);

    // N-gram bloom filter — accelerates ngramSearch/multiSearchAny
    entity.HasIndex(e => e.Message).UseNgramBF(ngramSize: 3, size: 10240, hashes: 3);
});
```

## Learn More

- [ClickHouse String Search Functions](https://clickhouse.com/docs/en/sql-reference/functions/string-search-functions)
- [Skip Indices Documentation](../../docs/features/skip-indices.md)
