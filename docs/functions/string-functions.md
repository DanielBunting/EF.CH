# String Functions

EF.CH translates standard .NET string operations and LINQ string methods to their ClickHouse SQL equivalents. This includes pattern matching, case conversion, trimming, substring operations, and string splitting/joining functions.

---

## String Method Translations

Standard .NET `string` methods are automatically translated when used in LINQ queries.

| C# Method | ClickHouse SQL | Notes |
|-----------|----------------|-------|
| `s.Contains("pattern")` | `s LIKE '%pattern%'` | Substring search via LIKE |
| `s.StartsWith("pattern")` | `s LIKE 'pattern%'` | Prefix match |
| `s.EndsWith("pattern")` | `s LIKE '%pattern'` | Suffix match |
| `s.ToUpper()` | `upperUTF8(s)` | UTF-8 aware upper case |
| `s.ToLower()` | `lowerUTF8(s)` | UTF-8 aware lower case |
| `s.Trim()` | `trim(s)` | Remove leading/trailing whitespace |
| `s.TrimStart()` | `trimLeft(s)` | Remove leading whitespace |
| `s.TrimEnd()` | `trimRight(s)` | Remove trailing whitespace |
| `s.Substring(i)` | `substring(s, i+1)` | Substring from index (auto-adjusted to 1-based) |
| `s.Substring(i, len)` | `substring(s, i+1, len)` | Substring with length (auto-adjusted to 1-based) |
| `s.Replace("old", "new")` | `replaceAll(s, 'old', 'new')` | Replace all occurrences |
| `s.IndexOf("sub")` | `positionUTF8(s, 'sub') - 1` | Find position (auto-adjusted to 0-based) |
| `string.IsNullOrEmpty(s)` | `s IS NULL OR empty(s)` | Null or empty check |
| `string.IsNullOrWhiteSpace(s)` | `s IS NULL OR empty(trim(s))` | Null or whitespace check |
| `string.Concat(a, b)` | `concat(a, b)` | String concatenation |
| `s.Length` | `char_length(s)` | Character count |
| `EF.Functions.Like(s, pattern)` | `s LIKE pattern` | Explicit LIKE pattern |

### Pattern Matching

```csharp
var results = await context.Products
    .Where(p => p.Name.Contains("widget"))         // Name LIKE '%widget%'
    .Where(p => p.Sku.StartsWith("SKU-"))          // Sku LIKE 'SKU-%'
    .Where(p => p.Category.EndsWith("-sale"))       // Category LIKE '%-sale'
    .ToListAsync();
```

### Case Conversion and Trimming

```csharp
var results = await context.Users
    .Select(u => new
    {
        UpperName = u.Name.ToUpper(),              // upperUTF8(Name)
        LowerEmail = u.Email.ToLower(),            // lowerUTF8(Email)
        CleanInput = u.RawInput.Trim()             // trim(RawInput)
    })
    .ToListAsync();
```

### Substring and Replace

```csharp
var results = await context.Logs
    .Select(l => new
    {
        Prefix = l.Message.Substring(0, 10),       // substring(Message, 1, 10)
        Suffix = l.Message.Substring(5),           // substring(Message, 6)
        Sanitized = l.Message.Replace("\n", " "),  // replaceAll(Message, '\n', ' ')
        Position = l.Message.IndexOf("ERROR")      // positionUTF8(Message, 'ERROR') - 1
    })
    .ToListAsync();
```

### String Length and Null Checks

```csharp
var results = await context.Products
    .Where(p => !string.IsNullOrEmpty(p.Description))  // NOT (Description IS NULL OR empty(Description))
    .Where(p => p.Name.Length > 3)                      // char_length(Name) > 3
    .ToListAsync();
```

### LIKE Pattern

```csharp
var results = await context.Users
    .Where(u => EF.Functions.Like(u.Email, "%@example.com"))  // Email LIKE '%@example.com'
    .ToListAsync();
```

---

## Index Adjustment

ClickHouse strings are 1-based while .NET strings are 0-based. EF.CH automatically handles this conversion:

- `Substring(i)` translates to `substring(s, i+1)` -- the index is incremented by 1
- `Substring(i, len)` translates to `substring(s, i+1, len)` -- the index is incremented by 1
- `IndexOf("sub")` translates to `positionUTF8(s, 'sub') - 1` -- the result is decremented by 1

This means C# code using 0-based indexing works correctly without manual adjustment.

---

## String Split Functions

String splitting and joining functions are available via `EF.Functions` on the `ClickHouseStringSplitDbFunctionsExtensions` class.

| C# Method | ClickHouse SQL | Notes |
|-----------|----------------|-------|
| `EF.Functions.SplitByChar(",", s)` | `splitByChar(',', s)` | Split by single character separator |
| `EF.Functions.SplitByString("::", s)` | `splitByString('::', s)` | Split by multi-character separator |
| `EF.Functions.ArrayStringConcat(arr)` | `arrayStringConcat(arr)` | Join array elements with no separator |
| `EF.Functions.ArrayStringConcat(arr, ",")` | `arrayStringConcat(arr, ',')` | Join array elements with separator |

### Splitting Strings

```csharp
var results = await context.Logs
    .Select(l => new
    {
        // Split CSV tags into an array
        Tags = EF.Functions.SplitByChar(",", l.TagsCsv),        // splitByChar(',', TagsCsv)

        // Split namespace path
        Parts = EF.Functions.SplitByString("::", l.Namespace)   // splitByString('::', Namespace)
    })
    .ToListAsync();
```

### Joining Arrays

```csharp
var results = await context.Products
    .Select(p => new
    {
        // Join tags array into a comma-separated string
        TagList = EF.Functions.ArrayStringConcat(p.Tags, ", "),  // arrayStringConcat(Tags, ', ')

        // Join with no separator
        Slug = EF.Functions.ArrayStringConcat(p.NameParts)       // arrayStringConcat(NameParts)
    })
    .ToListAsync();
```

### Split and Aggregate

```csharp
// Count distinct tags across all products
var tagCounts = await context.Products
    .Select(p => EF.Functions.SplitByChar(",", p.TagsCsv))
    .SelectMany(tags => tags)
    .GroupBy(tag => tag)
    .Select(g => new { Tag = g.Key, Count = g.Count() })
    .ToListAsync();
```

---

## See Also

- [Utility Functions](utility-functions.md) -- string distance, URL parsing, and encoding functions
- [Aggregate Functions](aggregate-functions.md) -- aggregate functions that can be combined with string operations
