# String Splitting & Joining Functions

ClickHouse provides functions for splitting strings into arrays and joining arrays back into strings. EF.CH exposes these as `EF.Functions` extensions.

## Available Functions

### Splitting

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|---------------|-------------|-------------|
| `SplitByChar(sep, s)` | `splitByChar(sep, s)` | `string[]` | Split by a single character |
| `SplitByString(sep, s)` | `splitByString(sep, s)` | `string[]` | Split by a multi-character string |

### Joining

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|---------------|-------------|-------------|
| `ArrayStringConcat(arr)` | `arrayStringConcat(arr)` | `string` | Join array elements (no separator) |
| `ArrayStringConcat(arr, sep)` | `arrayStringConcat(arr, sep)` | `string` | Join array elements with separator |

## Usage Examples

### Split and Process

```csharp
using EF.CH.Extensions;

// Split CSV tags and work with the parts
var tagged = await context.Articles
    .Select(a => new
    {
        a.Title,
        Tags = EF.Functions.SplitByChar(",", a.TagsCsv)
    })
    .ToListAsync();
```

Generates:
```sql
SELECT "Title", splitByChar(',', "TagsCsv") AS "Tags"
FROM "Articles"
```

### Split URL Paths

```csharp
// Break URL paths into segments
var segments = await context.PageViews
    .Select(p => new
    {
        Path = EF.Functions.UrlPath(p.Url),
        Parts = EF.Functions.SplitByChar("/", EF.Functions.UrlPath(p.Url))
    })
    .ToListAsync();
```

### Join Array Columns

```csharp
// Display array columns as comma-separated strings
var display = await context.Products
    .Select(p => new
    {
        p.Name,
        Categories = EF.Functions.ArrayStringConcat(p.Tags, ", ")
    })
    .ToListAsync();
// Result: [{ Name: "Widget", Categories: "electronics, gadgets" }]
```

Generates:
```sql
SELECT "Name", arrayStringConcat("Tags", ', ') AS "Categories"
FROM "Products"
```

### Split by Multi-Character Delimiter

```csharp
// Split log lines by " :: " separator
var parts = await context.Logs
    .Select(l => new
    {
        l.Id,
        Fields = EF.Functions.SplitByString(" :: ", l.RawLine)
    })
    .ToListAsync();
```

## Notes

- `SplitByChar` expects a single character as the separator. For multi-character separators, use `SplitByString`.
- Empty strings between consecutive separators are preserved in the result array.
- `ArrayStringConcat` without a separator concatenates directly (equivalent to separator `""`).
- For the inverse of `SplitByNonAlpha` (from [Text Search](../../samples/TextSearchSample/)), use `ArrayStringConcat` with a space separator.

## Learn More

- [ClickHouse splitByChar](https://clickhouse.com/docs/en/sql-reference/functions/splitting-merging-functions)
- [ClickHouse arrayStringConcat](https://clickhouse.com/docs/en/sql-reference/functions/array-functions#arraystringconcat)
