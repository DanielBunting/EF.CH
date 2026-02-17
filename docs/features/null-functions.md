# Null Handling Functions

ClickHouse has dedicated null-handling functions that differ from standard SQL `COALESCE` and `IS NULL`. EF.CH exposes these as `EF.Functions` extensions.

## Available Functions

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|---------------|-------------|-------------|
| `IfNull(value, default)` | `ifNull(x, default)` | `T` | Returns default if value is NULL |
| `NullIf(value, compare)` | `nullIf(x, y)` | `T?` | Returns NULL if value equals compare |
| `AssumeNotNull(value)` | `assumeNotNull(x)` | `T` | Strips Nullable wrapper (undefined if actually NULL) |
| `Coalesce(a, b)` | `coalesce(a, b)` | `T` | First non-NULL of two values |
| `Coalesce(a, b, c)` | `coalesce(a, b, c)` | `T` | First non-NULL of three values |
| `IsNull(value)` | `isNull(x)` | `bool` | True if value is NULL |
| `IsNotNull(value)` | `isNotNull(x)` | `bool` | True if value is not NULL |

## Usage Examples

### Default Values with IfNull

```csharp
using EF.CH.Extensions;

// Use a fallback when NickName is NULL
var users = await context.Users
    .Select(u => new
    {
        u.Id,
        DisplayName = EF.Functions.IfNull(u.NickName, u.UserName)
    })
    .ToListAsync();
```

Generates:
```sql
SELECT "Id", ifNull("NickName", "UserName") AS "DisplayName"
FROM "Users"
```

### Filtering with IsNull / IsNotNull

```csharp
// Find users without a nickname
var unnamed = await context.Users
    .Where(u => EF.Functions.IsNull(u.NickName))
    .ToListAsync();

// Find users with a nickname
var named = await context.Users
    .Where(u => EF.Functions.IsNotNull(u.NickName))
    .ToListAsync();
```

### Coalesce for Multi-Level Fallbacks

```csharp
// Try NickName, then DisplayName, then UserName
var labels = await context.Users
    .Select(u => new
    {
        u.Id,
        Label = EF.Functions.Coalesce(u.NickName, u.DisplayName, u.UserName)
    })
    .ToListAsync();
```

Generates:
```sql
SELECT "Id", coalesce("NickName", "DisplayName", "UserName") AS "Label"
FROM "Users"
```

### NullIf to Suppress Sentinel Values

```csharp
// Treat empty strings as NULL
var cleaned = await context.Products
    .Select(p => new
    {
        p.Id,
        Description = EF.Functions.NullIf(p.Description, "")
    })
    .ToListAsync();
```

### AssumeNotNull for Performance

```csharp
// When you know a Nullable column won't actually contain NULLs,
// AssumeNotNull avoids the overhead of Nullable type checking
var values = await context.Metrics
    .Select(m => new
    {
        m.Id,
        Value = EF.Functions.AssumeNotNull(m.OptionalValue)
    })
    .ToListAsync();
```

> **Warning:** If the value is actually NULL, `AssumeNotNull` has undefined behavior. Only use it when you're certain the column contains no NULLs.

## When to Use These vs. C# Null Operators

| Approach | Runs Where | Use When |
|----------|-----------|----------|
| `EF.Functions.IfNull(x, y)` | Server-side | Always — pushes logic to ClickHouse |
| `x ?? y` in LINQ | Depends on EF translation | May not translate for all expression types |
| `EF.Functions.IsNull(x)` | Server-side | Explicit null checks in complex expressions |
| `x == null` in LINQ | Server-side | Simple null comparisons |

The `EF.Functions` versions guarantee server-side execution and map directly to ClickHouse's optimized null functions.

## Learn More

- [ClickHouse Null Functions](https://clickhouse.com/docs/en/sql-reference/functions/functions-for-nulls)
