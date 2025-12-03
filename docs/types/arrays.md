# Array Types

Arrays in ClickHouse store multiple values of the same type in a single column. EF.CH maps .NET arrays and lists to ClickHouse `Array(T)`.

## Type Mappings

| .NET Type | ClickHouse Type |
|-----------|-----------------|
| `T[]` | `Array(T)` |
| `List<T>` | `Array(T)` |
| `IList<T>` | `Array(T)` |
| `ICollection<T>` | `Array(T)` |

## Entity Definition

```csharp
public class Product
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string[] Tags { get; set; } = [];           // Array(String)
    public int[] PriceTiers { get; set; } = [];        // Array(Int32)
    public List<string> Categories { get; set; } = []; // Array(String)
}
```

**Important:** Initialize arrays to empty to avoid null reference issues:

```csharp
public string[] Tags { get; set; } = [];           // Good
public string[] Tags { get; set; } = Array.Empty<string>(); // Also good
public string[]? Tags { get; set; }                // Nullable(Array(String))
```

## Configuration

Arrays work without special configuration:

```csharp
modelBuilder.Entity<Product>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => x.Id);
    // Array properties just work
});
```

## LINQ Operations

EF.CH translates common LINQ array operations to ClickHouse functions:

### Contains → has()

```csharp
// Find products with "electronics" tag
var products = await context.Products
    .Where(p => p.Tags.Contains("electronics"))
    .ToListAsync();
// SQL: ... WHERE has("Tags", 'electronics')
```

### Any() → notEmpty()

```csharp
// Find products with any tags
var products = await context.Products
    .Where(p => p.Tags.Any())
    .ToListAsync();
// SQL: ... WHERE notEmpty("Tags")
```

### Length / Count → length()

```csharp
// Find products with more than 3 tags
var products = await context.Products
    .Where(p => p.Tags.Length > 3)
    .ToListAsync();
// SQL: ... WHERE length("Tags") > 3

// Also works with List<T>
var products = await context.Products
    .Where(p => p.Categories.Count > 2)
    .ToListAsync();
```

### First() / Last() → arrayElement()

```csharp
// Get the first price tier
var firstPrices = await context.Products
    .Select(p => new { p.Name, FirstPrice = p.PriceTiers.First() })
    .ToListAsync();
// SQL: ... arrayElement("PriceTiers", 1)

// Get the last price tier
var lastPrices = await context.Products
    .Select(p => new { p.Name, LastPrice = p.PriceTiers.Last() })
    .ToListAsync();
// SQL: ... arrayElement("PriceTiers", -1)
```

### Index Access → arrayElement()

```csharp
// Get specific element (0-based in C#, converted to 1-based for ClickHouse)
var secondTags = await context.Products
    .Select(p => new { p.Name, SecondTag = p.Tags[1] })
    .ToListAsync();
// SQL: ... arrayElement("Tags", 2)
```

## Inserting Data

```csharp
context.Products.Add(new Product
{
    Id = Guid.NewGuid(),
    Name = "Laptop",
    Tags = ["electronics", "computers", "portable"],
    PriceTiers = [999, 1199, 1499],
    Categories = ["Technology", "Office"]
});
await context.SaveChangesAsync();
```

## Querying Examples

### Filter by Array Content

```csharp
// Products in multiple categories
var techProducts = await context.Products
    .Where(p => p.Tags.Contains("electronics") || p.Tags.Contains("gadgets"))
    .ToListAsync();
```

### Aggregate Array Data

```csharp
// Count products by tag count
var tagStats = await context.Products
    .GroupBy(p => p.Tags.Length)
    .Select(g => new { TagCount = g.Key, ProductCount = g.Count() })
    .ToListAsync();
```

### Combine with Other Filters

```csharp
// Premium electronics
var premium = await context.Products
    .Where(p => p.Tags.Contains("electronics"))
    .Where(p => p.PriceTiers.Any())
    .Where(p => p.PriceTiers.First() > 500)
    .ToListAsync();
```

## Generated DDL

```csharp
public class Product
{
    public Guid Id { get; set; }
    public string[] Tags { get; set; } = [];
    public int[] PriceTiers { get; set; } = [];
}
```

Generates:

```sql
CREATE TABLE "Products" (
    "Id" UUID NOT NULL,
    "Tags" Array(String) NOT NULL,
    "PriceTiers" Array(Int32) NOT NULL
)
ENGINE = MergeTree
ORDER BY ("Id")
```

## Nested Arrays

ClickHouse supports nested arrays:

```csharp
public class Matrix
{
    public Guid Id { get; set; }
    public int[][] Values { get; set; } = [];  // Array(Array(Int32))
}
```

**Note:** LINQ operations on nested arrays may be limited.

## Scaffolding

When reverse-engineering a ClickHouse database:

| ClickHouse Type | Generated .NET Type |
|-----------------|---------------------|
| `Array(String)` | `string[]` |
| `Array(Int32)` | `int[]` |
| `Array(UUID)` | `Guid[]` |
| `Array(Array(T))` | `T[][]` |

## Limitations

- **No AddRange in LINQ**: Can't use `array.AddRange()` in queries
- **Limited Nested Operations**: Complex nested array operations may not translate
- **No Array Modification**: Arrays are immutable in queries; modify in application code

## Best Practices

### Initialize Empty Arrays

```csharp
// Good: Non-null default
public string[] Tags { get; set; } = [];

// Avoid: Null default causes issues
public string[] Tags { get; set; } = null!;
```

### Use Contains for Filtering

```csharp
// Efficient: Uses has() function
.Where(p => p.Tags.Contains("value"))

// Less efficient: May not optimize well
.Where(p => p.Tags.Any(t => t == "value"))
```

### Consider Cardinality

Arrays work best for:
- Small to medium lists (< 1000 elements)
- Frequently queried together
- Semi-structured data

For very large arrays, consider a separate table with a foreign key pattern.

## See Also

- [Type Mappings Overview](overview.md)
- [Maps](maps.md) - For key-value data
- [Nested Types](nested.md) - For structured array data
