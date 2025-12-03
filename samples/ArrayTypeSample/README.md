# ArrayTypeSample

Demonstrates using Array columns in ClickHouse with EF.CH.

## What This Shows

- Defining entities with array properties (`string[]`, `int[]`, `List<T>`)
- LINQ operations on arrays: `Contains`, `Any`, `Length`, `First`, `Last`
- Filtering, grouping, and projecting array data

## LINQ to ClickHouse Translation

| LINQ | ClickHouse SQL |
|------|----------------|
| `array.Contains("x")` | `has("array", 'x')` |
| `array.Any()` | `notEmpty("array")` |
| `array.Length` | `length("array")` |
| `array.First()` | `arrayElement("array", 1)` |
| `array.Last()` | `arrayElement("array", -1)` |

## Prerequisites

- .NET 10.0+
- ClickHouse server running on localhost:8123

## Running

```bash
dotnet run
```

## Expected Output

```
Array Type Sample
=================

Creating database and tables...
Inserting products with tags and price tiers...

Inserted 5 products.

--- Products with 'electronics' tag (Contains) ---
  Gaming Laptop
  Wireless Mouse
  USB-C Hub

--- Gaming electronics (Contains with multiple tags) ---
  Gaming Laptop

--- Products with any tags (Any) ---
  5 products have tags

--- Products with 3+ price tiers (Length) ---
  Gaming Laptop: 3 price tiers
  Standing Desk: 3 price tiers

--- Lowest and highest price tiers (First/Last) ---
  Gaming Laptop: $1299 - $1799
  Wireless Mouse: $29 - $49
  USB-C Hub: $49 - $69
  Standing Desk: $399 - $699
  Monitor Arm: $79 - $99

--- Products grouped by tag count ---
  2 tags: 1 product(s)
  3 tags: 3 product(s)
  4 tags: 1 product(s)

--- Products with base price under $100 ---
  Wireless Mouse: $29
  USB-C Hub: $49
  Monitor Arm: $79

Done!
```

## Key Code

### Entity with Arrays

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

### Querying Arrays

```csharp
// Contains
var electronics = await context.Products
    .Where(p => p.Tags.Contains("electronics"))
    .ToListAsync();

// Length
var multiTier = await context.Products
    .Where(p => p.PriceTiers.Length >= 3)
    .ToListAsync();

// First/Last
var prices = await context.Products
    .Select(p => new {
        Low = p.PriceTiers.First(),
        High = p.PriceTiers.Last()
    })
    .ToListAsync();
```

## Learn More

- [Arrays Documentation](../../docs/types/arrays.md)
- [Type Mappings](../../docs/types/overview.md)
