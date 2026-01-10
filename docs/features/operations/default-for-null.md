# DefaultForNull

ClickHouse has performance overhead for `Nullable(T)` columns due to the bitmask tracking null values. `HasDefaultForNull()` lets you use a default value instead of NULL for better performance.

## When to Use

- High-volume tables where nullable overhead matters
- Columns where null vs default value distinction isn't important
- Performance-critical analytics scenarios

## Configuration

```csharp
modelBuilder.Entity<Order>(entity =>
{
    // Use 0 as default for nullable int
    entity.Property(e => e.DiscountPercent)
        .HasDefaultForNull(0);

    // Use empty string for nullable string
    entity.Property(e => e.Notes)
        .HasDefaultForNull("");

    // Use Guid.Empty for nullable Guid
    entity.Property(e => e.ExternalId)
        .HasDefaultForNull(Guid.Empty);
});
```

## How It Works

1. Column is generated as non-nullable with a DEFAULT value
2. When writing null, the default value is stored
3. When reading the default value, it's converted back to null

```csharp
public class Order
{
    public Guid Id { get; set; }
    public int? DiscountPercent { get; set; }  // Nullable in C#
}

entity.Property(e => e.DiscountPercent)
    .HasDefaultForNull(0);
```

Database column:
```sql
"DiscountPercent" Int32 DEFAULT 0  -- Non-nullable
```

## Complete Example

```csharp
public class Metric
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Name { get; set; } = string.Empty;
    public double Value { get; set; }
    public double? MinValue { get; set; }    // Optional min
    public double? MaxValue { get; set; }    // Optional max
    public string? Tags { get; set; }        // Optional tags
}

modelBuilder.Entity<Metric>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });

    // Use 0 as null placeholder for optional values
    entity.Property(e => e.MinValue)
        .HasDefaultForNull(0.0);

    entity.Property(e => e.MaxValue)
        .HasDefaultForNull(0.0);

    // Use empty string for optional tags
    entity.Property(e => e.Tags)
        .HasDefaultForNull("");
});
```

## Generated DDL

```sql
CREATE TABLE "Metrics" (
    "Id" UUID NOT NULL,
    "Timestamp" DateTime64(3) NOT NULL,
    "Name" String NOT NULL,
    "Value" Float64 NOT NULL,
    "MinValue" Float64 DEFAULT 0,
    "MaxValue" Float64 DEFAULT 0,
    "Tags" String DEFAULT ''
)
ENGINE = MergeTree
ORDER BY ("Timestamp", "Id")
```

## Querying

### Checking for Null

Use `== null` instead of `.HasValue`:

```csharp
// Good: Works correctly
var noDiscount = await context.Orders
    .Where(o => o.DiscountPercent == null)
    .ToListAsync();
// Translates to: WHERE DiscountPercent = 0

// Bad: Won't work - optimized away by EF Core
var noDiscount = await context.Orders
    .Where(o => !o.DiscountPercent.HasValue)  // Always false!
    .ToListAsync();
```

### Conditional Instead of Coalesce

Use conditional instead of `??`:

```csharp
// Good: Works correctly
var results = await context.Orders
    .Select(o => new {
        Discount = o.DiscountPercent == null ? 0 : o.DiscountPercent.Value
    })
    .ToListAsync();

// Bad: Won't work - database stores 0, not NULL
var results = await context.Orders
    .Select(o => new {
        Discount = o.DiscountPercent ?? 0  // Always gets stored value
    })
    .ToListAsync();
```

## Aggregate Behavior

Aggregates automatically exclude default values (treating them as null):

```csharp
// AVG ignores rows where MinValue = 0 (null placeholder)
var avgMin = await context.Metrics
    .AverageAsync(m => m.MinValue);

// SUM ignores rows where MinValue = 0
var totalMin = await context.Metrics
    .SumAsync(m => m.MinValue);
```

## Choosing Default Values

Pick defaults that are never valid business values:

```csharp
// Good: 0 is never a valid discount percentage
entity.Property(e => e.DiscountPercent)
    .HasDefaultForNull(0);

// Good: Empty string is distinguishable from real notes
entity.Property(e => e.Notes)
    .HasDefaultForNull("");

// Good: Guid.Empty is clearly a sentinel
entity.Property(e => e.ExternalId)
    .HasDefaultForNull(Guid.Empty);

// Risky: -1 might be a valid value in some contexts
entity.Property(e => e.Temperature)
    .HasDefaultForNull(-1.0);  // Be careful!
```

## Limitations

### Cannot Distinguish Default from Null

If you store the default value explicitly, it will read back as null:

```csharp
// Store 0 explicitly
context.Orders.Add(new Order { DiscountPercent = 0 });
await context.SaveChangesAsync();

// Read back - will be null!
var order = await context.Orders.FirstAsync();
Console.WriteLine(order.DiscountPercent);  // null, not 0
```

### Raw SQL Bypasses Conversion

```csharp
// LINQ query - conversion applies
var orders = await context.Orders.ToListAsync();
// order.DiscountPercent is null when database has 0

// Raw SQL - no conversion
var orders = await context.Database
    .SqlQueryRaw<OrderDto>("SELECT * FROM Orders")
    .ToListAsync();
// order.DiscountPercent is 0 (the stored value)
```

### Queries Match Default Values

```csharp
// This matches both null and explicitly-set-to-0 rows
var zeroDiscount = await context.Orders
    .Where(o => o.DiscountPercent == 0)
    .ToListAsync();
```

## Performance Benefit

Without DefaultForNull:
```sql
"DiscountPercent" Nullable(Int32)
-- Extra bitmask column for null tracking
-- ~1 bit per row overhead
-- Slightly slower queries due to null checks
```

With DefaultForNull:
```sql
"DiscountPercent" Int32 DEFAULT 0
-- No null tracking overhead
-- Simpler queries
-- Better compression
```

## When NOT to Use

- When distinguishing null from the default value matters
- When the default value could be a valid business value
- When null has semantic meaning different from "missing"

```csharp
// Don't use if 0 is a valid score
public int? Score { get; set; }  // 0 = failed, null = not graded

// Don't use if empty string has meaning
public string? OptionalField { get; set; }  // "" = cleared, null = never set
```

## See Also

- [Type Mappings](../types/overview.md)
- [ClickHouse Nullable Docs](https://clickhouse.com/docs/en/sql-reference/data-types/nullable)
