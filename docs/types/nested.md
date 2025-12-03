# Nested Types

Nested types in ClickHouse store arrays of structured data in a columnar format. EF.CH maps .NET record types in lists to ClickHouse `Nested(...)`.

## What is Nested?

`Nested` is ClickHouse's way of storing structured arrays. Unlike `Array(Tuple(...))`, Nested stores each field as a separate column array, enabling efficient columnar operations.

```sql
-- Logical view: array of structs
Items: [{ ProductId: 1, Qty: 2 }, { ProductId: 3, Qty: 1 }]

-- Physical storage: struct of arrays
Items.ProductId: [1, 3]
Items.Qty: [2, 1]
```

## Entity Definition

Use C# records for nested structures:

```csharp
// The nested structure (use record for value semantics)
public record LineItem
{
    public Guid ProductId { get; set; }
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
}

// The parent entity
public class Order
{
    public Guid Id { get; set; }
    public DateTime OrderDate { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public List<LineItem> Items { get; set; } = [];  // Nested(...)
}
```

## Configuration

Nested types work automatically when you use `List<TRecord>`:

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.OrderDate, x.Id });
    // Nested properties are configured automatically
});
```

## Generated DDL

```csharp
public record LineItem
{
    public Guid ProductId { get; set; }
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
}

public class Order
{
    public Guid Id { get; set; }
    public List<LineItem> Items { get; set; } = [];
}
```

Generates:

```sql
CREATE TABLE "Orders" (
    "Id" UUID NOT NULL,
    "Items" Nested(
        "ProductId" UUID,
        "Quantity" Int32,
        "UnitPrice" Decimal(18, 4)
    )
)
ENGINE = MergeTree
ORDER BY ("Id")
```

## Inserting Data

```csharp
context.Orders.Add(new Order
{
    Id = Guid.NewGuid(),
    OrderDate = DateTime.UtcNow,
    CustomerId = "CUST-001",
    Items =
    [
        new LineItem { ProductId = Guid.NewGuid(), Quantity = 2, UnitPrice = 29.99m },
        new LineItem { ProductId = Guid.NewGuid(), Quantity = 1, UnitPrice = 49.99m },
        new LineItem { ProductId = Guid.NewGuid(), Quantity = 3, UnitPrice = 9.99m }
    ]
});
await context.SaveChangesAsync();
```

## Querying Nested Data

### Access Nested Fields

```csharp
// Get orders with their item counts
var orders = await context.Orders
    .Select(o => new
    {
        o.Id,
        o.CustomerId,
        ItemCount = o.Items.Count
    })
    .ToListAsync();
```

### Filter by Nested Content

```csharp
// Orders containing a specific product
var ordersWithProduct = await context.Orders
    .Where(o => o.Items.Any(i => i.ProductId == targetProductId))
    .ToListAsync();
```

### Aggregate Nested Data

```csharp
// Total quantity per order
var orderTotals = await context.Orders
    .Select(o => new
    {
        o.Id,
        TotalQuantity = o.Items.Sum(i => i.Quantity),
        TotalValue = o.Items.Sum(i => i.Quantity * i.UnitPrice)
    })
    .ToListAsync();
```

## Real-World Examples

### E-Commerce Order

```csharp
public record OrderItem
{
    public Guid ProductId { get; set; }
    public string ProductName { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
    public decimal Discount { get; set; }
}

public class Order
{
    public Guid Id { get; set; }
    public DateTime OrderDate { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public List<OrderItem> Items { get; set; } = [];
    public decimal ShippingCost { get; set; }
}
```

### Event with Attributes

```csharp
public record EventAttribute
{
    public string Key { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
}

public class AnalyticsEvent
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventName { get; set; } = string.Empty;
    public List<EventAttribute> Attributes { get; set; } = [];
}
```

### Sports Statistics

```csharp
public record Goal
{
    public int Minute { get; set; }
    public string Scorer { get; set; } = string.Empty;
    public string Assist { get; set; } = string.Empty;
}

public class Match
{
    public Guid Id { get; set; }
    public DateTime MatchDate { get; set; }
    public string HomeTeam { get; set; } = string.Empty;
    public string AwayTeam { get; set; } = string.Empty;
    public List<Goal> Goals { get; set; } = [];
}
```

## Nested vs Array(Tuple) vs Map

| Type | Use Case | Structure |
|------|----------|-----------|
| `Nested(...)` | Structured arrays with named fields | Columnar storage |
| `Array(Tuple(...))` | Simple tuples | Row storage |
| `Map(K, V)` | Key-value pairs | Dictionary |
| `Array(T)` | Simple value lists | Single column |

**Choose Nested when:**
- You have structured data with multiple fields
- Field names are meaningful
- You need efficient columnar operations

**Choose Array(Tuple) when:**
- You have simple positional data
- Field names aren't important
- Structure is simple (2-3 fields)

## Scaffolding

When reverse-engineering, Nested types are scaffolded with TODO comments:

```csharp
// TODO: Configure Nested type for Items
public List<object> Items { get; set; } = [];
```

You'll need to manually create the record type and update the property.

## Limitations

- **Read-Heavy**: Nested is optimized for reads, not frequent updates
- **LINQ Limitations**: Complex nested queries may not fully translate
- **Record Required**: Use records for value equality semantics
- **No Modification**: Can't modify individual nested elements; replace entire list

## Best Practices

### Use Records

```csharp
// Good: Record with value semantics
public record LineItem { ... }

// Avoid: Class (reference semantics can cause issues)
public class LineItem { ... }
```

### Initialize Empty Lists

```csharp
public List<LineItem> Items { get; set; } = [];  // Good
public List<LineItem>? Items { get; set; }       // Avoid
```

### Keep Nested Data Small

Nested works well for:
- Order line items (typically < 100 items)
- Event attributes (typically < 50 attributes)
- Structured tags or labels

For large nested collections, consider a separate table.

### Design for Query Patterns

If you frequently filter by nested field values, ensure your queries can leverage ClickHouse's array functions effectively.

## See Also

- [Type Mappings Overview](overview.md)
- [Arrays](arrays.md) - For simple arrays
- [Maps](maps.md) - For key-value data
