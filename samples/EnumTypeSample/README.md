# EnumTypeSample

Demonstrates using C# enums with ClickHouse Enum8/Enum16 types.

## What This Shows

- Defining entities with enum properties
- Filtering by enum values (equality and comparison)
- Grouping and aggregating by enum values
- Automatic Enum8/Enum16 selection

## Enum Mapping

EF.CH automatically selects the ClickHouse enum type based on your values:

| C# Enum Values | ClickHouse Type |
|----------------|-----------------|
| -128 to 127 | `Enum8` |
| Outside Int8 range | `Enum16` |

## Prerequisites

- .NET 10.0+
- ClickHouse server running on localhost:8123

## Running

```bash
dotnet run
```

## Expected Output

```
Enum Type Sample
================

Creating database and tables...
Inserting orders with various statuses and priorities...

Inserted 6 orders.

--- Pending orders ---
  ORD-004: Critical priority, $999.99
  ORD-006: High priority, $199.99

--- Active orders (not delivered or cancelled) ---
  ORD-002: Shipped
  ORD-003: Processing
  ORD-004: Pending
  ORD-006: Pending

--- High priority or above ---
  ORD-002: High - Shipped
  ORD-004: Critical - Pending
  ORD-006: High - Pending

--- Order count by status ---
  Pending: 2 order(s)
  Processing: 1 order(s)
  Shipped: 1 order(s)
  Delivered: 1 order(s)
  Cancelled: 1 order(s)

--- Revenue by priority ---
  Critical: 1 order(s), $999.99
  High: 2 order(s), $499.98
  Normal: 2 order(s), $229.98
  Low: 0 order(s), $0.00

--- Critical pending orders (need immediate attention) ---
  ORD-004: Critical, $999.99
  ORD-006: High, $199.99

Done!
```

## Key Code

### Enum Definition

```csharp
public enum OrderStatus
{
    Pending = 0,
    Processing = 1,
    Shipped = 2,
    Delivered = 3,
    Cancelled = 4
}

public enum Priority
{
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3
}
```

### Entity with Enums

```csharp
public class Order
{
    public Guid Id { get; set; }
    public string OrderNumber { get; set; } = string.Empty;
    public OrderStatus Status { get; set; }  // Enum8
    public Priority Priority { get; set; }   // Enum8
}
```

### Querying Enums

```csharp
// Filter by value
var pending = await context.Orders
    .Where(o => o.Status == OrderStatus.Pending)
    .ToListAsync();

// Comparison (enums are comparable)
var urgent = await context.Orders
    .Where(o => o.Priority >= Priority.High)
    .ToListAsync();

// Group by enum
var statusCounts = await context.Orders
    .GroupBy(o => o.Status)
    .Select(g => new { Status = g.Key, Count = g.Count() })
    .ToListAsync();
```

## Generated DDL

```sql
CREATE TABLE "Orders" (
    "Id" UUID NOT NULL,
    "OrderNumber" String NOT NULL,
    "Status" Enum8('Pending' = 0, 'Processing' = 1, 'Shipped' = 2, 'Delivered' = 3, 'Cancelled' = 4),
    "Priority" Enum8('Low' = 0, 'Normal' = 1, 'High' = 2, 'Critical' = 3),
    "Total" Decimal(18, 4) NOT NULL
)
ENGINE = MergeTree
ORDER BY ("OrderDate", "Id")
```

## Best Practices

1. **Use explicit values**: `Pending = 0` instead of just `Pending`
2. **Start from zero**: Makes default value predictable
3. **Leave room for expansion**: Skip values for future additions

## Learn More

- [Enums Documentation](../../docs/types/enums.md)
- [Scaffolding](../../docs/scaffolding.md) - How enums are generated from existing tables
