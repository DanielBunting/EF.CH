# Enum Types

ClickHouse has native enum support with `Enum8` and `Enum16`. EF.CH automatically maps C# enums to the appropriate ClickHouse enum type.

## Type Mappings

| .NET Type | ClickHouse Type | Range |
|-----------|-----------------|-------|
| `enum` (values -128 to 127) | `Enum8` | 256 values |
| `enum` (values outside Int8) | `Enum16` | 65,536 values |

EF.CH automatically selects `Enum8` or `Enum16` based on your enum values.

## Entity Definition

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

public class Order
{
    public Guid Id { get; set; }
    public DateTime OrderDate { get; set; }
    public OrderStatus Status { get; set; }
    public Priority Priority { get; set; }
}
```

## Configuration

Enums work without special configuration:

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.OrderDate, x.Id });
    // Enum properties just work
});
```

## Generated DDL

```csharp
public enum OrderStatus
{
    Pending = 0,
    Processing = 1,
    Shipped = 2,
    Delivered = 3,
    Cancelled = 4
}
```

Generates:

```sql
CREATE TABLE "Orders" (
    "Id" UUID NOT NULL,
    "OrderDate" DateTime64(3) NOT NULL,
    "Status" Enum8('Pending' = 0, 'Processing' = 1, 'Shipped' = 2, 'Delivered' = 3, 'Cancelled' = 4) NOT NULL,
    "Priority" Enum8('Low' = 0, 'Normal' = 1, 'High' = 2, 'Critical' = 3) NOT NULL
)
ENGINE = MergeTree
ORDER BY ("OrderDate", "Id")
```

## LINQ Operations

### Filter by Enum Value

```csharp
// Orders that are pending
var pending = await context.Orders
    .Where(o => o.Status == OrderStatus.Pending)
    .ToListAsync();

// Orders with high or critical priority
var urgent = await context.Orders
    .Where(o => o.Priority >= Priority.High)
    .ToListAsync();
```

### Group by Enum

```csharp
// Count orders by status
var statusCounts = await context.Orders
    .GroupBy(o => o.Status)
    .Select(g => new { Status = g.Key, Count = g.Count() })
    .ToListAsync();
```

### Multiple Enum Conditions

```csharp
// High priority orders that aren't delivered
var activeUrgent = await context.Orders
    .Where(o => o.Priority >= Priority.High)
    .Where(o => o.Status != OrderStatus.Delivered && o.Status != OrderStatus.Cancelled)
    .ToListAsync();
```

## Inserting Data

```csharp
context.Orders.Add(new Order
{
    Id = Guid.NewGuid(),
    OrderDate = DateTime.UtcNow,
    Status = OrderStatus.Pending,
    Priority = Priority.Normal
});
await context.SaveChangesAsync();
```

## Enum8 vs Enum16

### Enum8 (Default)

Used when all values fit in `sbyte` (-128 to 127):

```csharp
public enum SmallEnum
{
    Value1 = 0,
    Value2 = 1,
    // ... up to 256 values
}
```

### Enum16

Used when values exceed `sbyte` range:

```csharp
public enum LargeEnum
{
    Value1 = 0,
    Value2 = 1000,   // > 127, forces Enum16
    Value3 = 2000
}
```

Or for enums with many values:

```csharp
public enum ManyValues
{
    V1 = 0, V2 = 1, V3 = 2, /* ... 300+ values ... */
}
```

## Scaffolding

When reverse-engineering a ClickHouse database, EF.CH generates C# enum files:

**ClickHouse:**
```sql
"Status" Enum8('Pending' = 0, 'Processing' = 1, 'Shipped' = 2)
```

**Generated C#:**
```csharp
// Generated file: OrderStatus.cs
public enum OrderStatus
{
    Pending = 0,
    Processing = 1,
    Shipped = 2
}
```

### Name Sanitization

ClickHouse enum values can contain characters not valid in C# identifiers. EF.CH sanitizes these:

| ClickHouse Value | Generated C# |
|------------------|--------------|
| `'In Progress'` | `InProgress` |
| `'New-Order'` | `NewOrder` |
| `'123Start'` | `_123Start` |

## Nullable Enums

```csharp
public class Order
{
    public OrderStatus? Status { get; set; }  // Nullable(Enum8(...))
}

// Query nullable enums
var noStatus = await context.Orders
    .Where(o => o.Status == null)
    .ToListAsync();
```

## Real-World Examples

### HTTP Status Categories

```csharp
public enum HttpStatusCategory
{
    Informational = 1,
    Success = 2,
    Redirection = 3,
    ClientError = 4,
    ServerError = 5
}

public class ApiLog
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Endpoint { get; set; } = string.Empty;
    public int StatusCode { get; set; }
    public HttpStatusCategory StatusCategory { get; set; }
}
```

### Log Levels

```csharp
public enum LogLevel
{
    Trace = 0,
    Debug = 1,
    Information = 2,
    Warning = 3,
    Error = 4,
    Critical = 5
}

public class LogEntry
{
    public DateTime Timestamp { get; set; }
    public LogLevel Level { get; set; }
    public string Message { get; set; } = string.Empty;
}
```

### State Machines

```csharp
public enum PaymentState
{
    Created = 0,
    Authorized = 1,
    Captured = 2,
    Refunded = 3,
    Failed = 4,
    Cancelled = 5
}
```

## Limitations

- **No Flags**: `[Flags]` enums work but lose bitwise semantics in ClickHouse
- **Explicit Values**: All enum members should have explicit values for consistency
- **No Dynamic Values**: Can't add enum values without schema migration

## Best Practices

### Use Explicit Values

```csharp
// Good: Explicit values
public enum Status
{
    Pending = 0,
    Active = 1,
    Completed = 2
}

// Avoid: Implicit values (can change if order changes)
public enum Status
{
    Pending,
    Active,
    Completed
}
```

### Start from Zero

```csharp
// Good: Start from 0
public enum Priority
{
    Low = 0,
    Normal = 1,
    High = 2
}
```

### Reserve Values for Future

```csharp
public enum Status
{
    Unknown = 0,    // Reserved for unknown/default
    Pending = 1,
    Active = 2,
    Completed = 3,
    // Leave gaps for future values
    Archived = 10,
    Deleted = 20
}
```

### Document Enum Meanings

```csharp
/// <summary>
/// Order processing status.
/// </summary>
public enum OrderStatus
{
    /// <summary>Order created but not yet processed.</summary>
    Pending = 0,

    /// <summary>Order is being prepared.</summary>
    Processing = 1,

    /// <summary>Order has been shipped.</summary>
    Shipped = 2
}
```

## See Also

- [Type Mappings Overview](overview.md)
- [Scaffolding](../scaffolding.md) - Enum generation
