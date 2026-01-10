# Computed Columns

ClickHouse supports three types of computed columns: MATERIALIZED, ALIAS, and DEFAULT expressions. These allow you to derive column values from expressions rather than storing explicit values.

## Column Types Overview

| Type | Storage | Computation | Insertable | SELECT * |
|------|---------|-------------|------------|----------|
| `MATERIALIZED` | Stored on disk | On INSERT | No | Excluded by default |
| `ALIAS` | Not stored | On every read | No | Excluded by default |
| `DEFAULT` | Stored if provided | When no value given | Yes | Included |

## When to Use Each Type

### MATERIALIZED

Use when:
- The derived value is needed frequently in queries
- Storage cost is acceptable for faster reads
- The value should be computed once at insert time

```csharp
// Good: Tax calculation used in many reports
[MaterializedColumn("Amount * 1.1")]
public decimal TotalWithTax { get; set; }

// Good: Hash computed once, used for deduplication
[MaterializedColumn("sipHash64(concat(UserId, EventType, toString(Timestamp)))")]
public ulong EventHash { get; set; }
```

### ALIAS

Use when:
- The value is rarely queried
- Storage cost needs to be minimized
- The underlying data may change (the alias reflects current state)

```csharp
// Good: Full name rarely needed, just concatenation
[AliasColumn("concat(FirstName, ' ', LastName)")]
public string FullName { get; set; }

// Good: Derived status computed on-demand
[AliasColumn("if(Amount > 1000, 'large', 'small')")]
public string OrderSize { get; set; }
```

### DEFAULT Expression

Use when:
- Auto-generating values like timestamps or UUIDs
- Values can still be explicitly provided on INSERT
- You need a server-side default (not client-side)

```csharp
// Good: Auto-generated timestamp
[DefaultExpression("now()")]
public DateTime CreatedAt { get; set; }

// Good: Auto-generated UUID
[DefaultExpression("generateUUIDv4()")]
public Guid EventId { get; set; }
```

## Configuration

### Attribute Decorators

```csharp
using EF.CH.Metadata.Attributes;

public class Order
{
    public Guid Id { get; set; }
    public decimal Amount { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;

    // MATERIALIZED - computed on INSERT, stored on disk
    [MaterializedColumn("Amount * 1.1")]
    public decimal TotalWithTax { get; set; }

    // ALIAS - computed at query time, not stored
    [AliasColumn("concat(FirstName, ' ', LastName)")]
    public string FullName { get; set; } = string.Empty;

    // DEFAULT expression - computed if no value provided
    [DefaultExpression("now()")]
    public DateTime CreatedAt { get; set; }
}
```

### Fluent API

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.CreatedAt, x.Id });

    // MATERIALIZED
    entity.Property(e => e.TotalWithTax)
        .HasMaterializedExpression("Amount * 1.1");

    // ALIAS
    entity.Property(e => e.FullName)
        .HasAliasExpression("concat(FirstName, ' ', LastName)");

    // DEFAULT expression
    entity.Property(e => e.CreatedAt)
        .HasDefaultExpression("now()");
});
```

## Generated DDL

```sql
CREATE TABLE "Order" (
    "Id" UUID,
    "Amount" Decimal(18,4),
    "FirstName" String,
    "LastName" String,
    "TotalWithTax" Decimal(18,4) MATERIALIZED Amount * 1.1,
    "FullName" String ALIAS concat(FirstName, ' ', LastName),
    "CreatedAt" DateTime64(3) DEFAULT now()
)
ENGINE = MergeTree
ORDER BY ("CreatedAt", "Id")
```

## ValueGenerated Behavior

The provider automatically configures `ValueGenerated` based on column type:

| Column Type | ValueGenerated | Effect |
|-------------|----------------|--------|
| `MATERIALIZED` | `OnAdd` | Excluded from INSERT statements |
| `ALIAS` | `OnAddOrUpdate` | Excluded from INSERT and UPDATE |
| `DEFAULT` | Normal | Included in INSERT if value provided |

This means you don't need to manually configure `ValueGeneratedOnAdd()` for MATERIALIZED columns - it's done automatically.

## Expression Syntax

Expressions use ClickHouse SQL syntax, not C# syntax:

```csharp
// String concatenation
[AliasColumn("concat(FirstName, ' ', LastName)")]

// Arithmetic
[MaterializedColumn("Amount * 1.1")]
[MaterializedColumn("Price * Quantity")]

// Conditionals
[AliasColumn("if(Amount > 1000, 'large', 'small')")]
[AliasColumn("multiIf(Status = 1, 'active', Status = 2, 'pending', 'unknown')")]

// Date functions
[DefaultExpression("now()")]
[MaterializedColumn("toStartOfHour(Timestamp)")]
[MaterializedColumn("toYYYYMM(OrderDate)")]

// Hash functions
[MaterializedColumn("sipHash64(UserId)")]
[MaterializedColumn("cityHash64(concat(A, B, C))")]

// Type conversions
[MaterializedColumn("toUInt32(Amount)")]
[AliasColumn("toString(Id)")]
```

## Complete Example

```csharp
public class SensorReading
{
    public Guid Id { get; set; }
    public long SensorId { get; set; }
    public double Value { get; set; }
    public double? MinThreshold { get; set; }
    public double? MaxThreshold { get; set; }

    // Store timestamp bucket for efficient querying
    [MaterializedColumn("toStartOfMinute(Timestamp)")]
    public DateTime MinuteBucket { get; set; }

    // Virtual column for alert status
    [AliasColumn("if(Value < MinThreshold OR Value > MaxThreshold, 'alert', 'normal')")]
    public string Status { get; set; } = string.Empty;

    // Auto-generated insert timestamp
    [DefaultExpression("now()")]
    public DateTime Timestamp { get; set; }

    // Auto-generated UUID for external reference
    [DefaultExpression("generateUUIDv4()")]
    public Guid ExternalId { get; set; }
}

// DbContext configuration
public class SensorDbContext : DbContext
{
    public DbSet<SensorReading> Readings => Set<SensorReading>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SensorReading>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.SensorId });
            entity.HasPartitionByDay(x => x.Timestamp);
        });
    }
}
```

## Generated DDL for Complete Example

```sql
CREATE TABLE "SensorReading" (
    "Id" UUID,
    "SensorId" Int64,
    "Value" Float64,
    "MinThreshold" Nullable(Float64),
    "MaxThreshold" Nullable(Float64),
    "MinuteBucket" DateTime64(3) MATERIALIZED toStartOfMinute(Timestamp),
    "Status" String ALIAS if(Value < MinThreshold OR Value > MaxThreshold, 'alert', 'normal'),
    "Timestamp" DateTime64(3) DEFAULT now(),
    "ExternalId" UUID DEFAULT generateUUIDv4()
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD("Timestamp")
ORDER BY ("Timestamp", "SensorId")
```

## Precedence

Fluent API configuration **overrides** attribute decorators (standard EF Core behavior):

```csharp
public class Example
{
    [MaterializedColumn("Amount * 1.1")]  // Attribute says MATERIALIZED
    public decimal Computed { get; set; }
}

modelBuilder.Entity<Example>(entity =>
{
    // Fluent API wins - will be ALIAS, not MATERIALIZED
    entity.Property(e => e.Computed).HasAliasExpression("Amount * 1.2");
});
```

## Combining with Codecs

Computed columns can have compression codecs:

```csharp
// Attribute combination
[MaterializedColumn("toStartOfHour(Timestamp)")]
[TimestampCodec]
public DateTime HourBucket { get; set; }

// Fluent API
entity.Property(e => e.HourBucket)
    .HasMaterializedExpression("toStartOfHour(Timestamp)")
    .HasCodec(c => c.DoubleDelta().LZ4());
```

**Generated DDL:**
```sql
"HourBucket" DateTime64(3) MATERIALIZED toStartOfHour(Timestamp) CODEC(DoubleDelta, LZ4)
```

## Querying MATERIALIZED and ALIAS Columns

MATERIALIZED and ALIAS columns are excluded from `SELECT *` by default. To read them:

```csharp
// Explicit column selection includes them
var orders = await context.Orders
    .Select(o => new { o.Id, o.Amount, o.TotalWithTax, o.FullName })
    .ToListAsync();

// EF Core projections work normally
var totals = await context.Orders
    .Where(o => o.TotalWithTax > 100)
    .ToListAsync();
```

In raw SQL:
```sql
-- Excluded from SELECT *
SELECT * FROM "Order";  -- TotalWithTax and FullName not returned

-- Must explicitly select
SELECT *, "TotalWithTax", "FullName" FROM "Order";
```

## Inserting Data

### MATERIALIZED Columns

Cannot be inserted - ClickHouse computes the value:

```csharp
// TotalWithTax is computed automatically from Amount
context.Orders.Add(new Order
{
    Id = Guid.NewGuid(),
    Amount = 100m,  // TotalWithTax will be 110
    FirstName = "John",
    LastName = "Doe"
});
await context.SaveChangesAsync();
```

### ALIAS Columns

Cannot be inserted - virtual columns have no storage:

```csharp
// FullName is not stored, computed on read
context.Orders.Add(new Order
{
    Id = Guid.NewGuid(),
    Amount = 100m,
    FirstName = "John",  // FullName computed as "John Doe"
    LastName = "Doe"
});
```

### DEFAULT Expression Columns

Can be omitted (uses expression) or explicitly provided:

```csharp
// CreatedAt uses now() from server
context.Orders.Add(new Order
{
    Id = Guid.NewGuid(),
    Amount = 100m,
    FirstName = "John",
    LastName = "Doe"
    // CreatedAt not set - will use now()
});

// Or explicitly provide a value
context.Orders.Add(new Order
{
    Id = Guid.NewGuid(),
    Amount = 100m,
    FirstName = "John",
    LastName = "Doe",
    CreatedAt = new DateTime(2024, 1, 1)  // Explicit value used
});
```

## DEFAULT Expression vs DefaultValueSql

EF Core has `HasDefaultValueSql()` for SQL defaults. `HasDefaultExpression()` is similar but ClickHouse-specific:

| Feature | `HasDefaultValueSql()` | `HasDefaultExpression()` |
|---------|------------------------|--------------------------|
| EF Core standard | Yes | No (ClickHouse-specific) |
| ClickHouse DDL | `DEFAULT <value>` | `DEFAULT <expression>` |
| Expression support | Limited | Full ClickHouse SQL |
| Recommended for ClickHouse | No | Yes |

```csharp
// EF Core standard (works but limited)
entity.Property(e => e.CreatedAt).HasDefaultValueSql("now()");

// ClickHouse-specific (recommended)
entity.Property(e => e.CreatedAt).HasDefaultExpression("now()");
```

## Limitations

### No Expression Validation

Expressions are not validated at build time. Invalid expressions will fail at table creation:

```csharp
// Will fail when table is created
[MaterializedColumn("invalid_function(x)")]
public string Bad { get; set; }
```

### Column References Must Exist

Referenced columns must be defined in the same table:

```csharp
// FullName references FirstName and LastName
[AliasColumn("concat(FirstName, ' ', LastName)")]
public string FullName { get; set; }

// These must exist:
public string FirstName { get; set; }
public string LastName { get; set; }
```

### No Cross-Table References

Expressions cannot reference other tables:

```csharp
// Invalid - cannot reference other tables
[AliasColumn("(SELECT Name FROM Categories WHERE Id = CategoryId)")]
public string CategoryName { get; set; }  // Won't work
```

### ALTER TABLE Limitations

Changing a column from regular to MATERIALIZED/ALIAS requires special handling:

```sql
-- Cannot simply change column type
-- Must: DROP column, ADD column with new definition
ALTER TABLE "Order" DROP COLUMN "TotalWithTax";
ALTER TABLE "Order" ADD COLUMN "TotalWithTax" Decimal(18,4) MATERIALIZED Amount * 1.1;
```

## Best Practices

### 1. Use MATERIALIZED for Frequently Queried Derived Values

```csharp
// Good: Used in WHERE clauses often
[MaterializedColumn("toYYYYMM(OrderDate)")]
public int YearMonth { get; set; }

// Query is fast - reads stored value
var orders = context.Orders.Where(o => o.YearMonth == 202401).ToList();
```

### 2. Use ALIAS for Rarely Used Virtual Columns

```csharp
// Good: Only needed in specific reports
[AliasColumn("concat(Street, ', ', City, ', ', Country)")]
public string FullAddress { get; set; }
```

### 3. Use DEFAULT for Auto-Generated Timestamps/IDs

```csharp
[DefaultExpression("now()")]
public DateTime CreatedAt { get; set; }

[DefaultExpression("generateUUIDv4()")]
public Guid TraceId { get; set; }
```

### 4. Consider MATERIALIZED for ORDER BY Key Derivatives

```csharp
// Store the bucket for efficient range scans
[MaterializedColumn("toStartOfHour(EventTime)")]
public DateTime HourBucket { get; set; }

// Configure as part of ORDER BY
entity.UseMergeTree(x => new { x.HourBucket, x.Id });
```

### 5. Document Expression Dependencies

```csharp
public class Order
{
    // These are used by TotalWithTax
    public decimal Amount { get; set; }
    public decimal TaxRate { get; set; }

    /// <summary>
    /// MATERIALIZED: Amount * (1 + TaxRate)
    /// Depends on: Amount, TaxRate
    /// </summary>
    [MaterializedColumn("Amount * (1 + TaxRate)")]
    public decimal TotalWithTax { get; set; }
}
```

## See Also

- [Compression Codecs](compression-codecs.md)
- [Partitioning](partitioning.md)
- [TTL (Time-To-Live)](ttl.md)
- [MergeTree Engine](../engines/mergetree.md)
- [ClickHouse MATERIALIZED Documentation](https://clickhouse.com/docs/en/sql-reference/statements/create/table#materialized)
