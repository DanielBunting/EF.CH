# Scaffolding (Reverse Engineering)

Scaffolding generates C# entity classes and DbContext from an existing ClickHouse database. This is useful when you have existing tables and want to use them with EF Core.

## Basic Usage

```bash
dotnet ef dbcontext scaffold "Host=localhost;Database=mydb" EF.CH
```

This generates:
- Entity classes for each table
- A DbContext with DbSet properties
- ClickHouse-specific configurations (ENGINE, ORDER BY, etc.)

## Options

### Output Directory

```bash
dotnet ef dbcontext scaffold "Host=localhost;Database=mydb" EF.CH \
    --output-dir Models \
    --context-dir Data
```

### Specific Tables

```bash
dotnet ef dbcontext scaffold "Host=localhost;Database=mydb" EF.CH \
    --table Orders \
    --table Products
```

### Context Name

```bash
dotnet ef dbcontext scaffold "Host=localhost;Database=mydb" EF.CH \
    --context MyDbContext
```

### Force Overwrite

```bash
dotnet ef dbcontext scaffold "Host=localhost;Database=mydb" EF.CH \
    --force
```

## What Gets Scaffolded

### Tables

Each ClickHouse table becomes an entity class:

```sql
-- ClickHouse table
CREATE TABLE Products (
    Id UUID,
    Name String,
    Price Decimal(18, 4),
    CreatedAt DateTime64(3)
)
ENGINE = MergeTree
ORDER BY (Id)
```

Generated entity:
```csharp
public class Product
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public DateTime CreatedAt { get; set; }
}
```

### Engine Configuration

ENGINE and ORDER BY are preserved:

```csharp
modelBuilder.Entity<Product>(entity =>
{
    entity.UseMergeTree(x => x.Id);
});
```

### Partitioning

PARTITION BY expressions are scaffolded:

```csharp
entity.HasPartitionBy("toYYYYMM(CreatedAt)");
```

### Special Engines

ReplacingMergeTree, SummingMergeTree, etc. are detected:

```csharp
// From: ENGINE = ReplacingMergeTree(UpdatedAt)
entity.UseReplacingMergeTree("UpdatedAt", "Id");

// From: ENGINE = CollapsingMergeTree(Sign)
entity.UseCollapsingMergeTree("Sign", "UserId", "Timestamp");
```

## Type Mappings

ClickHouse types are mapped to .NET types:

| ClickHouse | .NET |
|------------|------|
| `UUID` | `Guid` |
| `String` | `string` |
| `Int32` | `int` |
| `Int64` | `long` |
| `Float64` | `double` |
| `Decimal(p, s)` | `decimal` |
| `DateTime64(3)` | `DateTime` |
| `Date` | `DateOnly` |
| `Bool` | `bool` |
| `Array(T)` | `T[]` |
| `Nullable(T)` | `T?` |

## Enum Scaffolding

ClickHouse Enum8/Enum16 types are scaffolded as C# enums:

```sql
-- ClickHouse
CREATE TABLE Orders (
    Status Enum8('pending' = 0, 'shipped' = 1, 'delivered' = 2)
)
```

Generated:
```csharp
public enum OrderStatus
{
    Pending = 0,
    Shipped = 1,
    Delivered = 2
}

public class Order
{
    public OrderStatus Status { get; set; }
}
```

## Nested Types

Nested columns are scaffolded as array properties:

```sql
-- ClickHouse
CREATE TABLE Events (
    Tags Nested(
        Name String,
        Value String
    )
)
```

Generated:
```csharp
public class Event
{
    public string[] TagsName { get; set; } = Array.Empty<string>();
    public string[] TagsValue { get; set; } = Array.Empty<string>();
}
```

## LowCardinality

LowCardinality columns are scaffolded with appropriate configuration:

```sql
-- ClickHouse
CREATE TABLE Orders (
    Status LowCardinality(String)
)
```

Generated:
```csharp
entity.Property(e => e.Status)
    .HasColumnType("LowCardinality(String)");
```

## Complete Example

### Source Database

```sql
CREATE TABLE Events (
    Id UUID,
    Timestamp DateTime64(3),
    EventType String,
    UserId String,
    Data String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(Timestamp)
ORDER BY (Timestamp, Id)
TTL Timestamp + INTERVAL 90 DAY
```

### Scaffold Command

```bash
dotnet ef dbcontext scaffold "Host=localhost;Database=analytics" EF.CH \
    --output-dir Models \
    --context AnalyticsContext
```

### Generated Entity

```csharp
public class Event
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string UserId { get; set; } = string.Empty;
    public string Data { get; set; } = string.Empty;
}
```

### Generated DbContext

```csharp
public class AnalyticsContext : DbContext
{
    public DbSet<Event> Events => Set<Event>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse("Host=localhost;Database=analytics");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.HasNoKey();
            entity.UseMergeTree("Timestamp", "Id");
            entity.HasPartitionBy("toYYYYMM(Timestamp)");
            entity.HasTtl("Timestamp + INTERVAL 90 DAY");
        });
    }
}
```

## Updating After Schema Changes

When your database schema changes:

```bash
# Regenerate (overwrites existing files)
dotnet ef dbcontext scaffold "Host=localhost;Database=mydb" EF.CH --force
```

Consider using partial classes to preserve custom code:

```csharp
// Generated file (will be overwritten)
public partial class Product { ... }

// Custom file (preserved)
public partial class Product
{
    public decimal DiscountedPrice => Price * 0.9m;
}
```

## Limitations

### Materialized Views

Materialized views are scaffolded as regular tables. You'll need to manually configure them if you want to recreate the view.

### Complex Expressions

Some complex ClickHouse expressions may not fully round-trip:

```sql
-- Original
PARTITION BY (TenantId, toYYYYMM(CreatedAt))

-- Scaffolded as string
entity.HasPartitionBy("(TenantId, toYYYYMM(CreatedAt))");
```

### Primary Keys

ClickHouse doesn't have traditional primary keys. Scaffolding uses ORDER BY columns as a heuristic for entity keys, but tables may be scaffolded as keyless.

## Best Practices

### Start Fresh

For existing databases, scaffolding is a good starting point. Review and adjust the generated code.

### Preserve Engine Configuration

After scaffolding, verify the ENGINE configuration matches your needs - especially for specialized engines like ReplacingMergeTree.

### Use Partial Classes

Keep custom logic in separate partial class files to survive regeneration.

### Review Type Mappings

Check that scaffolded types match your expectations, especially for nullable columns and decimal precision.

## See Also

- [Migrations](migrations.md)
- [Type Mappings](types/overview.md)
- [EF Core Scaffolding Docs](https://learn.microsoft.com/en-us/ef/core/managing-schemas/scaffolding/)
