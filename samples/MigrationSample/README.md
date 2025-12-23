# MigrationSample

Demonstrates EF Core migrations with ClickHouse, including keyless entities and partitioning.

## What This Shows

- Creating migrations with `dotnet ef migrations add`
- ClickHouse-specific DDL generation (ENGINE, ORDER BY, PARTITION BY)
- Keyless entities for append-only tables
- Applying migrations to create database schema

## Prerequisites

- .NET 8.0+
- ClickHouse server running on localhost:8123
- EF Core tools: `dotnet tool install --global dotnet-ef`

## Project Structure

```
MigrationSample/
├── Program.cs              # Entry point
├── SampleDbContext.cs      # DbContext with entity configurations
├── MigrationSample.csproj  # Project file with EF.CH and Design packages
└── Migrations/             # Generated migration files
    ├── *_InitialCreate.cs
    └── SampleDbContextModelSnapshot.cs
```

## Entities

### Product (With Key)

```csharp
public class Product
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    public decimal Price { get; set; }
    public DateTime CreatedAt { get; set; }
}
```

### Order (With Partitioning)

```csharp
public class Order
{
    public Guid Id { get; set; }
    public Guid ProductId { get; set; }
    public int Quantity { get; set; }
    public DateTime OrderDate { get; set; }
    public string CustomerName { get; set; }
}
```

Configuration:
```csharp
entity.UseMergeTree(x => new { x.OrderDate, x.Id });
entity.HasPartitionByMonth(x => x.OrderDate);
```

### EventLog (Keyless)

```csharp
public class EventLog
{
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; }
    public string Message { get; set; }
    public string? UserId { get; set; }
}
```

Configuration:
```csharp
entity.HasNoKey();
entity.UseMergeTree(x => new { x.Timestamp, x.EventType });
entity.HasPartitionByDay(x => x.Timestamp);
```

## Creating Migrations

### Initial Migration

```bash
cd samples/MigrationSample
dotnet ef migrations add InitialCreate
```

### View Generated SQL

```bash
dotnet ef migrations script
```

## Generated DDL

The migration generates ClickHouse-specific DDL:

```sql
-- Products table
CREATE TABLE "Products" (
    "Id" UUID NOT NULL,
    "Name" String NOT NULL,
    "Price" Decimal(18, 4) NOT NULL,
    "CreatedAt" DateTime64(3) NOT NULL
)
ENGINE = MergeTree
ORDER BY ("Id")

-- Orders table with partitioning
CREATE TABLE "Orders" (
    "Id" UUID NOT NULL,
    "ProductId" UUID NOT NULL,
    "Quantity" Int32 NOT NULL,
    "OrderDate" DateTime64(3) NOT NULL,
    "CustomerName" String NOT NULL
)
ENGINE = MergeTree
PARTITION BY toYYYYMM("OrderDate")
ORDER BY ("OrderDate", "Id")

-- EventLogs table (keyless)
CREATE TABLE "EventLogs" (
    "Timestamp" DateTime64(3) NOT NULL,
    "EventType" String NOT NULL,
    "Message" String NOT NULL,
    "UserId" String NULL,
    "Metadata" String NULL
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD("Timestamp")
ORDER BY ("Timestamp", "EventType")
```

## Applying Migrations

### To Database

```bash
dotnet ef database update
```

### Programmatically

```csharp
await context.Database.MigrateAsync();
```

## Adding New Migrations

After changing entities:

```bash
dotnet ef migrations add AddNewColumn
```

## Learn More

- [Migrations Documentation](../../docs/migrations.md)
- [Keyless Entities](../../docs/features/keyless-entities.md)
- [Partitioning](../../docs/features/partitioning.md)
