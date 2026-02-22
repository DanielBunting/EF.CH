# Enum Types

## CLR to ClickHouse Mapping

```
Any C# enum  --> Enum8('Name1' = Value1, 'Name2' = Value2, ...)
Any C# enum  --> Enum16('Name1' = Value1, 'Name2' = Value2, ...)
```

The provider automatically selects `Enum8` or `Enum16` based on the range of values defined in the enum.

## Selection Rules

- **Enum8**: Used when all values fit in `[-128, 127]` and the enum has at most 256 members.
- **Enum16**: Used when any value falls outside the Enum8 range, or the enum has more than 256 members. Supports `[-32768, 32767]` with up to 65,536 members.

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

public class Order
{
    public uint Id { get; set; }
    public OrderStatus Status { get; set; }
}
```

```sql
CREATE TABLE "Orders" (
    "Id" UInt32,
    "Status" Enum8('Pending' = 0, 'Processing' = 1, 'Shipped' = 2, 'Delivered' = 3, 'Cancelled' = 4)
) ENGINE = MergeTree() ORDER BY ("Id")
```

## Enum16 Example

```csharp
public enum ExtendedStatus
{
    Critical = -100,
    Normal = 0,
    HighPriority = 200  // Exceeds Enum8 max of 127
}
```

```sql
-- Auto-selected as Enum16 because 200 > 127
"Priority" Enum16('Critical' = -100, 'Normal' = 0, 'HighPriority' = 200)
```

## Value Conversion

Enum values are stored and transmitted as strings in ClickHouse. The provider uses a built-in `EnumToStringConverter<TEnum>` that converts between the C# enum and its string name:

- **Write**: `OrderStatus.Shipped` becomes `'Shipped'`
- **Read**: `'Shipped'` becomes `OrderStatus.Shipped`

```csharp
context.Orders.Where(o => o.Status == OrderStatus.Shipped)
```

```sql
SELECT * FROM "Orders" WHERE "Status" = 'Shipped'
```

## Nullable Enums

Nullable enum properties map to `Nullable(Enum8(...))` or `Nullable(Enum16(...))`:

```csharp
public OrderStatus? CancellationReason { get; set; }
// DDL: "CancellationReason" Nullable(Enum8('Pending' = 0, ...))
```

## Migration Behavior

When the enum definition changes in C# code:

- **Adding new members**: Requires an `ALTER TABLE MODIFY COLUMN` to update the enum definition.
- **Removing members**: Existing data referencing removed members will cause read errors. Migrate data first.
- **Changing numeric values**: Requires column modification. The string names are the primary identifiers in ClickHouse.

## Scaffolding Behavior

When scaffolding from an existing ClickHouse database, `Enum8` and `Enum16` columns are reverse-engineered into C# enum types with the corresponding member names and integer values.

## See Also

- [Type System Overview](overview.md)
