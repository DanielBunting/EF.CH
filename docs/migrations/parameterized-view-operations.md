# Parameterized View Operations

EF.CH provides migration builder extensions for creating and dropping parameterized views as part of the EF Core migration pipeline.

## CreateParameterizedView

Creates a parameterized view with typed parameter placeholders.

```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    migrationBuilder.CreateParameterizedView(
        viewName: "user_events_view",
        selectSql: @"
            SELECT event_id, event_type, timestamp, user_id
            FROM events
            WHERE user_id = {user_id:UInt64}
              AND timestamp >= {start_date:DateTime}
              AND timestamp < {end_date:DateTime}
        ",
        parameters: new[]
        {
            ("user_id", "UInt64"),
            ("start_date", "DateTime"),
            ("end_date", "DateTime")
        }
    );
}
```

Generated DDL:

```sql
CREATE VIEW "user_events_view" AS
SELECT event_id, event_type, timestamp, user_id
FROM events
WHERE user_id = {user_id:UInt64}
  AND timestamp >= {start_date:DateTime}
  AND timestamp < {end_date:DateTime}
```

### Dictionary Parameter Overload

Parameters can also be passed as a dictionary.

```csharp
migrationBuilder.CreateParameterizedView(
    viewName: "user_events_view",
    selectSql: "SELECT * FROM events WHERE user_id = {user_id:UInt64}",
    parameters: new Dictionary<string, string>
    {
        ["user_id"] = "UInt64"
    }
);
```

### Schema Support

Specify an optional schema for the view.

```csharp
migrationBuilder.CreateParameterizedView(
    viewName: "user_events_view",
    selectSql: "SELECT * FROM events WHERE user_id = {user_id:UInt64}",
    parameters: new[] { ("user_id", "UInt64") },
    schema: "analytics"
);
```

Generated DDL:

```sql
CREATE VIEW "analytics"."user_events_view" AS
SELECT * FROM events WHERE user_id = {user_id:UInt64}
```

## DropParameterizedView

Drops a parameterized view.

```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    migrationBuilder.DropParameterizedView("user_events_view");
}
```

Generated DDL:

```sql
DROP VIEW IF EXISTS "user_events_view"
```

The `ifExists` parameter defaults to `true`. Set it to `false` to generate DDL without the `IF EXISTS` clause.

```csharp
migrationBuilder.DropParameterizedView(
    viewName: "user_events_view",
    ifExists: false
);
```

```sql
DROP VIEW "user_events_view"
```

### Schema Support

```csharp
migrationBuilder.DropParameterizedView(
    viewName: "user_events_view",
    schema: "analytics"
);
```

```sql
DROP VIEW IF EXISTS "analytics"."user_events_view"
```

## Common ClickHouse Parameter Types

The parameter type string uses ClickHouse's native type names.

| ClickHouse Type | Use Case |
|----------------|----------|
| `UInt8`, `UInt16`, `UInt32`, `UInt64` | Unsigned integers |
| `Int8`, `Int16`, `Int32`, `Int64` | Signed integers |
| `Float32`, `Float64` | Floating-point numbers |
| `String` | Text values |
| `Date` | Date without time |
| `DateTime` | Date with time (second precision) |
| `DateTime64` | Date with time (sub-second precision) |
| `UUID` | Unique identifiers |

## Complete Migration Example

A migration that creates a parameterized view and later drops it:

```csharp
public partial class AddUserEventsView : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateParameterizedView(
            viewName: "user_events_view",
            selectSql: @"
                SELECT event_id, event_type, timestamp, user_id, payload
                FROM events
                WHERE user_id = {user_id:UInt64}
                  AND timestamp >= {start_date:DateTime}
                ORDER BY timestamp DESC
                LIMIT {max_results:UInt32}
            ",
            parameters: new[]
            {
                ("user_id", "UInt64"),
                ("start_date", "DateTime"),
                ("max_results", "UInt32")
            }
        );
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropParameterizedView("user_events_view");
    }
}
```

> **Note:** Unlike other EF.CH migrations that generate forward-only `Down` methods, parameterized view operations can have safe `Down` methods since `DROP VIEW IF EXISTS` is idempotent. You may choose to include a `Down` method for parameterized view migrations.

## See Also

- [Parameterized Views](../advanced/parameterized-views.md) -- runtime creation and querying
- [Migrations Overview](overview.md) -- general migration workflow
- [Projection Operations](projection-operations.md) -- another custom migration operation type
