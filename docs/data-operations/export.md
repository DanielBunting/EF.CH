# Export

Export LINQ query results in any ClickHouse-supported format. The export extensions execute the query via ClickHouse's HTTP interface and return the raw formatted output, bypassing the ADO.NET driver's binary protocol so that text and binary formats (CSV, JSON, Parquet, etc.) are returned as-is.

All extension methods live in the `EF.CH.Extensions` namespace.

```csharp
using EF.CH.Extensions;
```

## CSV Export

```csharp
var csv = await context.Events
    .Where(e => e.Timestamp >= startOfDay)
    .ToCsvAsync(context);

File.WriteAllText("events.csv", csv);
```

Generated SQL:

```sql
SELECT e."Id", e."Timestamp", e."EventType", e."Amount"
FROM "Events" AS e
WHERE e."Timestamp" >= '2024-01-15 00:00:00'
FORMAT CSVWithNames
```

The `CSVWithNames` format includes a header row with column names.

## JSON Export

```csharp
var json = await context.Events
    .Where(e => e.EventType == "purchase")
    .Take(100)
    .ToJsonAsync(context);
```

Generated SQL:

```sql
SELECT e."Id", e."Timestamp", e."EventType", e."Amount"
FROM "Events" AS e
WHERE e."EventType" = 'purchase'
LIMIT 100
FORMAT JSON
```

The JSON format returns a structured object with `meta`, `data`, `rows`, and `statistics` fields -- the full ClickHouse JSON response.

## JSON Lines Export

```csharp
var jsonLines = await context.Events
    .ToJsonLinesAsync(context);
```

Generated SQL:

```sql
SELECT e."Id", e."Timestamp", e."EventType", e."Amount"
FROM "Events" AS e
FORMAT JSONEachRow
```

Each line is a self-contained JSON object, suitable for streaming ingestion into other systems.

## Any ClickHouse Format

Use `ToFormatAsync` to export in any format that ClickHouse supports:

```csharp
// Tab-separated values
var tsv = await context.Events
    .ToFormatAsync(context, "TabSeparatedWithNames");

// Pretty-printed table for debugging
var pretty = await context.Events
    .Take(10)
    .ToFormatAsync(context, "PrettyCompact");
```

Common ClickHouse formats:

| Format String | Description |
|---------------|-------------|
| `CSVWithNames` | CSV with header row |
| `JSON` | Full JSON response with metadata |
| `JSONEachRow` | One JSON object per line |
| `TabSeparatedWithNames` | TSV with header row |
| `Parquet` | Apache Parquet (binary) |
| `Arrow` | Apache Arrow IPC (binary) |
| `Avro` | Apache Avro (binary) |
| `PrettyCompact` | Human-readable table |

## Streaming Export

For large exports where buffering the entire result in memory is impractical, stream directly to a file or network stream:

```csharp
await using var fileStream = File.Create("events.parquet");

await context.Events
    .Where(e => e.Timestamp >= startOfMonth)
    .ToFormatStreamAsync(context, "Parquet", fileStream);
```

The response is streamed from ClickHouse to the output stream without intermediate buffering. This is essential for Parquet and other binary formats that can produce multi-gigabyte outputs.

### Streaming to an HTTP Response

```csharp
app.MapGet("/export/events", async (AppDbContext context, HttpContext http) =>
{
    http.Response.ContentType = "application/octet-stream";
    http.Response.Headers.ContentDisposition = "attachment; filename=events.parquet";

    await context.Events
        .ToFormatStreamAsync(context, "Parquet", http.Response.Body);
});
```

## Composing with LINQ

Export methods work with any `IQueryable<T>`, so all standard LINQ operators and ClickHouse-specific extensions apply:

```csharp
var csv = await context.Events
    .Final()
    .PreWhere(e => e.IsActive)
    .Where(e => e.Timestamp >= start && e.Timestamp < end)
    .OrderByDescending(e => e.Timestamp)
    .Take(10_000)
    .ToCsvAsync(context);
```

## See Also

- [Bulk Insert](bulk-insert.md) -- High-throughput data ingestion
- [INSERT...SELECT](insert-select.md) -- Server-side data movement
- [Temporary Tables](temp-tables.md) -- Stage data before export
