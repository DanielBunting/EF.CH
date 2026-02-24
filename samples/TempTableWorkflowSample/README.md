# TempTableWorkflowSample

Demonstrates a realistic ETL-style workflow using ClickHouse temporary tables for multi-step data processing: extract, filter, enrich, and load.

## What This Shows

- **BulkInsert** to simulate importing raw external data
- **ToTempTableAsync** to create temp tables from filtered queries
- **CreateTempTableAsync** to create empty temp tables for lookup data
- **InsertAsync** to populate temp tables with reference data
- **LINQ composition** on temp table queries (GroupBy, OrderBy, Where)
- **INSERT...SELECT** to load from temp tables into final destination tables
- **BeginTempTableScope** for automatic cleanup of multiple temp tables
- **Multi-step ETL pipeline**: extract -> filter -> analyze -> load

## ETL Pipeline

```
External Data (simulated CSV)
    |
    v
[RawImport] ---BulkInsert---> Source Table
    |
    |---ToTempTableAsync---> temp_valid_records (filtered)
    |
    |---CreateTempTableAsync---> temp_lookups (reference data)
    |                               |
    |                               |--- InsertAsync (populate)
    |                               |
    |<---------- Join / Enrich -----|
    |
    |---ExecuteInsertFromQueryAsync---> [FinalRecord] (destination)
    |
    |---BeginTempTableScope--->
         |--- Extract (temp #1)
         |--- Filter  (temp #2)
         |--- Load    (INSERT...SELECT)
         |--- Auto-cleanup on dispose
```

## API Reference

| Feature | API Used |
|---------|----------|
| Bulk import | `context.BulkInsertAsync(rawRecords)` |
| Temp from query | `query.ToTempTableAsync(context, "name")` |
| Empty temp table | `context.CreateTempTableAsync<T>("name")` |
| Populate temp | `tempTable.InsertAsync(entities)` |
| Insert into temp | `query.InsertIntoTempTableAsync(tempTable)` |
| Query temp table | `tempTable.Query().Where(...).ToListAsync()` |
| INSERT...SELECT | `dbSet.ExecuteInsertFromQueryAsync(sourceQuery, mapping)` |
| Scoped cleanup | `context.BeginTempTableScope()` |
| Scoped create | `scope.CreateFromQueryAsync(query)` |

## Prerequisites

- .NET 8.0+
- Docker (for running ClickHouse)

## Running

Start a local ClickHouse instance:

```bash
docker run -d --name clickhouse-etl \
  -p 8123:8123 -p 9000:9000 \
  clickhouse/clickhouse-server:latest
```

Run the sample:

```bash
dotnet run
```

Stop ClickHouse when done:

```bash
docker stop clickhouse-etl && docker rm clickhouse-etl
```

## Expected Output

```
Temp Table Workflow Sample
=========================

Creating database and tables...
Setup complete.

--- Step 1: Import Raw Data ---
Imported 10,000 raw records.
Raw import table: 10,000 rows

--- Step 2: Extract and Filter (Temp Table) ---
Filtered to 5,000 valid records in temp table 'temp_valid_records'
Valid records by category:
  electronics       1,050 records
  clothing          1,020 records
  food                990 records
  furniture           970 records
  books               970 records
Valid records by region:
  US-EAST           1,020 records
  US-WEST           1,010 records
  EU-WEST             995 records
  EU-EAST             990 records
  APAC                985 records
Temp table dropped on dispose.

--- Step 3: Enrich with Lookup Data ---
Created lookup temp table: temp_lookups
Populated 5 lookup mappings.
Lookup mappings loaded:
  books           -> Books & Media              Tax: 0%   WH: WH-05
  clothing        -> Apparel & Fashion          Tax: 6%   WH: WH-02
  electronics     -> Consumer Electronics       Tax: 8%   WH: WH-01
  food            -> Food & Beverages           Tax: 2%   WH: WH-03
  furniture       -> Home Furniture             Tax: 7%   WH: WH-04

Enriched category summary:
  Consumer Electronics       1,050 records  Tax: 8%  Ship from: WH-01
  Apparel & Fashion          1,020 records  Tax: 6%  Ship from: WH-02
  ...
Temp tables dropped on dispose.

--- Step 4: Load to Final Table (INSERT...SELECT) ---
Staging 5,000 valid records in temp table.
INSERT...SELECT completed in 25ms
Final table now contains 5,000 records.
Sample final records:
  EXT-123456 | electronics  | US-EAST  | Amount: 450.25
  ...

--- Step 5: Scoped Multi-Step ETL Pipeline ---
Using BeginTempTableScope for automatic cleanup.

[Extract] Filtering raw imports by region...
[Extract] 4,000 US records extracted to temp table.

[Filter] Removing invalid and duplicate records...
[Filter] 2,000 valid US records after quality filter.

[Analyze] Processing filtered data...
US region breakdown:
  US-EAST      1,020 valid records
  US-WEST        980 valid records
Category breakdown (US only):
  electronics      420 records
  clothing         400 records
  ...

[Load] Moving filtered data to final table...
[Load] Loaded 2,000 records to final table in 15ms.

[Pipeline Summary]
  Raw input:     4,000 US records
  After filter:  2,000 valid records
  Final output:  2,000 loaded records

All temp tables automatically dropped by scope dispose.

Done!
```

## Key Code

### Create Temp Table from Query

```csharp
// Filter raw data and materialize into a temp table
await using var tempFiltered = await context.RawImports
    .Where(r => r.Status == "valid")
    .ToTempTableAsync(context, "temp_valid_records");

// Query the temp table like any other table
var count = await tempFiltered.Query().CountAsync();
```

### Create Empty Temp Table and Populate

```csharp
// Create empty temp table from entity schema
await using var tempLookup = await context.CreateTempTableAsync<LookupMapping>("temp_lookups");

// Insert reference data
await tempLookup.InsertAsync(lookups);
```

### INSERT...SELECT to Final Table

```csharp
// Server-side data movement - no client round-trip
var result = await context.FinalRecords.ExecuteInsertFromQueryAsync(
    context.RawImports.Where(r => r.Status == "valid"),
    raw => new FinalRecord
    {
        ExternalId = raw.ExternalId,
        CategoryCode = raw.CategoryCode,
        Amount = raw.RawAmount,
        ProcessedAt = DateTime.UtcNow
    });
```

### Scoped Multi-Table Workflow

```csharp
// All temp tables cleaned up when scope is disposed
await using (var scope = context.BeginTempTableScope())
{
    var extract = await scope.CreateFromQueryAsync(sourceQuery);
    var filtered = await scope.CreateFromQueryAsync(extract.Query().Where(...));

    await context.FinalRecords.ExecuteInsertFromQueryAsync(
        filtered.Query(), mapping);
}
// Temp tables automatically dropped here
```

## Learn More

- [Temporary Tables](../../docs/features/temp-tables.md)
- [Bulk Insert](../../docs/features/bulk-insert.md)
- [INSERT...SELECT](../../docs/features/insert-select.md)
