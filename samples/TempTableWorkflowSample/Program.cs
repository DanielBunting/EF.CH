using EF.CH.BulkInsert;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;

// ============================================================
// Temp Table Workflow Sample
// ============================================================
// Demonstrates a realistic ETL-style workflow using temporary
// tables for multi-step data processing:
//
// Pipeline: External import -> Filter (temp) -> Enrich (temp) -> Load (final)
//
// Steps:
// 1. Create source table with raw import data
// 2. BulkInsert raw data (simulating external file import)
// 3. Filter raw data into a temp table via ToTempTableAsync
// 4. Create a lookup temp table with CreateTempTableAsync
// 5. Populate lookup temp table with InsertIntoTempTableAsync
// 6. Query joining temp tables with main tables
// 7. INSERT...SELECT from temp table into final destination
// 8. BeginTempTableScope for automatic multi-table cleanup
// 9. Full multi-step ETL: extract -> filter -> enrich -> load
// ============================================================

var container = new ClickHouseBuilder()
    .WithImage("clickhouse/clickhouse-server:latest")
    .Build();

Console.WriteLine("Starting ClickHouse container...");
await container.StartAsync();
Console.WriteLine("ClickHouse container started.\n");

try
{
    var connectionString = container.GetConnectionString();

    Console.WriteLine("Temp Table Workflow Sample");
    Console.WriteLine("=========================\n");

    await using var context = new EtlDbContext(connectionString);

    await SetupTablesAsync(context);
    await ImportRawDataAsync(context);
    await ExtractAndFilterAsync(context);
    await EnrichWithLookupsAsync(context);
    await LoadToFinalAsync(context);
    await ScopedWorkflowAsync(context);

    Console.WriteLine("\nDone!");
}
finally
{
    Console.WriteLine("\nStopping container...");
    await container.DisposeAsync();
    Console.WriteLine("Done.");
}

// ============================================================
// Setup: Create all permanent tables
// ============================================================

static async Task SetupTablesAsync(EtlDbContext context)
{
    Console.WriteLine("Creating database and tables...");
    await context.Database.EnsureCreatedAsync();
    Console.WriteLine("Setup complete.\n");
}

// ============================================================
// Step 1: Import raw data (simulating external file import)
// ============================================================

static async Task ImportRawDataAsync(EtlDbContext context)
{
    Console.WriteLine("--- Step 1: Import Raw Data ---");

    var random = new Random(42);
    var categories = new[] { "electronics", "clothing", "food", "furniture", "books" };
    var regions = new[] { "US-EAST", "US-WEST", "EU-WEST", "EU-EAST", "APAC" };
    var statuses = new[] { "valid", "valid", "valid", "invalid", "pending", "duplicate" };

    // Simulate importing raw string data from an external CSV
    var rawRecords = Enumerable.Range(1, 10_000)
        .Select(i => new RawImport
        {
            RowId = i,
            RawDate = DateTime.UtcNow.AddDays(-random.Next(1, 90)).ToString("yyyy-MM-dd"),
            RawAmount = (random.NextDouble() * 1000).ToString("F2"),
            CategoryCode = categories[random.Next(categories.Length)],
            RegionCode = regions[random.Next(regions.Length)],
            Status = statuses[random.Next(statuses.Length)],
            ExternalId = $"EXT-{random.Next(100000, 999999)}"
        })
        .ToList();

    var result = await context.BulkInsertAsync(rawRecords);
    Console.WriteLine($"Imported {result.RowsInserted:N0} raw records.");

    var totalCount = await context.RawImports.CountAsync();
    Console.WriteLine($"Raw import table: {totalCount:N0} rows\n");
}

// ============================================================
// Step 2: Extract and filter into a temp table
// ============================================================

static async Task ExtractAndFilterAsync(EtlDbContext context)
{
    Console.WriteLine("--- Step 2: Extract and Filter (Temp Table) ---");

    // Filter: only valid records with amount > 100
    var validRecordsQuery = context.RawImports
        .Where(r => r.Status == "valid");

    // Create temp table from filtered query
    await using var tempFiltered = await validRecordsQuery
        .ToTempTableAsync(context, "temp_valid_records");

    var filteredCount = await tempFiltered.Query().CountAsync();
    Console.WriteLine($"Filtered to {filteredCount:N0} valid records in temp table '{tempFiltered.TableName}'");

    // Further analysis on filtered temp table
    var categoryCounts = await tempFiltered.Query()
        .GroupBy(r => r.CategoryCode)
        .Select(g => new { Category = g.Key, Count = g.Count() })
        .OrderByDescending(x => x.Count)
        .ToListAsync();

    Console.WriteLine("Valid records by category:");
    foreach (var cat in categoryCounts)
    {
        Console.WriteLine($"  {cat.Category,-15} {cat.Count,6:N0} records");
    }

    // Regional breakdown
    var regionCounts = await tempFiltered.Query()
        .GroupBy(r => r.RegionCode)
        .Select(g => new { Region = g.Key, Count = g.Count() })
        .OrderByDescending(x => x.Count)
        .ToListAsync();

    Console.WriteLine("Valid records by region:");
    foreach (var reg in regionCounts)
    {
        Console.WriteLine($"  {reg.Region,-15} {reg.Count,6:N0} records");
    }

    Console.WriteLine("Temp table dropped on dispose.\n");
}

// ============================================================
// Step 3: Enrich with lookup data via temp tables
// ============================================================

static async Task EnrichWithLookupsAsync(EtlDbContext context)
{
    Console.WriteLine("--- Step 3: Enrich with Lookup Data ---");

    // Create a temp table for lookup/reference data
    await using var tempLookup = await context.CreateTempTableAsync<LookupMapping>("temp_lookups");
    Console.WriteLine($"Created lookup temp table: {tempLookup.TableName}");

    // Populate lookup table with reference mappings
    var lookups = new List<LookupMapping>
    {
        new() { Code = "electronics", DisplayName = "Consumer Electronics", TaxRate = 0.08m, Warehouse = "WH-01" },
        new() { Code = "clothing", DisplayName = "Apparel & Fashion", TaxRate = 0.06m, Warehouse = "WH-02" },
        new() { Code = "food", DisplayName = "Food & Beverages", TaxRate = 0.02m, Warehouse = "WH-03" },
        new() { Code = "furniture", DisplayName = "Home Furniture", TaxRate = 0.07m, Warehouse = "WH-04" },
        new() { Code = "books", DisplayName = "Books & Media", TaxRate = 0.00m, Warehouse = "WH-05" }
    };

    await tempLookup.InsertAsync(lookups);
    Console.WriteLine($"Populated {lookups.Count} lookup mappings.");

    // Verify lookup data
    var lookupData = await tempLookup.Query()
        .OrderBy(l => l.Code)
        .ToListAsync();

    Console.WriteLine("Lookup mappings loaded:");
    foreach (var lk in lookupData)
    {
        Console.WriteLine($"  {lk.Code,-15} -> {lk.DisplayName,-25} Tax: {lk.TaxRate:P0}  WH: {lk.Warehouse}");
    }

    // Create filtered temp table from raw data
    await using var tempValid = await context.RawImports
        .Where(r => r.Status == "valid")
        .ToTempTableAsync(context, "temp_valid_for_enrich");

    var validCount = await tempValid.Query().CountAsync();
    Console.WriteLine($"\nFiltered {validCount:N0} valid records for enrichment.");

    // Query joining temp tables: raw data + lookup data
    // Note: In a real scenario you would join these server-side.
    // Here we demonstrate querying each temp table independently.
    var categoryStats = await tempValid.Query()
        .GroupBy(r => r.CategoryCode)
        .Select(g => new
        {
            Category = g.Key,
            Count = g.Count()
        })
        .ToListAsync();

    var lookupDict = lookupData.ToDictionary(l => l.Code);

    Console.WriteLine("\nEnriched category summary:");
    foreach (var stat in categoryStats.OrderByDescending(s => s.Count))
    {
        if (lookupDict.TryGetValue(stat.Category, out var lookup))
        {
            Console.WriteLine($"  {lookup.DisplayName,-25} {stat.Count,6:N0} records  " +
                              $"Tax: {lookup.TaxRate:P0}  Ship from: {lookup.Warehouse}");
        }
    }

    Console.WriteLine("Temp tables dropped on dispose.\n");
}

// ============================================================
// Step 4: Load to final destination via INSERT...SELECT
// ============================================================

static async Task LoadToFinalAsync(EtlDbContext context)
{
    Console.WriteLine("--- Step 4: Load to Final Table (INSERT...SELECT) ---");

    // Create temp table with only valid, high-value records
    await using var tempHighValue = await context.RawImports
        .Where(r => r.Status == "valid")
        .ToTempTableAsync(context, "temp_high_value");

    var tempCount = await tempHighValue.Query().CountAsync();
    Console.WriteLine($"Staging {tempCount:N0} valid records in temp table.");

    // INSERT...SELECT from source to final table
    // This moves data server-side without client round-trips
    var insertResult = await context.FinalRecords.ExecuteInsertFromQueryAsync(
        context.RawImports.Where(r => r.Status == "valid"),
        raw => new FinalRecord
        {
            ExternalId = raw.ExternalId,
            CategoryCode = raw.CategoryCode,
            RegionCode = raw.RegionCode,
            Amount = raw.RawAmount,
            ImportDate = raw.RawDate,
            ProcessedAt = DateTime.UtcNow
        });

    Console.WriteLine($"INSERT...SELECT completed in {insertResult.Elapsed.TotalMilliseconds:F0}ms");

    var finalCount = await context.FinalRecords.CountAsync();
    Console.WriteLine($"Final table now contains {finalCount:N0} records.");

    // Verify data in final table
    var sampleRecords = await context.FinalRecords
        .OrderByDescending(f => f.ProcessedAt)
        .Take(5)
        .ToListAsync();

    Console.WriteLine("Sample final records:");
    foreach (var record in sampleRecords)
    {
        Console.WriteLine($"  {record.ExternalId} | {record.CategoryCode,-12} | " +
                          $"{record.RegionCode,-8} | Amount: {record.Amount}");
    }

    Console.WriteLine();
}

// ============================================================
// Step 5: Scoped workflow with BeginTempTableScope
// ============================================================

static async Task ScopedWorkflowAsync(EtlDbContext context)
{
    Console.WriteLine("--- Step 5: Scoped Multi-Step ETL Pipeline ---");
    Console.WriteLine("Using BeginTempTableScope for automatic cleanup.\n");

    await using (var scope = context.BeginTempTableScope())
    {
        // EXTRACT: Pull raw data into first temp table
        Console.WriteLine("[Extract] Filtering raw imports by region...");
        var extractTemp = await scope.CreateFromQueryAsync(
            context.RawImports.Where(r => r.RegionCode == "US-EAST" || r.RegionCode == "US-WEST"));

        var extractedCount = await extractTemp.Query().CountAsync();
        Console.WriteLine($"[Extract] {extractedCount:N0} US records extracted to temp table.");

        // FILTER: Apply quality checks, move to second temp table
        Console.WriteLine("\n[Filter] Removing invalid and duplicate records...");
        var filterTemp = await scope.CreateFromQueryAsync(
            extractTemp.Query().Where(r => r.Status == "valid"));

        var filteredCount = await filterTemp.Query().CountAsync();
        Console.WriteLine($"[Filter] {filteredCount:N0} valid US records after quality filter.");

        // ANALYZE: Run analytics on filtered temp table
        Console.WriteLine("\n[Analyze] Processing filtered data...");

        var regionSummary = await filterTemp.Query()
            .GroupBy(r => r.RegionCode)
            .Select(g => new
            {
                Region = g.Key,
                Count = g.Count()
            })
            .OrderByDescending(x => x.Count)
            .ToListAsync();

        Console.WriteLine("US region breakdown:");
        foreach (var region in regionSummary)
        {
            Console.WriteLine($"  {region.Region,-10} {region.Count,6:N0} valid records");
        }

        var categorySummary = await filterTemp.Query()
            .GroupBy(r => r.CategoryCode)
            .Select(g => new
            {
                Category = g.Key,
                Count = g.Count()
            })
            .OrderByDescending(x => x.Count)
            .ToListAsync();

        Console.WriteLine("Category breakdown (US only):");
        foreach (var cat in categorySummary)
        {
            Console.WriteLine($"  {cat.Category,-15} {cat.Count,6:N0} records");
        }

        // LOAD: INSERT...SELECT from filtered temp to final table
        Console.WriteLine("\n[Load] Moving filtered data to final table...");

        // Clear existing final records for clean demo
        await context.Database.ExecuteSqlRawAsync(@"TRUNCATE TABLE ""final_records""");

        var loadResult = await context.FinalRecords.ExecuteInsertFromQueryAsync(
            filterTemp.Query(),
            raw => new FinalRecord
            {
                ExternalId = raw.ExternalId,
                CategoryCode = raw.CategoryCode,
                RegionCode = raw.RegionCode,
                Amount = raw.RawAmount,
                ImportDate = raw.RawDate,
                ProcessedAt = DateTime.UtcNow
            });

        var finalCount = await context.FinalRecords.CountAsync();
        Console.WriteLine($"[Load] Loaded {finalCount:N0} records to final table " +
                          $"in {loadResult.Elapsed.TotalMilliseconds:F0}ms.");

        Console.WriteLine("\n[Pipeline Summary]");
        Console.WriteLine($"  Raw input:     {extractedCount:N0} US records");
        Console.WriteLine($"  After filter:  {filteredCount:N0} valid records");
        Console.WriteLine($"  Final output:  {finalCount:N0} loaded records");
    }

    Console.WriteLine("\nAll temp tables automatically dropped by scope dispose.");
}

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// Raw import data. All fields are strings to simulate ingesting
/// untyped data from an external CSV or file.
/// </summary>
public class RawImport
{
    public int RowId { get; set; }
    public string RawDate { get; set; } = string.Empty;
    public string RawAmount { get; set; } = string.Empty;
    public string CategoryCode { get; set; } = string.Empty;
    public string RegionCode { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public string ExternalId { get; set; } = string.Empty;
}

/// <summary>
/// Lookup/reference data for enrichment. Loaded into a temp table
/// to join with raw imports during processing.
/// </summary>
public class LookupMapping
{
    public string Code { get; set; } = string.Empty;
    public string DisplayName { get; set; } = string.Empty;
    public decimal TaxRate { get; set; }
    public string Warehouse { get; set; } = string.Empty;
}

/// <summary>
/// Final destination table for processed, validated, enriched records.
/// </summary>
public class FinalRecord
{
    public string ExternalId { get; set; } = string.Empty;
    public string CategoryCode { get; set; } = string.Empty;
    public string RegionCode { get; set; } = string.Empty;
    public string Amount { get; set; } = string.Empty;
    public string ImportDate { get; set; } = string.Empty;
    public DateTime ProcessedAt { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class EtlDbContext(string connectionString) : DbContext
{
    public DbSet<RawImport> RawImports => Set<RawImport>();
    public DbSet<FinalRecord> FinalRecords => Set<FinalRecord>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse(connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Raw import table: stores incoming data before processing
        modelBuilder.Entity<RawImport>(entity =>
        {
            entity.ToTable("raw_imports");
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.RowId });

            entity.Property(e => e.CategoryCode).HasLowCardinality();
            entity.Property(e => e.RegionCode).HasLowCardinality();
            entity.Property(e => e.Status).HasLowCardinality();
        });

        // Lookup mapping: used in temp tables, needs entity registration
        // for CreateTempTableAsync<LookupMapping>() to work
        modelBuilder.Entity<LookupMapping>(entity =>
        {
            entity.ToTable("_lookup_mappings_schema");
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.Code });
        });

        // Final records: destination for processed data
        modelBuilder.Entity<FinalRecord>(entity =>
        {
            entity.ToTable("final_records");
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.ProcessedAt, x.ExternalId });
            entity.HasPartitionByMonth(x => x.ProcessedAt);

            entity.Property(e => e.CategoryCode).HasLowCardinality();
            entity.Property(e => e.RegionCode).HasLowCardinality();
        });
    }
}
