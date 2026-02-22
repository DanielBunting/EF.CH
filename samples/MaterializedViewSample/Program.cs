// MaterializedViewSample - Demonstrates materialized views via EF.CH
//
// 1. Basic MV         - AsMaterializedViewRaw with GroupBy aggregation
// 2. Raw SQL MV       - AsMaterializedViewRaw with custom aggregation logic
// 3. Null engine      - UseNullEngine as MV source (data discarded after MV processes)
// 4. Populate option  - Backfill from existing data when creating the view

using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;

var container = new ClickHouseBuilder()
    .WithImage("clickhouse/clickhouse-server:latest")
    .Build();

Console.WriteLine("Starting ClickHouse container...");
await container.StartAsync();
Console.WriteLine("ClickHouse container started.\n");

try
{
    var connectionString = container.GetConnectionString();

    Console.WriteLine("=== EF.CH Materialized View Sample ===");
    Console.WriteLine();

    await DemoBasicMaterializedView(connectionString);
    await DemoRawSqlMaterializedView(connectionString);
    await DemoNullEngineSource(connectionString);
    await DemoPopulateOption(connectionString);

    Console.WriteLine("=== All materialized view demos complete ===");
}
finally
{
    Console.WriteLine("\nStopping container...");
    await container.DisposeAsync();
    Console.WriteLine("Done.");
}

// ---------------------------------------------------------------------------
// 1. Basic Materialized View
// ---------------------------------------------------------------------------
static async Task DemoBasicMaterializedView(string connectionString)
{
    Console.WriteLine("--- 1. Basic Materialized View ---");
    Console.WriteLine("AsMaterializedViewRaw with GroupBy aggregation for daily order summaries.");
    Console.WriteLine("EnsureCreatedAsync generates the source table, target table, and view.");
    Console.WriteLine();

    await using var context = new LinqMvContext(connectionString);

    // EnsureCreatedAsync generates:
    //   CREATE TABLE LinqOrders (...) ENGINE = MergeTree() ORDER BY (OrderDate, OrderId)
    //   CREATE TABLE LinqDailySummary (...) ENGINE = SummingMergeTree() ORDER BY (Date, ProductId)
    //   CREATE MATERIALIZED VIEW ... TO LinqDailySummary AS SELECT ... FROM LinqOrders GROUP BY ...
    await context.Database.EnsureDeletedAsync();
    await context.Database.EnsureCreatedAsync();

    // Insert orders -- the materialized view processes them automatically
    var orders = new List<LinqOrder>
    {
        new() { OrderId = 1, ProductId = 101, Quantity = 2, Revenue = 29.99, OrderDate = new DateTime(2025, 1, 15, 10, 0, 0, DateTimeKind.Utc) },
        new() { OrderId = 2, ProductId = 102, Quantity = 1, Revenue = 49.99, OrderDate = new DateTime(2025, 1, 15, 11, 0, 0, DateTimeKind.Utc) },
        new() { OrderId = 3, ProductId = 101, Quantity = 3, Revenue = 44.97, OrderDate = new DateTime(2025, 1, 15, 14, 0, 0, DateTimeKind.Utc) },
        new() { OrderId = 4, ProductId = 103, Quantity = 1, Revenue = 99.99, OrderDate = new DateTime(2025, 1, 15, 15, 0, 0, DateTimeKind.Utc) },
        new() { OrderId = 5, ProductId = 101, Quantity = 1, Revenue = 14.99, OrderDate = new DateTime(2025, 1, 16, 9, 0, 0, DateTimeKind.Utc) },
        new() { OrderId = 6, ProductId = 102, Quantity = 2, Revenue = 99.98, OrderDate = new DateTime(2025, 1, 16, 10, 0, 0, DateTimeKind.Utc) },
        new() { OrderId = 7, ProductId = 103, Quantity = 5, Revenue = 499.95, OrderDate = new DateTime(2025, 1, 16, 11, 0, 0, DateTimeKind.Utc) },
        new() { OrderId = 8, ProductId = 101, Quantity = 2, Revenue = 29.98, OrderDate = new DateTime(2025, 1, 16, 14, 0, 0, DateTimeKind.Utc) },
    };

    await context.BulkInsertAsync(orders);
    Console.WriteLine("Inserted 8 orders into LinqOrders.");
    Console.WriteLine("Materialized view automatically aggregates to daily summaries.");

    // Force merge to see summed results
    await context.Database.OptimizeTableAsync("LinqDailySummary", o => o.WithFinal());
    await Task.Delay(500);

    // Query the materialized view target
    var summaries = await context.DailySummaries
        .OrderBy(s => s.Date)
        .ThenBy(s => s.ProductId)
        .ToListAsync();

    Console.WriteLine("Daily summaries (from materialized view):");
    foreach (var s in summaries)
    {
        Console.WriteLine($"  Date={s.Date:yyyy-MM-dd}, ProductId={s.ProductId}, " +
                          $"Qty={s.TotalQuantity}, Revenue={s.TotalRevenue:F2}, Orders={s.OrderCount}");
    }

    await context.Database.EnsureDeletedAsync();
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 2. Raw SQL Materialized View
// ---------------------------------------------------------------------------
static async Task DemoRawSqlMaterializedView(string connectionString)
{
    Console.WriteLine("--- 2. Raw SQL Materialized View ---");
    Console.WriteLine("AsMaterializedViewRaw for full control over the SELECT transformation.");
    Console.WriteLine();

    await using var context = new RawMvContext(connectionString);

    // EnsureCreatedAsync generates all tables and the materialized view from the model
    await context.Database.EnsureDeletedAsync();
    await context.Database.EnsureCreatedAsync();

    // Insert access log entries
    var logs = new List<AccessLog>
    {
        new() { RequestId = 1, Path = "/api/users", StatusCode = 200, ResponseTimeMs = 45, RequestedAt = new DateTime(2025, 1, 15, 10, 5, 0, DateTimeKind.Utc) },
        new() { RequestId = 2, Path = "/api/users", StatusCode = 200, ResponseTimeMs = 52, RequestedAt = new DateTime(2025, 1, 15, 10, 10, 0, DateTimeKind.Utc) },
        new() { RequestId = 3, Path = "/api/orders", StatusCode = 500, ResponseTimeMs = 2500, RequestedAt = new DateTime(2025, 1, 15, 10, 15, 0, DateTimeKind.Utc) },
        new() { RequestId = 4, Path = "/api/users", StatusCode = 200, ResponseTimeMs = 38, RequestedAt = new DateTime(2025, 1, 15, 10, 20, 0, DateTimeKind.Utc) },
        new() { RequestId = 5, Path = "/api/orders", StatusCode = 200, ResponseTimeMs = 120, RequestedAt = new DateTime(2025, 1, 15, 10, 25, 0, DateTimeKind.Utc) },
        new() { RequestId = 6, Path = "/api/users", StatusCode = 503, ResponseTimeMs = 5000, RequestedAt = new DateTime(2025, 1, 15, 11, 5, 0, DateTimeKind.Utc) },
        new() { RequestId = 7, Path = "/api/orders", StatusCode = 200, ResponseTimeMs = 95, RequestedAt = new DateTime(2025, 1, 15, 11, 10, 0, DateTimeKind.Utc) },
        new() { RequestId = 8, Path = "/api/users", StatusCode = 200, ResponseTimeMs = 42, RequestedAt = new DateTime(2025, 1, 15, 11, 15, 0, DateTimeKind.Utc) },
        new() { RequestId = 9, Path = "/api/orders", StatusCode = 200, ResponseTimeMs = 88, RequestedAt = new DateTime(2025, 1, 15, 11, 20, 0, DateTimeKind.Utc) },
        new() { RequestId = 10, Path = "/api/users", StatusCode = 200, ResponseTimeMs = 55, RequestedAt = new DateTime(2025, 1, 15, 11, 25, 0, DateTimeKind.Utc) },
    };

    await context.BulkInsertAsync(logs);
    Console.WriteLine("Inserted 10 access log entries.");

    await context.Database.OptimizeTableAsync("HourlySummary", o => o.WithFinal());
    await Task.Delay(500);

    var summaries = await context.HourlySummaries
        .OrderBy(s => s.Hour)
        .ThenBy(s => s.Path)
        .ToListAsync();

    Console.WriteLine("Hourly summaries (from raw SQL materialized view):");
    foreach (var s in summaries)
    {
        Console.WriteLine($"  Hour={s.Hour:HH:mm}, Path={s.Path}, " +
                          $"Requests={s.RequestCount}, AvgMs={s.AvgResponseMs:F0}, Errors={s.ErrorCount}");
    }

    await context.Database.EnsureDeletedAsync();
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 3. Null Engine Source
// ---------------------------------------------------------------------------
static async Task DemoNullEngineSource(string connectionString)
{
    Console.WriteLine("--- 3. Null Engine Source ---");
    Console.WriteLine("Source table uses Null engine: data is discarded after the MV processes it.");
    Console.WriteLine("Only the aggregated result in the MV target is kept.");
    Console.WriteLine();

    await using var context = new NullEngineMvContext(connectionString);

    // EnsureCreatedAsync generates:
    //   CREATE TABLE RawMetrics (...) ENGINE = Null
    //   CREATE TABLE MetricsSummary (...) ENGINE = SummingMergeTree() ORDER BY (MetricName, MinuteSlot)
    //   CREATE MATERIALIZED VIEW ... TO MetricsSummary AS SELECT ... FROM RawMetrics GROUP BY ...
    await context.Database.EnsureDeletedAsync();
    await context.Database.EnsureCreatedAsync();

    // Insert raw metrics -- they flow through the Null engine to the MV
    var metrics = new List<RawMetric>
    {
        new() { MetricName = "cpu_usage", Value = 72.5, Timestamp = new DateTime(2025, 1, 15, 10, 0, 5, DateTimeKind.Utc) },
        new() { MetricName = "cpu_usage", Value = 68.3, Timestamp = new DateTime(2025, 1, 15, 10, 0, 15, DateTimeKind.Utc) },
        new() { MetricName = "cpu_usage", Value = 75.1, Timestamp = new DateTime(2025, 1, 15, 10, 0, 25, DateTimeKind.Utc) },
        new() { MetricName = "memory_mb", Value = 4096, Timestamp = new DateTime(2025, 1, 15, 10, 0, 10, DateTimeKind.Utc) },
        new() { MetricName = "memory_mb", Value = 4200, Timestamp = new DateTime(2025, 1, 15, 10, 0, 20, DateTimeKind.Utc) },
        new() { MetricName = "cpu_usage", Value = 80.2, Timestamp = new DateTime(2025, 1, 15, 10, 1, 5, DateTimeKind.Utc) },
        new() { MetricName = "cpu_usage", Value = 82.0, Timestamp = new DateTime(2025, 1, 15, 10, 1, 15, DateTimeKind.Utc) },
        new() { MetricName = "memory_mb", Value = 4150, Timestamp = new DateTime(2025, 1, 15, 10, 1, 10, DateTimeKind.Utc) },
    };

    await context.BulkInsertAsync(metrics);
    Console.WriteLine("Inserted 8 raw metrics into Null engine table.");

    // Verify: the source table has NO data (Null engine discards it)
    var sourceCount = await context.RawMetrics.CountAsync();
    Console.WriteLine($"Rows in RawMetrics (Null engine): {sourceCount}");

    // But the materialized view target has the aggregated data
    await context.Database.OptimizeTableAsync("MetricsSummary", o => o.WithFinal());
    await Task.Delay(500);

    var summaries = await context.MetricsSummaries
        .OrderBy(s => s.MetricName)
        .ThenBy(s => s.MinuteSlot)
        .ToListAsync();

    Console.WriteLine("Metric summaries (data preserved only in MV target):");
    foreach (var s in summaries)
    {
        Console.WriteLine($"  {s.MetricName} @ {s.MinuteSlot:HH:mm}: " +
                          $"Sum={s.ValueSum:F1}, Count={s.ValueCount}");
    }

    await context.Database.EnsureDeletedAsync();
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 4. Populate Option
// ---------------------------------------------------------------------------
static async Task DemoPopulateOption(string connectionString)
{
    Console.WriteLine("--- 4. Populate Option ---");
    Console.WriteLine("POPULATE backfills the MV from existing source data at creation time.");
    Console.WriteLine();

    await using var context = new PopulateMvContext(connectionString);

    // First create the source table only
    await context.Database.EnsureDeletedAsync();
    await context.Database.EnsureCreatedAsync();

    // Insert data BEFORE the view would normally process it.
    // The POPULATE option on the MV means it will backfill when created.
    var orders = new List<PopulateOrder>
    {
        new() { OrderId = 1, ProductId = 101, Amount = 29.99, OrderDate = new DateTime(2025, 1, 15, 10, 0, 0, DateTimeKind.Utc) },
        new() { OrderId = 2, ProductId = 102, Amount = 49.99, OrderDate = new DateTime(2025, 1, 15, 11, 0, 0, DateTimeKind.Utc) },
        new() { OrderId = 3, ProductId = 101, Amount = 19.99, OrderDate = new DateTime(2025, 1, 15, 14, 0, 0, DateTimeKind.Utc) },
        new() { OrderId = 4, ProductId = 103, Amount = 99.99, OrderDate = new DateTime(2025, 1, 15, 15, 0, 0, DateTimeKind.Utc) },
        new() { OrderId = 5, ProductId = 101, Amount = 39.99, OrderDate = new DateTime(2025, 1, 16, 9, 0, 0, DateTimeKind.Utc) },
        new() { OrderId = 6, ProductId = 102, Amount = 59.99, OrderDate = new DateTime(2025, 1, 16, 10, 0, 0, DateTimeKind.Utc) },
    };

    await context.BulkInsertAsync(orders);
    Console.WriteLine("Inserted 6 orders BEFORE materialized view processes them.");
    Console.WriteLine("The MV was created with populate: true, so it backfills from existing data.");

    await context.Database.OptimizeTableAsync("PopulateSummary", o => o.WithFinal());
    await Task.Delay(500);

    var summaries = await context.PopulateSummaries
        .OrderBy(s => s.Date)
        .ToListAsync();

    Console.WriteLine("Summaries (backfilled from pre-existing data):");
    foreach (var s in summaries)
    {
        Console.WriteLine($"  Date={s.Date:yyyy-MM-dd}, TotalAmount={s.TotalAmount:F2}, Orders={s.OrderCount}");
    }

    // Now insert new data -- the MV processes it automatically going forward
    var newOrders = new List<PopulateOrder>
    {
        new() { OrderId = 7, ProductId = 101, Amount = 74.99, OrderDate = new DateTime(2025, 1, 17, 10, 0, 0, DateTimeKind.Utc) },
        new() { OrderId = 8, ProductId = 102, Amount = 24.99, OrderDate = new DateTime(2025, 1, 17, 11, 0, 0, DateTimeKind.Utc) },
    };

    await context.BulkInsertAsync(newOrders);
    Console.WriteLine("Inserted 2 more orders after MV creation.");

    await context.Database.OptimizeTableAsync("PopulateSummary", o => o.WithFinal());
    await Task.Delay(500);

    var updatedSummaries = await context.PopulateSummaries
        .OrderBy(s => s.Date)
        .ToListAsync();

    Console.WriteLine("Updated summaries (includes both backfilled and new data):");
    foreach (var s in updatedSummaries)
    {
        Console.WriteLine($"  Date={s.Date:yyyy-MM-dd}, TotalAmount={s.TotalAmount:F2}, Orders={s.OrderCount}");
    }

    await context.Database.EnsureDeletedAsync();
    Console.WriteLine();
}

// ===========================================================================
// Entity classes and DbContext classes
// ===========================================================================

// --- LINQ MV ---

public class LinqOrder
{
    public ulong OrderId { get; set; }
    public uint ProductId { get; set; }
    public uint Quantity { get; set; }
    public double Revenue { get; set; }
    public DateTime OrderDate { get; set; }
}

public class LinqDailySummary
{
    public DateTime Date { get; set; }
    public uint ProductId { get; set; }
    public ulong TotalQuantity { get; set; }
    public double TotalRevenue { get; set; }
    public ulong OrderCount { get; set; }
}

public class LinqMvContext(string connectionString) : DbContext
{
    public DbSet<LinqOrder> Orders => Set<LinqOrder>();
    public DbSet<LinqDailySummary> DailySummaries => Set<LinqDailySummary>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Source table
        modelBuilder.Entity<LinqOrder>(entity =>
        {
            entity.ToTable("LinqOrders");
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.OrderDate, x.OrderId });
        });

        // Target table with materialized view for daily order aggregation
        modelBuilder.Entity<LinqDailySummary>(entity =>
        {
            entity.ToTable("LinqDailySummary");
            entity.HasNoKey();
            entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });

            // MV aggregates orders into daily summaries per product
            entity.AsMaterializedViewRaw(
                sourceTable: "LinqOrders",
                selectSql: @"SELECT
                    toDate(""OrderDate"") AS ""Date"",
                    ""ProductId"",
                    sum(""Quantity"") AS ""TotalQuantity"",
                    sum(""Revenue"") AS ""TotalRevenue"",
                    count() AS ""OrderCount""
                FROM ""LinqOrders""
                GROUP BY ""Date"", ""ProductId""",
                populate: false);
        });
    }
}

// --- Raw SQL MV ---

public class AccessLog
{
    public ulong RequestId { get; set; }
    public string Path { get; set; } = "";
    public ushort StatusCode { get; set; }
    public uint ResponseTimeMs { get; set; }
    public DateTime RequestedAt { get; set; }
}

public class HourlySummary
{
    public DateTime Hour { get; set; }
    public string Path { get; set; } = "";
    public ulong RequestCount { get; set; }
    public double AvgResponseMs { get; set; }
    public ulong ErrorCount { get; set; }
}

public class RawMvContext(string connectionString) : DbContext
{
    public DbSet<AccessLog> AccessLogs => Set<AccessLog>();
    public DbSet<HourlySummary> HourlySummaries => Set<HourlySummary>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Source table
        modelBuilder.Entity<AccessLog>(entity =>
        {
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.RequestedAt, x.RequestId });
        });

        // Target table with raw SQL materialized view for custom aggregation logic
        modelBuilder.Entity<HourlySummary>(entity =>
        {
            entity.ToTable("HourlySummary");
            entity.HasNoKey();
            entity.UseSummingMergeTree(x => new { x.Hour, x.Path });

            // Raw SQL MV: full control over the SELECT transformation
            entity.AsMaterializedViewRaw(
                sourceTable: "AccessLogs",
                selectSql: @"SELECT
                    toStartOfHour(RequestedAt) AS Hour,
                    Path,
                    count() AS RequestCount,
                    avg(ResponseTimeMs) AS AvgResponseMs,
                    countIf(StatusCode >= 500) AS ErrorCount
                FROM AccessLogs
                GROUP BY Hour, Path");
        });
    }
}

// --- Null Engine MV ---

public class RawMetric
{
    public string MetricName { get; set; } = "";
    public double Value { get; set; }
    public DateTime Timestamp { get; set; }
}

public class MetricsSummary
{
    public string MetricName { get; set; } = "";
    public DateTime MinuteSlot { get; set; }
    public double ValueSum { get; set; }
    public ulong ValueCount { get; set; }
}

public class NullEngineMvContext(string connectionString) : DbContext
{
    public DbSet<RawMetric> RawMetrics => Set<RawMetric>();
    public DbSet<MetricsSummary> MetricsSummaries => Set<MetricsSummary>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Source table: Null engine -- raw data is discarded after insert
        modelBuilder.Entity<RawMetric>(entity =>
        {
            entity.HasNoKey();
            entity.UseNullEngine();
        });

        // Target table: SummingMergeTree to store aggregated metrics
        modelBuilder.Entity<MetricsSummary>(entity =>
        {
            entity.ToTable("MetricsSummary");
            entity.HasNoKey();
            entity.UseSummingMergeTree(x => new { x.MetricName, x.MinuteSlot });

            entity.AsMaterializedViewRaw(
                sourceTable: "RawMetrics",
                selectSql: @"SELECT
                    MetricName,
                    toStartOfMinute(Timestamp) AS MinuteSlot,
                    sum(Value) AS ValueSum,
                    count() AS ValueCount
                FROM RawMetrics
                GROUP BY MetricName, MinuteSlot");
        });
    }
}

// --- Populate MV ---

public class PopulateOrder
{
    public ulong OrderId { get; set; }
    public uint ProductId { get; set; }
    public double Amount { get; set; }
    public DateTime OrderDate { get; set; }
}

public class PopulateSummary
{
    public DateTime Date { get; set; }
    public double TotalAmount { get; set; }
    public ulong OrderCount { get; set; }
}

public class PopulateMvContext(string connectionString) : DbContext
{
    public DbSet<PopulateOrder> Orders => Set<PopulateOrder>();
    public DbSet<PopulateSummary> PopulateSummaries => Set<PopulateSummary>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Source table
        modelBuilder.Entity<PopulateOrder>(entity =>
        {
            entity.ToTable("PopulateOrders");
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.OrderDate, x.OrderId });
        });

        // Target table with populate: true to backfill from existing data
        modelBuilder.Entity<PopulateSummary>(entity =>
        {
            entity.ToTable("PopulateSummary");
            entity.HasNoKey();
            entity.UseSummingMergeTree(x => new { x.Date });

            entity.AsMaterializedViewRaw(
                sourceTable: "PopulateOrders",
                selectSql: @"SELECT
                    toDate(OrderDate) AS Date,
                    sum(Amount) AS TotalAmount,
                    count() AS OrderCount
                FROM PopulateOrders
                GROUP BY Date",
                populate: true);
        });
    }
}
