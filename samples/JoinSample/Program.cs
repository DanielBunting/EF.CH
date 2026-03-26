using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;

// Start ClickHouse container
var container = new ClickHouseBuilder()
    .WithImage("clickhouse/clickhouse-server:latest")
    .Build();

Console.WriteLine("Starting ClickHouse container...");
await container.StartAsync();

try
{
    var connectionString = container.GetConnectionString();
    var options = new DbContextOptionsBuilder<AnalyticsContext>()
        .UseClickHouse(connectionString)
        .Options;

    await using var context = new AnalyticsContext(options);

    // Create tables
    await context.Database.ExecuteSqlRawAsync(@"
        CREATE TABLE IF NOT EXISTS ""Events"" (
            ""Id"" UInt32,
            ""Timestamp"" DateTime,
            ""Symbol"" String,
            ""Name"" String,
            ""Tags"" Array(String),
            ""Scores"" Array(Int32)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (""Symbol"", ""Timestamp"")
    ");

    await context.Database.ExecuteSqlRawAsync(@"
        CREATE TABLE IF NOT EXISTS ""Prices"" (
            ""Timestamp"" DateTime,
            ""Symbol"" String,
            ""Price"" Decimal64(4),
            ""Volume"" UInt64
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (""Symbol"", ""Timestamp"")
    ");

    // Seed data
    await context.Database.ExecuteSqlRawAsync(@"
        INSERT INTO ""Events"" VALUES
        (1, '2024-01-01 09:30:00', 'AAPL', 'Trade1', ['critical','urgent'], [10,20]),
        (2, '2024-01-01 10:30:00', 'AAPL', 'Trade2', ['info'], [5]),
        (3, '2024-01-01 09:30:00', 'GOOG', 'Trade3', ['critical','debug'], [30,40]),
        (4, '2024-01-01 11:00:00', 'GOOG', 'Trade4', [], [])
    ");

    await context.Database.ExecuteSqlRawAsync(@"
        INSERT INTO ""Prices"" VALUES
        ('2024-01-01 09:00:00', 'AAPL', 150.0000, 1000),
        ('2024-01-01 10:00:00', 'AAPL', 152.5000, 2000),
        ('2024-01-01 11:00:00', 'AAPL', 151.0000, 1500),
        ('2024-01-01 09:00:00', 'GOOG', 2800.0000, 500),
        ('2024-01-01 10:00:00', 'GOOG', 2820.0000, 800)
    ");

    // ===============================
    // 1. ARRAY JOIN - Explode tags
    // ===============================
    Console.WriteLine("\n=== 1. ARRAY JOIN: Explode tags ===");
    var taggedEvents = await context.Events
        .ArrayJoin(e => e.Tags, (e, tag) => new { e.Id, e.Name, Tag = tag })
        .OrderBy(x => x.Id)
        .ToListAsync();

    foreach (var r in taggedEvents)
        Console.WriteLine($"  Id={r.Id}, Name={r.Name}, Tag={r.Tag}");

    // ===============================
    // 2. LEFT ARRAY JOIN - Preserve empty
    // ===============================
    Console.WriteLine("\n=== 2. LEFT ARRAY JOIN: Preserve empty arrays ===");
    var allTags = await context.Events
        .LeftArrayJoin(e => e.Tags, (e, tag) => new { e.Id, e.Name, Tag = tag })
        .OrderBy(x => x.Id)
        .ToListAsync();

    foreach (var r in allTags)
        Console.WriteLine($"  Id={r.Id}, Name={r.Name}, Tag='{r.Tag}'");

    // ===============================
    // 3. ARRAY JOIN + WHERE
    // ===============================
    Console.WriteLine("\n=== 3. ARRAY JOIN + WHERE: Critical tags only ===");
    var criticalEvents = await context.Events
        .ArrayJoin(e => e.Tags, (e, tag) => new { e.Id, e.Name, Tag = tag })
        .Where(x => x.Tag == "critical")
        .ToListAsync();

    foreach (var r in criticalEvents)
        Console.WriteLine($"  Id={r.Id}, Name={r.Name}, Tag={r.Tag}");

    // ===============================
    // 4. Multiple ARRAY JOINs
    // ===============================
    Console.WriteLine("\n=== 4. Multiple ARRAY JOINs: Tags + Scores ===");
    var tagScores = await context.Events
        .ArrayJoin(e => e.Tags, e => e.Scores,
            (e, tag, score) => new { e.Id, Tag = tag, Score = score })
        .OrderBy(x => x.Id)
        .ToListAsync();

    foreach (var r in tagScores)
        Console.WriteLine($"  Id={r.Id}, Tag={r.Tag}, Score={r.Score}");

    // ===============================
    // 5. ASOF JOIN - Closest price
    // ===============================
    Console.WriteLine("\n=== 5. ASOF JOIN: Match events to closest price ===");
    var enriched = await context.Events
        .AsofJoin(context.Prices,
            e => e.Symbol, p => p.Symbol,
            (e, p) => e.Timestamp >= p.Timestamp,
            (e, p) => new { e.Name, e.Symbol, e.Timestamp, PriceAtEvent = p.Value })
        .OrderBy(x => x.Timestamp)
        .ToListAsync();

    foreach (var r in enriched)
        Console.WriteLine($"  {r.Name} ({r.Symbol} @ {r.Timestamp:HH:mm}): Price={r.PriceAtEvent}");

    // ===============================
    // 6. ASOF LEFT JOIN
    // ===============================
    Console.WriteLine("\n=== 6. ASOF LEFT JOIN: Keep all events ===");
    var allEnriched = await context.Events
        .AsofLeftJoin(context.Prices,
            e => e.Symbol, p => p.Symbol,
            (e, p) => e.Timestamp >= p.Timestamp,
            (e, p) => new { e.Name, e.Symbol, Price = p.Value })
        .OrderBy(x => x.Name)
        .ToListAsync();

    foreach (var r in allEnriched)
        Console.WriteLine($"  {r.Name} ({r.Symbol}): Price={r.Price}");

    // ===============================
    // 7. ARRAY JOIN + FINAL
    // ===============================
    Console.WriteLine("\n=== 7. ARRAY JOIN + FINAL ===");
    var finalTags = await context.Events
        .Final()
        .ArrayJoin(e => e.Tags, (e, tag) => new { e.Id, Tag = tag })
        .ToListAsync();

    Console.WriteLine($"  {finalTags.Count} rows after FINAL + ARRAY JOIN");

    Console.WriteLine("\nAll samples completed successfully!");
}
finally
{
    await container.DisposeAsync();
}

// ============================
// Entity Classes
// ============================

public class Event
{
    public uint Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Symbol { get; set; } = "";
    public string Name { get; set; } = "";
    public string[] Tags { get; set; } = [];
    public int[] Scores { get; set; } = [];
}

public class Price
{
    public DateTime Timestamp { get; set; }
    public string Symbol { get; set; } = "";
    public decimal Value { get; set; }
    public ulong Volume { get; set; }
}

public class AnalyticsContext : DbContext
{
    public AnalyticsContext(DbContextOptions<AnalyticsContext> options) : base(options) { }

    public DbSet<Event> Events => Set<Event>();
    public DbSet<Price> Prices => Set<Price>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("Events");
            entity.UseMergeTree(x => new { x.Symbol, x.Timestamp });
        });

        modelBuilder.Entity<Price>(entity =>
        {
            entity.HasKey(e => new { e.Symbol, e.Timestamp });
            entity.ToTable("Prices");
            entity.Property(e => e.Value).HasColumnName("Price");
            entity.UseMergeTree(x => new { x.Symbol, x.Timestamp });
        });
    }
}
