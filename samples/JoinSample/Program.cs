using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;

// Start ClickHouse container
var container = new ClickHouseBuilder()
    .WithImage("clickhouse/clickhouse-server:25.6")
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

    await context.Database.EnsureDeletedAsync();
    await context.Database.EnsureCreatedAsync();

    context.Events.AddRange(
        new Event { Id = 1, Timestamp = new DateTime(2024, 1, 1, 9, 30, 0), Symbol = "AAPL", Name = "Trade1", Tags = ["critical", "urgent"], Scores = [10, 20] },
        new Event { Id = 2, Timestamp = new DateTime(2024, 1, 1, 10, 30, 0), Symbol = "AAPL", Name = "Trade2", Tags = ["info"], Scores = [5] },
        new Event { Id = 3, Timestamp = new DateTime(2024, 1, 1, 9, 30, 0), Symbol = "GOOG", Name = "Trade3", Tags = ["critical", "debug"], Scores = [30, 40] },
        new Event { Id = 4, Timestamp = new DateTime(2024, 1, 1, 11, 0, 0), Symbol = "GOOG", Name = "Trade4", Tags = [], Scores = [] });
    context.Prices.AddRange(
        new Price { Timestamp = new DateTime(2024, 1, 1, 9, 0, 0), Symbol = "AAPL", Value = 150.0000m, Volume = 1000 },
        new Price { Timestamp = new DateTime(2024, 1, 1, 10, 0, 0), Symbol = "AAPL", Value = 152.5000m, Volume = 2000 },
        new Price { Timestamp = new DateTime(2024, 1, 1, 11, 0, 0), Symbol = "AAPL", Value = 151.0000m, Volume = 1500 },
        new Price { Timestamp = new DateTime(2024, 1, 1, 9, 0, 0), Symbol = "GOOG", Value = 2800.0000m, Volume = 500 },
        new Price { Timestamp = new DateTime(2024, 1, 1, 10, 0, 0), Symbol = "GOOG", Value = 2820.0000m, Volume = 800 });
    await context.SaveChangesAsync();

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
            entity.UseReplacingMergeTree(x => new { x.Symbol, x.Timestamp });
        });

        modelBuilder.Entity<Price>(entity =>
        {
            entity.HasKey(e => new { e.Symbol, e.Timestamp });
            entity.ToTable("Prices");
            entity.Property(e => e.Value).HasColumnName("Price");
            entity.UseReplacingMergeTree(x => new { x.Symbol, x.Timestamp });
        });
    }
}
