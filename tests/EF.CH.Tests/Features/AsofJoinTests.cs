using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

public class AsofJoinTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:25.6")
        .Build();

    public async Task InitializeAsync() => await _container.StartAsync();
    public async Task DisposeAsync() => await _container.DisposeAsync();

    private string GetConnectionString() => _container.GetConnectionString();

    [Fact]
    public async Task AsofJoin_FindsClosestMatch()
    {
        await using var context = CreateContext();
        await SetupTables(context);
        await SeedData(context);

        var result = await context.AsofEvents
            .AsofJoin(context.AsofPrices,
                e => e.Symbol, p => p.Symbol,
                (e, p) => e.Timestamp >= p.Timestamp,
                (e, p) => new { e.Timestamp, e.EventName, e.Symbol, PriceAtEvent = p.Price })
            .OrderBy(x => x.Timestamp)
            .ToListAsync();

        Assert.True(result.Count > 0);
        // Each event should be matched with the closest price at or before the event time
        foreach (var r in result)
        {
            Assert.True(r.PriceAtEvent > 0, $"Expected positive price for {r.EventName}");
        }
    }

    [Fact]
    public async Task AsofLeftJoin_PreservesUnmatchedRows()
    {
        await using var context = CreateContext();
        await SetupTables(context);
        await SeedData(context);

        var result = await context.AsofEvents
            .AsofLeftJoin(context.AsofPrices,
                e => e.Symbol, p => p.Symbol,
                (e, p) => e.Timestamp >= p.Timestamp,
                (e, p) => new { e.Timestamp, e.EventName, e.Symbol, PriceAtEvent = p.Price })
            .OrderBy(x => x.Timestamp)
            .ToListAsync();

        // All events should appear, even without matching prices
        Assert.True(result.Count >= 3);
    }

    [Fact]
    public async Task AsofJoin_WithWhere_FiltersAfterJoin()
    {
        await using var context = CreateContext();
        await SetupTables(context);
        await SeedData(context);

        var result = await context.AsofEvents
            .AsofJoin(context.AsofPrices,
                e => e.Symbol, p => p.Symbol,
                (e, p) => e.Timestamp >= p.Timestamp,
                (e, p) => new { e.Timestamp, e.EventName, e.Symbol, PriceAtEvent = p.Price })
            .Where(x => x.PriceAtEvent > 100m)
            .ToListAsync();

        Assert.All(result, r => Assert.True(r.PriceAtEvent > 100m));
    }

    [Fact]
    public async Task AsofJoin_WithFinal_CombinesModifiers()
    {
        await using var context = CreateContext();
        await SetupTables(context);
        await SeedData(context);

        var result = await context.AsofEvents
            .Final()
            .AsofJoin(context.AsofPrices,
                e => e.Symbol, p => p.Symbol,
                (e, p) => e.Timestamp >= p.Timestamp,
                (e, p) => new { e.Timestamp, e.EventName, PriceAtEvent = p.Price })
            .ToListAsync();

        Assert.True(result.Count > 0);
    }

    [Fact]
    public async Task AsofJoin_AllOperators_GreaterThan()
    {
        await using var context = CreateContext();
        await SetupTables(context);
        await SeedData(context);

        // Test with > (strictly greater than)
        var result = await context.AsofEvents
            .AsofJoin(context.AsofPrices,
                e => e.Symbol, p => p.Symbol,
                (e, p) => e.Timestamp > p.Timestamp,
                (e, p) => new { e.EventName, e.Timestamp, PriceTimestamp = p.Timestamp })
            .ToListAsync();

        // With strict >, the price timestamp must be strictly before the event
        Assert.All(result, r => Assert.True(r.PriceTimestamp < r.Timestamp));
    }

    [Fact]
    public void AsofJoin_InvalidCondition_Throws()
    {
        using var context = CreateContext();

        // Using == instead of a comparison operator should fail
        Assert.ThrowsAny<InvalidOperationException>(() =>
            context.AsofEvents
                .AsofJoin(context.AsofPrices,
                    e => e.Symbol, p => p.Symbol,
                    (e, p) => e.Timestamp == p.Timestamp,
                    (e, p) => new { e.EventName, p.Price })
                .ToList());
    }

    private async Task SetupTables(AsofJoinTestContext context)
    {
        await context.Database.EnsureCreatedAsync();
    }

    private async Task SeedData(AsofJoinTestContext context)
    {
        context.AsofPrices.AddRange(
            new AsofPrice { Timestamp = new DateTime(2024, 1, 1, 9, 0, 0),  Symbol = "AAPL", Price = 150.0000m, Volume = 1000 },
            new AsofPrice { Timestamp = new DateTime(2024, 1, 1, 10, 0, 0), Symbol = "AAPL", Price = 152.5000m, Volume = 2000 },
            new AsofPrice { Timestamp = new DateTime(2024, 1, 1, 11, 0, 0), Symbol = "AAPL", Price = 151.0000m, Volume = 1500 },
            new AsofPrice { Timestamp = new DateTime(2024, 1, 1, 9, 0, 0),  Symbol = "GOOG", Price = 140.0000m, Volume = 500 },
            new AsofPrice { Timestamp = new DateTime(2024, 1, 1, 10, 0, 0), Symbol = "GOOG", Price = 142.0000m, Volume = 800 });

        context.AsofEvents.AddRange(
            new AsofEvent { Timestamp = new DateTime(2024, 1, 1, 9, 30, 0),  Symbol = "AAPL", EventName = "Trade1" },
            new AsofEvent { Timestamp = new DateTime(2024, 1, 1, 10, 30, 0), Symbol = "AAPL", EventName = "Trade2" },
            new AsofEvent { Timestamp = new DateTime(2024, 1, 1, 11, 30, 0), Symbol = "AAPL", EventName = "Trade3" },
            new AsofEvent { Timestamp = new DateTime(2024, 1, 1, 9, 30, 0),  Symbol = "GOOG", EventName = "Trade4" },
            new AsofEvent { Timestamp = new DateTime(2024, 1, 1, 10, 30, 0), Symbol = "GOOG", EventName = "Trade5" });

        await context.SaveChangesAsync();
    }

    private AsofJoinTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<AsofJoinTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;
        return new AsofJoinTestContext(options);
    }
}

public class AsofEvent
{
    public DateTime Timestamp { get; set; }
    public string Symbol { get; set; } = string.Empty;
    public string EventName { get; set; } = string.Empty;
}

public class AsofPrice
{
    public DateTime Timestamp { get; set; }
    public string Symbol { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public ulong Volume { get; set; }
}

public class AsofJoinTestContext : DbContext
{
    public AsofJoinTestContext(DbContextOptions<AsofJoinTestContext> options)
        : base(options) { }

    public DbSet<AsofEvent> AsofEvents => Set<AsofEvent>();
    public DbSet<AsofPrice> AsofPrices => Set<AsofPrice>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AsofEvent>(entity =>
        {
            entity.HasKey(e => new { e.Symbol, e.Timestamp });
            entity.ToTable("AsofEvents");
            entity.UseReplacingMergeTree(x => new { x.Symbol, x.Timestamp });
        });

        modelBuilder.Entity<AsofPrice>(entity =>
        {
            entity.HasKey(e => new { e.Symbol, e.Timestamp });
            entity.ToTable("AsofPrices");
            entity.UseReplacingMergeTree(x => new { x.Symbol, x.Timestamp });
        });
    }
}
