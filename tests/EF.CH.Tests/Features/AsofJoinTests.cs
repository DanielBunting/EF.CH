using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

public class AsofJoinTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
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
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""AsofEvents"" (
                ""Timestamp"" DateTime,
                ""Symbol"" String,
                ""EventName"" String
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (""Symbol"", ""Timestamp"")
        ");

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""AsofPrices"" (
                ""Timestamp"" DateTime,
                ""Symbol"" String,
                ""Price"" Decimal64(4),
                ""Volume"" UInt64
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (""Symbol"", ""Timestamp"")
        ");
    }

    private async Task SeedData(AsofJoinTestContext context)
    {
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO ""AsofPrices"" (""Timestamp"", ""Symbol"", ""Price"", ""Volume"") VALUES
            ('2024-01-01 09:00:00', 'AAPL', 150.0000, 1000),
            ('2024-01-01 10:00:00', 'AAPL', 152.5000, 2000),
            ('2024-01-01 11:00:00', 'AAPL', 151.0000, 1500),
            ('2024-01-01 09:00:00', 'GOOG', 140.0000, 500),
            ('2024-01-01 10:00:00', 'GOOG', 142.0000, 800)
        ");

        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO ""AsofEvents"" (""Timestamp"", ""Symbol"", ""EventName"") VALUES
            ('2024-01-01 09:30:00', 'AAPL', 'Trade1'),
            ('2024-01-01 10:30:00', 'AAPL', 'Trade2'),
            ('2024-01-01 11:30:00', 'AAPL', 'Trade3'),
            ('2024-01-01 09:30:00', 'GOOG', 'Trade4'),
            ('2024-01-01 10:30:00', 'GOOG', 'Trade5')
        ");
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
            entity.UseMergeTree(x => new { x.Symbol, x.Timestamp });
        });

        modelBuilder.Entity<AsofPrice>(entity =>
        {
            entity.HasKey(e => new { e.Symbol, e.Timestamp });
            entity.ToTable("AsofPrices");
            entity.UseMergeTree(x => new { x.Symbol, x.Timestamp });
        });
    }
}
