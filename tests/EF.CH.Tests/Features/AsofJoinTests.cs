using EF.CH.Extensions;
using EF.CH.Query;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

/// <summary>
/// Integration tests for ASOF JOIN feature.
/// ASOF JOIN matches rows based on the closest timestamp rather than exact equality.
///
/// Note: Due to EF Core's query pipeline architecture, ASOF JOIN cannot be fully
/// integrated as a LINQ extension method because the NavigationExpandingExpressionVisitor
/// runs before our custom visitor and doesn't recognize the custom method.
///
/// For production use, ASOF JOIN should be executed via:
/// 1. Raw SQL queries using FromSqlRaw/SqlQuery
/// 2. Client-side evaluation after loading both datasets
/// </summary>
public class AsofJoinTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    private string GetConnectionString() => _container.GetConnectionString();

    [Fact(Skip = "ASOF JOIN requires EF Core pipeline extension - use raw SQL for ASOF queries")]
    public async Task AsofJoin_FindsClosestMatch()
    {
        await using var context = CreateContext();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var baseTime = new DateTime(2024, 1, 1, 10, 0, 0, DateTimeKind.Utc);

        // Insert quotes at specific times
        context.Quotes.AddRange(
            new Quote { Id = Guid.NewGuid(), Symbol = "AAPL", QuoteTime = baseTime.AddMinutes(-10), BidPrice = 150.00m },
            new Quote { Id = Guid.NewGuid(), Symbol = "AAPL", QuoteTime = baseTime.AddMinutes(-5), BidPrice = 150.50m },
            new Quote { Id = Guid.NewGuid(), Symbol = "AAPL", QuoteTime = baseTime, BidPrice = 151.00m },
            new Quote { Id = Guid.NewGuid(), Symbol = "GOOG", QuoteTime = baseTime.AddMinutes(-8), BidPrice = 140.00m },
            new Quote { Id = Guid.NewGuid(), Symbol = "GOOG", QuoteTime = baseTime.AddMinutes(-3), BidPrice = 140.50m }
        );
        await context.SaveChangesAsync();

        // Insert trades between quote times
        context.Trades.AddRange(
            // AAPL trade at -7 should match quote at -10 (closest before)
            new Trade { Id = Guid.NewGuid(), Symbol = "AAPL", TradeTime = baseTime.AddMinutes(-7), Price = 150.25m },
            // AAPL trade at -2 should match quote at -5 (closest before)
            new Trade { Id = Guid.NewGuid(), Symbol = "AAPL", TradeTime = baseTime.AddMinutes(-2), Price = 150.75m },
            // GOOG trade at -4 should match quote at -8 (closest before)
            new Trade { Id = Guid.NewGuid(), Symbol = "GOOG", TradeTime = baseTime.AddMinutes(-4), Price = 140.40m }
        );
        await context.SaveChangesAsync();

        // Perform ASOF JOIN
        var results = await context.Trades
            .AsofJoin(
                context.Quotes,
                t => t.Symbol,
                q => q.Symbol,
                t => t.TradeTime,
                q => q.QuoteTime,
                AsofJoinType.GreaterOrEqual,
                (t, q) => new { t.Symbol, t.TradeTime, t.Price, QuoteBid = q.BidPrice })
            .OrderBy(x => x.Symbol)
            .ThenBy(x => x.TradeTime)
            .ToListAsync();

        Assert.Equal(3, results.Count);

        // Verify AAPL trade at -7 matched quote at -10
        var aaplTrade1 = results.First(x => x.Symbol == "AAPL" && x.TradeTime == baseTime.AddMinutes(-7));
        Assert.Equal(150.00m, aaplTrade1.QuoteBid);

        // Verify AAPL trade at -2 matched quote at -5
        var aaplTrade2 = results.First(x => x.Symbol == "AAPL" && x.TradeTime == baseTime.AddMinutes(-2));
        Assert.Equal(150.50m, aaplTrade2.QuoteBid);

        // Verify GOOG trade at -4 matched quote at -8
        var googTrade = results.First(x => x.Symbol == "GOOG");
        Assert.Equal(140.00m, googTrade.QuoteBid);
    }

    [Fact(Skip = "ASOF JOIN requires EF Core pipeline extension - use raw SQL for ASOF queries")]
    public async Task AsofLeftJoin_PreservesUnmatchedOuter()
    {
        await using var context = CreateContext();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var baseTime = new DateTime(2024, 1, 1, 10, 0, 0, DateTimeKind.Utc);

        // Insert quotes only for AAPL
        context.Quotes.Add(
            new Quote { Id = Guid.NewGuid(), Symbol = "AAPL", QuoteTime = baseTime.AddMinutes(-5), BidPrice = 150.00m }
        );
        await context.SaveChangesAsync();

        // Insert trades for AAPL and MSFT (no quotes for MSFT)
        context.Trades.AddRange(
            new Trade { Id = Guid.NewGuid(), Symbol = "AAPL", TradeTime = baseTime, Price = 150.25m },
            new Trade { Id = Guid.NewGuid(), Symbol = "MSFT", TradeTime = baseTime, Price = 380.00m }
        );
        await context.SaveChangesAsync();

        // Perform ASOF LEFT JOIN
        var results = await context.Trades
            .AsofLeftJoin(
                context.Quotes,
                t => t.Symbol,
                q => q.Symbol,
                t => t.TradeTime,
                q => q.QuoteTime,
                AsofJoinType.GreaterOrEqual,
                (t, q) => new { t.Symbol, t.Price, HasQuote = q != null, QuoteBid = q != null ? q.BidPrice : (decimal?)null })
            .OrderBy(x => x.Symbol)
            .ToListAsync();

        Assert.Equal(2, results.Count);

        // AAPL should have a matched quote
        var aapl = results.First(x => x.Symbol == "AAPL");
        Assert.True(aapl.HasQuote);
        Assert.Equal(150.00m, aapl.QuoteBid);

        // MSFT should not have a matched quote (LEFT JOIN preserves it)
        var msft = results.First(x => x.Symbol == "MSFT");
        Assert.False(msft.HasQuote);
        Assert.Null(msft.QuoteBid);
    }

    /// <summary>
    /// Demonstrates ASOF JOIN using raw SQL - the recommended approach for ClickHouse ASOF queries.
    /// </summary>
    [Fact]
    public async Task AsofJoin_WithRawSql_FindsClosestMatch()
    {
        await using var context = CreateContext();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var baseTime = new DateTime(2024, 1, 1, 10, 0, 0, DateTimeKind.Utc);

        // Insert quotes at specific times
        context.Quotes.AddRange(
            new Quote { Id = Guid.NewGuid(), Symbol = "AAPL", QuoteTime = baseTime.AddMinutes(-10), BidPrice = 150.00m },
            new Quote { Id = Guid.NewGuid(), Symbol = "AAPL", QuoteTime = baseTime.AddMinutes(-5), BidPrice = 150.50m },
            new Quote { Id = Guid.NewGuid(), Symbol = "AAPL", QuoteTime = baseTime, BidPrice = 151.00m }
        );
        await context.SaveChangesAsync();

        // Insert trades between quote times
        context.Trades.AddRange(
            // AAPL trade at -7 should match quote at -10 (closest before)
            new Trade { Id = Guid.NewGuid(), Symbol = "AAPL", TradeTime = baseTime.AddMinutes(-7), Price = 150.25m }
        );
        await context.SaveChangesAsync();

        // Perform ASOF JOIN using raw SQL
        var sql = @"
            SELECT t.Symbol, t.TradeTime, t.Price, q.BidPrice AS QuoteBid
            FROM ""Trades"" AS t
            ASOF JOIN ""Quotes"" AS q
            ON t.Symbol = q.Symbol AND t.TradeTime >= q.QuoteTime
            ORDER BY t.Symbol, t.TradeTime";

        var results = await context.Database.SqlQueryRaw<AsofJoinResult>(sql).ToListAsync();

        Assert.Single(results);
        Assert.Equal("AAPL", results[0].Symbol);
        Assert.Equal(150.00m, results[0].QuoteBid); // Matched quote at -10 (150.00)
    }

    [Fact]
    public void AsofJoin_ThrowsForNullSource()
    {
        IQueryable<Trade> nullSource = null!;

        using var context = CreateContext();

        Assert.Throws<ArgumentNullException>(() =>
            nullSource.AsofJoin(
                context.Quotes,
                t => t.Symbol,
                q => q.Symbol,
                t => t.TradeTime,
                q => q.QuoteTime,
                AsofJoinType.GreaterOrEqual,
                (t, q) => new { t, q }));
    }

    [Fact]
    public void AsofJoin_ThrowsForNullInner()
    {
        using var context = CreateContext();
        IQueryable<Quote> nullInner = null!;

        Assert.Throws<ArgumentNullException>(() =>
            context.Trades.AsofJoin(
                nullInner,
                t => t.Symbol,
                q => q.Symbol,
                t => t.TradeTime,
                q => q.QuoteTime,
                AsofJoinType.GreaterOrEqual,
                (t, q) => new { t, q }));
    }

    private AsofJoinDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<AsofJoinDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new AsofJoinDbContext(options);
    }
}

public class AsofJoinDbContext : DbContext
{
    public AsofJoinDbContext(DbContextOptions<AsofJoinDbContext> options) : base(options)
    {
    }

    public DbSet<Quote> Quotes => Set<Quote>();
    public DbSet<Trade> Trades => Set<Trade>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Quote>(entity =>
        {
            entity.ToTable("Quotes");
            entity.HasKey(e => e.Id);
            // ORDER BY must include ASOF column for efficient joins
            entity.UseMergeTree(x => new { x.Symbol, x.QuoteTime });
        });

        modelBuilder.Entity<Trade>(entity =>
        {
            entity.ToTable("Trades");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Symbol, x.TradeTime });
        });
    }
}

public class Quote
{
    public Guid Id { get; set; }
    public string Symbol { get; set; } = string.Empty;
    public DateTime QuoteTime { get; set; }
    public decimal BidPrice { get; set; }
}

public class Trade
{
    public Guid Id { get; set; }
    public string Symbol { get; set; } = string.Empty;
    public DateTime TradeTime { get; set; }
    public decimal Price { get; set; }
}

/// <summary>
/// Result type for raw SQL ASOF JOIN query.
/// </summary>
public class AsofJoinResult
{
    public string Symbol { get; set; } = string.Empty;
    public DateTime TradeTime { get; set; }
    public decimal Price { get; set; }
    public decimal QuoteBid { get; set; }
}
