using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.QueryOperators;

/// <summary>
/// Coverage of <c>AsofJoin</c> and <c>AsofLeftJoin</c>. ASOF JOIN finds the
/// closest preceding row in the inner relation matching the outer's equi-key.
/// Time-series scenario: each trade matches the last quote at-or-before its time.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class AsofJoinTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public AsofJoinTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        var t0 = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        ctx.Quotes.AddRange(
            new Quote { Id = 1, Symbol = "AAA", T = t0.AddMinutes(0), Price = 100m },
            new Quote { Id = 2, Symbol = "AAA", T = t0.AddMinutes(10), Price = 110m },
            new Quote { Id = 3, Symbol = "AAA", T = t0.AddMinutes(20), Price = 120m },
            new Quote { Id = 4, Symbol = "BBB", T = t0.AddMinutes(0), Price = 50m });
        ctx.Trades.AddRange(
            new Trade { Id = 10, Symbol = "AAA", T = t0.AddMinutes(5),  Qty = 1 },
            new Trade { Id = 11, Symbol = "AAA", T = t0.AddMinutes(15), Qty = 2 },
            new Trade { Id = 12, Symbol = "BBB", T = t0.AddMinutes(7),  Qty = 3 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task AsofJoin_MatchesLastQuoteAtOrBefore()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Trades.AsofJoin(
            ctx.Quotes,
            t => t.Symbol,
            q => q.Symbol,
            (t, q) => t.T >= q.T,
            (t, q) => new { t.Id, t.Symbol, t.T, q.Price })
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal(100m, rows[0].Price);   // trade @ 5m → quote @ 0m
        Assert.Equal(110m, rows[1].Price);   // trade @ 15m → quote @ 10m
        Assert.Equal(50m,  rows[2].Price);   // BBB trade @ 7m → quote @ 0m
    }

    [Fact]
    public async Task AsofLeftJoin_PreservesUnmatchedOuterRows()
    {
        await using var ctx = await SeededAsync();
        // Add a trade whose timestamp is BEFORE all quotes for that symbol → unmatched.
        var t0 = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        ctx.Trades.Add(new Trade { Id = 99, Symbol = "CCC", T = t0, Qty = 1 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var rows = await ctx.Trades.AsofLeftJoin(
            ctx.Quotes,
            t => t.Symbol,
            q => q.Symbol,
            (t, q) => t.T >= q.T,
            (t, q) => new { t.Id, q.Price })
            .OrderBy(r => r.Id)
            .ToListAsync();
        Assert.Equal(4, rows.Count);
        Assert.Contains(rows, r => r.Id == 99 && r.Price == 0m); // unmatched → default Price
    }

    public sealed class Quote
    {
        public uint Id { get; set; }
        public string Symbol { get; set; } = "";
        public DateTime T { get; set; }
        public decimal Price { get; set; }
    }
    public sealed class Trade
    {
        public uint Id { get; set; }
        public string Symbol { get; set; } = "";
        public DateTime T { get; set; }
        public int Qty { get; set; }
    }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Quote> Quotes => Set<Quote>();
        public DbSet<Trade> Trades => Set<Trade>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Quote>(e =>
            {
                e.ToTable("AsofJoinOpTests_Quotes"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.Symbol, x.T });
            });
            mb.Entity<Trade>(e =>
            {
                e.ToTable("AsofJoinOpTests_Trades"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.Symbol, x.T });
            });
        }
    }
}
