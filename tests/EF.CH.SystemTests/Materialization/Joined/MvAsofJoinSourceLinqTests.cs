using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Joined;

/// <summary>
/// ASOF and ASOF LEFT JOIN coverage inside materialized-view definitions using
/// <c>ClickHouseQueryableExtensions.AsofJoin</c> and <c>AsofLeftJoin</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvAsofJoinSourceLinqTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvAsofJoinSourceLinqTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task LinqAsofJoin_Inner_MatchesLastQuoteAtOrBefore()
    {
        await using var ctx = TestContextFactory.Create<AsofInnerCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var t0 = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        ctx.Quotes.AddRange(
            new Quote { Id = 1, Symbol = "AAA", T = t0.AddMinutes(0),  Price = 100m },
            new Quote { Id = 2, Symbol = "AAA", T = t0.AddMinutes(10), Price = 110m });
        await ctx.SaveChangesAsync();

        ctx.Trades.AddRange(
            new Trade { Id = 10, Symbol = "AAA", T = t0.AddMinutes(5),  Qty = 1 },
            new Trade { Id = 11, Symbol = "AAA", T = t0.AddMinutes(15), Qty = 2 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "LinqAsofInnerTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt64(TradeId) AS TradeId, toFloat64(QuotePrice) AS QuotePrice FROM \"LinqAsofInnerTarget\" ORDER BY TradeId");
        Assert.Equal(2, rows.Count);
        Assert.Equal(100.0, Convert.ToDouble(rows[0]["QuotePrice"]));   // trade @ 5m → quote @ 0m
        Assert.Equal(110.0, Convert.ToDouble(rows[1]["QuotePrice"]));   // trade @ 15m → quote @ 10m
    }

    [Fact]
    public async Task LinqAsofLeftJoin_PreservesUnmatched()
    {
        await using var ctx = TestContextFactory.Create<AsofLeftCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var t0 = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        ctx.Quotes.Add(new Quote { Id = 1, Symbol = "AAA", T = t0.AddMinutes(10), Price = 110m });
        await ctx.SaveChangesAsync();

        ctx.Trades.AddRange(
            new Trade { Id = 10, Symbol = "AAA", T = t0.AddMinutes(15), Qty = 1 }, // matches
            new Trade { Id = 11, Symbol = "BBB", T = t0.AddMinutes(15), Qty = 2 }, // no AAA quote, but symbol mismatch entirely
            new Trade { Id = 12, Symbol = "AAA", T = t0.AddMinutes(0),  Qty = 3 }); // before any quote
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "LinqAsofLeftTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt64(TradeId) AS TradeId, toFloat64(QuotePrice) AS QuotePrice FROM \"LinqAsofLeftTarget\" ORDER BY TradeId");
        Assert.Equal(3, rows.Count);
        Assert.Equal(110.0, Convert.ToDouble(rows[0]["QuotePrice"]));   // matched
        Assert.Equal(0.0,   Convert.ToDouble(rows[1]["QuotePrice"]));   // unmatched → default
        Assert.Equal(0.0,   Convert.ToDouble(rows[2]["QuotePrice"]));   // unmatched → default
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
    public sealed class TradeWithQuote
    {
        public uint TradeId { get; set; }
        public DateTime TradeT { get; set; }
        public decimal QuotePrice { get; set; }
    }

    private static readonly IQueryable<Quote> _quotes = Enumerable.Empty<Quote>().AsQueryable();

    public sealed class AsofInnerCtx(DbContextOptions<AsofInnerCtx> o) : DbContext(o)
    {
        public DbSet<Quote> Quotes => Set<Quote>();
        public DbSet<Trade> Trades => Set<Trade>();
        public DbSet<TradeWithQuote> Target => Set<TradeWithQuote>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Quote>(e => { e.ToTable("LinqAsofInnerQuotes"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Symbol, x.T }); });
            mb.Entity<Trade>(e => { e.ToTable("LinqAsofInnerTrades"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Symbol, x.T }); });
            mb.Entity<TradeWithQuote>(e =>
            {
                e.ToTable("LinqAsofInnerTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.TradeId);

            });
            mb.MaterializedView<TradeWithQuote>().From<Trade>().DefinedAs(trades => trades
                    .AsofJoin(_quotes,
                        t => t.Symbol,
                        q => q.Symbol,
                        (t, q) => t.T >= q.T,
                        (t, q) => new TradeWithQuote { TradeId = t.Id, TradeT = t.T, QuotePrice = q.Price }));
        }
    }

    public sealed class AsofLeftCtx(DbContextOptions<AsofLeftCtx> o) : DbContext(o)
    {
        public DbSet<Quote> Quotes => Set<Quote>();
        public DbSet<Trade> Trades => Set<Trade>();
        public DbSet<TradeWithQuote> Target => Set<TradeWithQuote>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Quote>(e => { e.ToTable("LinqAsofLeftQuotes"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Symbol, x.T }); });
            mb.Entity<Trade>(e => { e.ToTable("LinqAsofLeftTrades"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Symbol, x.T }); });
            mb.Entity<TradeWithQuote>(e =>
            {
                e.ToTable("LinqAsofLeftTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.TradeId);

            });
            mb.MaterializedView<TradeWithQuote>().From<Trade>().DefinedAs(trades => trades
                    .AsofLeftJoin(_quotes,
                        t => t.Symbol,
                        q => q.Symbol,
                        (t, q) => t.T >= q.T,
                        (t, q) => new TradeWithQuote { TradeId = t.Id, TradeT = t.T, QuotePrice = q.Price }));
        }
    }
}
