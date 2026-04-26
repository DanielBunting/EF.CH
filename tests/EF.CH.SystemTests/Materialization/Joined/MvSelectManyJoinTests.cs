using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Joined;

/// <summary>
/// Pins LINQ <c>SelectMany(source, collectionSelector, resultSelector)</c> —
/// the method form behind query-syntax <c>from o in outer from c in inner select …</c>.
/// The translator's <c>VisitSelectMany</c> arm extracts the inner queryable and an
/// optional <c>Where</c> predicate for the JOIN ON clause.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvSelectManyJoinTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvSelectManyJoinTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task SelectMany_WithInnerWhere_TranslatesToInnerJoin()
    {
        await using var ctx = TestContextFactory.Create<InnerJoinCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Customers.AddRange(
            new Customer { Id = 1, Region = "eu" },
            new Customer { Id = 2, Region = "us" });
        await ctx.SaveChangesAsync();

        ctx.Orders.AddRange(
            new Order { Id = 1, CustomerId = 1, Amount = 10 },
            new Order { Id = 2, CustomerId = 1, Amount = 20 },
            new Order { Id = 3, CustomerId = 2, Amount = 15 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvSelectManyJoinTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Region, toInt64(Total) AS T FROM \"MvSelectManyJoinTarget\" FINAL ORDER BY Region");
        Assert.Equal(2, rows.Count);
        Assert.Equal("eu", (string)rows[0]["Region"]!); Assert.Equal(30L, Convert.ToInt64(rows[0]["T"]));
        Assert.Equal("us", (string)rows[1]["Region"]!); Assert.Equal(15L, Convert.ToInt64(rows[1]["T"]));
    }

    [Fact]
    public async Task SelectMany_WithoutPredicate_TranslatesToCrossJoin()
    {
        await using var ctx = TestContextFactory.Create<CrossCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Customers.AddRange(
            new Customer { Id = 1, Region = "eu" },
            new Customer { Id = 2, Region = "us" });
        await ctx.SaveChangesAsync();

        ctx.Orders.AddRange(
            new Order { Id = 1, CustomerId = 99, Amount = 10 },
            new Order { Id = 2, CustomerId = 99, Amount = 20 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvSelectManyCrossTarget");

        // Cross product: 2 orders × 2 customers = 4 rows; sum per region = 30 each.
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Region, toInt64(Total) AS T FROM \"MvSelectManyCrossTarget\" FINAL ORDER BY Region");
        Assert.Equal(2, rows.Count);
        Assert.Equal(30L, Convert.ToInt64(rows[0]["T"]));
        Assert.Equal(30L, Convert.ToInt64(rows[1]["T"]));
    }

    public sealed class Customer { public long Id { get; set; } public string Region { get; set; } = ""; }
    public sealed class Order { public long Id { get; set; } public long CustomerId { get; set; } public long Amount { get; set; } }
    public sealed class Tgt { public string Region { get; set; } = ""; public long Total { get; set; } }

    private static readonly IQueryable<Customer> _customers = Enumerable.Empty<Customer>().AsQueryable();

    public sealed class InnerJoinCtx(DbContextOptions<InnerJoinCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<Tgt> Target => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("SmJInCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Order>(e => { e.ToTable("SmJInOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvSelectManyJoinTarget"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Region);
                e.AsMaterializedView<Tgt, Order>(orders => orders
                    .SelectMany(
                        o => _customers.Where(c => c.Id == o.CustomerId),
                        (o, c) => new { o.Amount, c.Region })
                    .GroupBy(x => x.Region)
                    .Select(g => new Tgt { Region = g.Key, Total = g.Sum(x => x.Amount) }));
            });
        }
    }

    public sealed class CrossCtx(DbContextOptions<CrossCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<Tgt> Target => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("SmJCrCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Order>(e => { e.ToTable("SmJCrOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvSelectManyCrossTarget"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Region);
                e.AsMaterializedView<Tgt, Order>(orders => orders
                    .SelectMany(
                        _ => _customers,
                        (o, c) => new { o.Amount, c.Region })
                    .GroupBy(x => x.Region)
                    .Select(g => new Tgt { Region = g.Key, Total = g.Sum(x => x.Amount) }));
            });
        }
    }
}
