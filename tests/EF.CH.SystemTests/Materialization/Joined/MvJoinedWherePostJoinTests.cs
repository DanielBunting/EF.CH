using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Joined;

/// <summary>
/// Pins <c>Where</c> applied AFTER a <c>Join</c>. The translator's
/// <c>EnterLambda</c> consumes the staged transparent identifier so the
/// predicate's parameter resolves member access through the joined-row map
/// (covering both outer and inner side columns).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvJoinedWherePostJoinTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvJoinedWherePostJoinTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Where_AfterJoin_FiltersOnInnerColumn()
    {
        await using var ctx = TestContextFactory.Create<InnerColCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Customers.AddRange(
            new Customer { Id = 1, Region = "eu" },
            new Customer { Id = 2, Region = "us" });
        await ctx.SaveChangesAsync();

        ctx.Orders.AddRange(
            new Order { Id = 1, CustomerId = 1, Amount = 10 },
            new Order { Id = 2, CustomerId = 2, Amount = 25 },
            new Order { Id = 3, CustomerId = 1, Amount = 30 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvWhereAfterJoinInnerTarget");

        // Where region == "eu" should filter out the us-customer order (Id=2).
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Region, toInt64(Total) AS T FROM \"MvWhereAfterJoinInnerTarget\" FINAL ORDER BY Region");
        Assert.Single(rows);
        Assert.Equal("eu", (string)rows[0]["Region"]!);
        Assert.Equal(40L, Convert.ToInt64(rows[0]["T"]));
    }

    [Fact]
    public async Task Where_AfterJoin_FiltersOnOuterColumn()
    {
        await using var ctx = TestContextFactory.Create<OuterColCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Customers.AddRange(
            new Customer { Id = 1, Region = "eu" },
            new Customer { Id = 2, Region = "us" });
        await ctx.SaveChangesAsync();

        ctx.Orders.AddRange(
            new Order { Id = 1, CustomerId = 1, Amount = 10 },
            new Order { Id = 2, CustomerId = 1, Amount = 50 },
            new Order { Id = 3, CustomerId = 2, Amount = 25 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvWhereAfterJoinOuterTarget");

        // Where Amount > 20 keeps only orders 2 and 3.
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Region, toInt64(Total) AS T FROM \"MvWhereAfterJoinOuterTarget\" FINAL ORDER BY Region");
        Assert.Equal(2, rows.Count);
        Assert.Equal("eu", (string)rows[0]["Region"]!); Assert.Equal(50L, Convert.ToInt64(rows[0]["T"]));
        Assert.Equal("us", (string)rows[1]["Region"]!); Assert.Equal(25L, Convert.ToInt64(rows[1]["T"]));
    }

    [Fact]
    public async Task Where_AfterJoin_BothSidesInPredicate()
    {
        await using var ctx = TestContextFactory.Create<BothSidesCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Customers.AddRange(
            new Customer { Id = 1, Region = "eu" },
            new Customer { Id = 2, Region = "us" });
        await ctx.SaveChangesAsync();

        ctx.Orders.AddRange(
            new Order { Id = 1, CustomerId = 1, Amount = 50 },  // eu, kept
            new Order { Id = 2, CustomerId = 1, Amount = 10 },  // eu but small, dropped
            new Order { Id = 3, CustomerId = 2, Amount = 50 }); // us, dropped (region check)
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvWhereAfterJoinBothTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Region, toInt64(Total) AS T FROM \"MvWhereAfterJoinBothTarget\" FINAL");
        Assert.Single(rows);
        Assert.Equal("eu", (string)rows[0]["Region"]!);
        Assert.Equal(50L, Convert.ToInt64(rows[0]["T"]));
    }

    public sealed class Customer { public long Id { get; set; } public string Region { get; set; } = ""; }
    public sealed class Order { public long Id { get; set; } public long CustomerId { get; set; } public long Amount { get; set; } }
    public sealed class Tgt { public string Region { get; set; } = ""; public long Total { get; set; } }

    private static readonly IQueryable<Customer> _customers = Enumerable.Empty<Customer>().AsQueryable();

    public sealed class InnerColCtx(DbContextOptions<InnerColCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<Tgt> Target => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("WaJInCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Order>(e => { e.ToTable("WaJInOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvWhereAfterJoinInnerTarget"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Region);

            });
            mb.MaterializedView<Tgt>().From<Order>().DefinedAs(orders => orders
                    .Join(_customers, o => o.CustomerId, c => c.Id, (o, c) => new { o.Amount, c.Region })
                    .Where(x => x.Region == "eu")
                    .GroupBy(x => x.Region)
                    .Select(g => new Tgt { Region = g.Key, Total = g.Sum(x => x.Amount) }));
        }
    }

    public sealed class OuterColCtx(DbContextOptions<OuterColCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<Tgt> Target => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("WaJOutCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Order>(e => { e.ToTable("WaJOutOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvWhereAfterJoinOuterTarget"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Region);

            });
            mb.MaterializedView<Tgt>().From<Order>().DefinedAs(orders => orders
                    .Join(_customers, o => o.CustomerId, c => c.Id, (o, c) => new { o.Amount, c.Region })
                    .Where(x => x.Amount > 20)
                    .GroupBy(x => x.Region)
                    .Select(g => new Tgt { Region = g.Key, Total = g.Sum(x => x.Amount) }));
        }
    }

    public sealed class BothSidesCtx(DbContextOptions<BothSidesCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<Tgt> Target => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("WaJBoCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Order>(e => { e.ToTable("WaJBoOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvWhereAfterJoinBothTarget"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Region);

            });
            mb.MaterializedView<Tgt>().From<Order>().DefinedAs(orders => orders
                    .Join(_customers, o => o.CustomerId, c => c.Id, (o, c) => new { o.Amount, c.Region })
                    .Where(x => x.Region == "eu" && x.Amount >= 30)
                    .GroupBy(x => x.Region)
                    .Select(g => new Tgt { Region = g.Key, Total = g.Sum(x => x.Amount) }));
        }
    }
}
