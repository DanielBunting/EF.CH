using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Joined;

/// <summary>
/// JOIN-based / multi-source MVs defined via fluent LINQ <c>.Join(...)</c> and
/// <c>.GroupJoin(...)</c>. The translator's <c>VisitJoin</c> / <c>VisitGroupJoin</c>
/// arms emit the SQL JOIN under outer-source aliases; this file pins the
/// behavioural contract: only inserts to the leftmost (outer) source fire the MV.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvJoinedSourceLinqTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvJoinedSourceLinqTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task InnerJoin_LeftSide_TriggersMv()
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

        await RawClickHouse.SettleMaterializationAsync(Conn, "LinqInnerJoinTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Region, toInt64(Total) AS Total FROM \"LinqInnerJoinTarget\" FINAL ORDER BY Region");
        Assert.Equal(2, rows.Count);
        Assert.Equal("eu", (string)rows[0]["Region"]!); Assert.Equal(30L, Convert.ToInt64(rows[0]["Total"]));
        Assert.Equal("us", (string)rows[1]["Region"]!); Assert.Equal(15L, Convert.ToInt64(rows[1]["Total"]));
    }

    [Fact]
    public async Task RightSideOnlyInsert_DoesNotTriggerMv()
    {
        await using var ctx = TestContextFactory.Create<InnerJoinCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // Only insert into Customers (right-hand side of the JOIN). ClickHouse
        // must NOT propagate this into the MV target — the trigger fires only
        // on inserts into the leftmost (outer) source, which is Orders.
        ctx.Customers.Add(new Customer { Id = 1, Region = "eu" });
        await ctx.SaveChangesAsync();

        Assert.Equal(0UL, await RawClickHouse.RowCountAsync(Conn, "LinqInnerJoinTarget"));
    }

    [Fact]
    public async Task GroupJoin_LeftJoinSemantics_HandlesUnmatchedOrders()
    {
        await using var ctx = TestContextFactory.Create<GroupJoinCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // Some orders match a customer, some don't. GroupJoin emits LEFT JOIN
        // semantics, so unmatched orders survive with an empty Region.
        ctx.Customers.Add(new Customer { Id = 1, Region = "eu" });
        await ctx.SaveChangesAsync();

        ctx.Orders.AddRange(
            new Order { Id = 1, CustomerId = 1,   Amount = 10 },
            new Order { Id = 2, CustomerId = 999, Amount = 99 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "LinqGroupJoinTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt64(OrderId) AS OrderId, Region, toInt64(Amount) AS Amount FROM \"LinqGroupJoinTarget\" ORDER BY OrderId");
        Assert.Equal(2, rows.Count);
        Assert.Equal("eu", (string)rows[0]["Region"]!);
        Assert.Equal(10L, Convert.ToInt64(rows[0]["Amount"]));
        Assert.True(rows[1]["Region"] is null or "", "Unmatched LEFT JOIN row should yield empty/NULL Region");
        Assert.Equal(99L, Convert.ToInt64(rows[1]["Amount"]));
    }

    public sealed class Customer { public long Id { get; set; } public string Region { get; set; } = ""; }
    public sealed class Order { public long Id { get; set; } public long CustomerId { get; set; } public long Amount { get; set; } }

    public sealed class RevenueByRegion { public string Region { get; set; } = ""; public long Total { get; set; } }
    public sealed class OrderRegion { public long OrderId { get; set; } public string Region { get; set; } = ""; public long Amount { get; set; } }

    /// <summary>
    /// Right-hand-side queryable for the JOIN. The translator only needs an
    /// <c>IQueryable&lt;T&gt;</c> shape at design time — it resolves <c>T</c>'s
    /// table name through the EF model, so an empty stand-in is sufficient.
    /// At runtime the MV reads from the resolved table, not this queryable.
    /// </summary>
    private static readonly IQueryable<Customer> _customers = Enumerable.Empty<Customer>().AsQueryable();

    public sealed class InnerJoinCtx(DbContextOptions<InnerJoinCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<RevenueByRegion> Target => Set<RevenueByRegion>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("LinqJoinCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Order>(e => { e.ToTable("LinqJoinOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<RevenueByRegion>(e =>
            {
                e.ToTable("LinqInnerJoinTarget"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Region);

            });
            mb.MaterializedView<RevenueByRegion>().From<Order>().DefinedAs(orders => orders
                    .Join(_customers,
                        o => o.CustomerId,
                        c => c.Id,
                        (o, c) => new { o.Amount, c.Region })
                    .GroupBy(x => x.Region)
                    .Select(g => new RevenueByRegion { Region = g.Key, Total = g.Sum(x => x.Amount) }));
        }
    }

    public sealed class GroupJoinCtx(DbContextOptions<GroupJoinCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<OrderRegion> Target => Set<OrderRegion>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("LinqGJCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Order>(e => { e.ToTable("LinqGJOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<OrderRegion>(e =>
            {
                e.ToTable("LinqGroupJoinTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.OrderId);

            });
            mb.MaterializedView<OrderRegion>().From<Order>().DefinedAs(orders => orders
                    .GroupJoin(_customers,
                        o => o.CustomerId,
                        c => c.Id,
                        (o, cs) => new OrderRegion
                        {
                            OrderId = o.Id,
                            Region = cs.Select(c => c.Region).FirstOrDefault() ?? "",
                            Amount = o.Amount,
                        }));
        }
    }
}
