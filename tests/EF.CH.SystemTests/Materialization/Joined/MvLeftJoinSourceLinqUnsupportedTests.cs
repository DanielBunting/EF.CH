using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Joined;

/// <summary>
/// Canonical LINQ LEFT JOIN via the C# query-syntax idiom
/// (<c>from o in outer join c in inner on … into cs from c in cs.DefaultIfEmpty() select …</c>)
/// and the equivalent fluent form (explicit <c>GroupJoin</c> + <c>SelectMany</c>
/// + <c>DefaultIfEmpty</c>). Both lower to the same expression tree shape that
/// the MV translator must recognise as a single SQL <c>LEFT JOIN</c>.
/// TODO: rename to MvLeftJoinSourceLinqTests once green.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvLeftJoinSourceLinqUnsupportedTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvLeftJoinSourceLinqUnsupportedTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task LinqLeftJoin_QuerySyntax_ShouldEventuallyWork()
    {
        await using var ctx = TestContextFactory.Create<QuerySyntaxCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Customers.Add(new Customer { Id = 1, Region = "eu" });
        await ctx.SaveChangesAsync();

        ctx.Orders.AddRange(
            new Order { Id = 1, CustomerId = 1,   Amount = 10 },
            new Order { Id = 2, CustomerId = 999, Amount = 99 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "LinqLeftJoinQueryTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt64(OrderId) AS OrderId, Region, toInt64(Amount) AS Amount FROM \"LinqLeftJoinQueryTarget\" ORDER BY OrderId");
        Assert.Equal(2, rows.Count);
        Assert.Equal("eu", (string)rows[0]["Region"]!);
        Assert.True(rows[1]["Region"] is null or "", "Unmatched LEFT JOIN row should yield empty/NULL Region");
    }

    [Fact]
    public async Task LinqLeftJoin_FluentSyntax_ShouldEventuallyWork()
    {
        await using var ctx = TestContextFactory.Create<FluentCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Customers.Add(new Customer { Id = 1, Region = "eu" });
        await ctx.SaveChangesAsync();

        ctx.Orders.AddRange(
            new Order { Id = 1, CustomerId = 1,   Amount = 10 },
            new Order { Id = 2, CustomerId = 999, Amount = 99 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "LinqLeftJoinFluentTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt64(OrderId) AS OrderId, Region, toInt64(Amount) AS Amount FROM \"LinqLeftJoinFluentTarget\" ORDER BY OrderId");
        Assert.Equal(2, rows.Count);
        Assert.Equal("eu", (string)rows[0]["Region"]!);
        Assert.True(rows[1]["Region"] is null or "", "Unmatched LEFT JOIN row should yield empty/NULL Region");
    }

    public sealed class Customer { public long Id { get; set; } public string Region { get; set; } = ""; }
    public sealed class Order { public long Id { get; set; } public long CustomerId { get; set; } public long Amount { get; set; } }
    public sealed class OrderRegion { public long OrderId { get; set; } public string Region { get; set; } = ""; public long Amount { get; set; } }

    private static readonly IQueryable<Customer> _customers = Enumerable.Empty<Customer>().AsQueryable();

    public sealed class QuerySyntaxCtx(DbContextOptions<QuerySyntaxCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<OrderRegion> Target => Set<OrderRegion>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("LinqLeftJoinQueryCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Order>(e => { e.ToTable("LinqLeftJoinQueryOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<OrderRegion>(e =>
            {
                e.ToTable("LinqLeftJoinQueryTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.OrderId);
                e.AsMaterializedView<OrderRegion, Order>(orders =>
                    from o in orders
                    join c in _customers on o.CustomerId equals c.Id into cs
                    from c in cs.DefaultIfEmpty()
                    select new OrderRegion { OrderId = o.Id, Region = c == null ? "" : c.Region, Amount = o.Amount });
            });
        }
    }

    public sealed class FluentCtx(DbContextOptions<FluentCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<OrderRegion> Target => Set<OrderRegion>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("LinqLeftJoinFluentCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Order>(e => { e.ToTable("LinqLeftJoinFluentOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<OrderRegion>(e =>
            {
                e.ToTable("LinqLeftJoinFluentTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.OrderId);
                e.AsMaterializedView<OrderRegion, Order>(orders => orders
                    .GroupJoin(_customers,
                        o => o.CustomerId,
                        c => c.Id,
                        (o, cs) => new { o, cs })
                    .SelectMany(
                        t => t.cs.DefaultIfEmpty(),
                        (t, c) => new OrderRegion
                        {
                            OrderId = t.o.Id,
                            Region = c == null ? "" : c.Region,
                            Amount = t.o.Amount,
                        }));
            });
        }
    }
}
