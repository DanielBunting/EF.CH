using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Joined;

/// <summary>
/// Pins multi-source JOIN chains: <c>orders.Join(customers, …).Join(products, …)</c>.
/// The second <c>VisitJoin</c> picks up the prior join's transparent identifier
/// as its outer binding, so the second outer-key lambda's member access
/// resolves through the first join's flat row map.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvThreeWayJoinTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvThreeWayJoinTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task OrdersCustomersProducts_InnerJoinChain()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Customers.AddRange(
            new Customer { Id = 1, Region = "eu" },
            new Customer { Id = 2, Region = "us" });
        await ctx.SaveChangesAsync();

        ctx.Products.AddRange(
            new Product { Id = 100, Category = "books" },
            new Product { Id = 200, Category = "music" });
        await ctx.SaveChangesAsync();

        ctx.Orders.AddRange(
            new Order { Id = 1, CustomerId = 1, ProductId = 100, Amount = 10 },
            new Order { Id = 2, CustomerId = 1, ProductId = 200, Amount = 20 },
            new Order { Id = 3, CustomerId = 2, ProductId = 100, Amount = 30 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "Mv3WayJoinTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Region, Category, toInt64(Total) AS T FROM \"Mv3WayJoinTarget\" FINAL ORDER BY Region, Category");
        Assert.Equal(3, rows.Count);
        Assert.Equal(("eu", "books", 10L), ((string)rows[0]["Region"]!, (string)rows[0]["Category"]!, Convert.ToInt64(rows[0]["T"])));
        Assert.Equal(("eu", "music", 20L), ((string)rows[1]["Region"]!, (string)rows[1]["Category"]!, Convert.ToInt64(rows[1]["T"])));
        Assert.Equal(("us", "books", 30L), ((string)rows[2]["Region"]!, (string)rows[2]["Category"]!, Convert.ToInt64(rows[2]["T"])));
    }

    public sealed class Customer { public long Id { get; set; } public string Region { get; set; } = ""; }
    public sealed class Product { public long Id { get; set; } public string Category { get; set; } = ""; }
    public sealed class Order { public long Id { get; set; } public long CustomerId { get; set; } public long ProductId { get; set; } public long Amount { get; set; } }
    public sealed class Tgt { public string Region { get; set; } = ""; public string Category { get; set; } = ""; public long Total { get; set; } }

    private static readonly IQueryable<Customer> _customers = Enumerable.Empty<Customer>().AsQueryable();
    private static readonly IQueryable<Product> _products = Enumerable.Empty<Product>().AsQueryable();

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Product> Products => Set<Product>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<Tgt> Target => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("M3WCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Product>(e => { e.ToTable("M3WProducts"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Order>(e => { e.ToTable("M3WOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("Mv3WayJoinTarget"); e.HasNoKey();
                e.UseSummingMergeTree(x => new { x.Region, x.Category });
                e.AsMaterializedView<Tgt, Order>(orders => orders
                    .Join(_customers,
                        o => o.CustomerId,
                        c => c.Id,
                        (o, c) => new { o.Amount, o.ProductId, c.Region })
                    .Join(_products,
                        oc => oc.ProductId,
                        p => p.Id,
                        (oc, p) => new { oc.Region, p.Category, oc.Amount })
                    .GroupBy(x => new { x.Region, x.Category })
                    .Select(g => new Tgt
                    {
                        Region = g.Key.Region,
                        Category = g.Key.Category,
                        Total = g.Sum(x => x.Amount),
                    }));
            });
        }
    }
}
