using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Joined;

/// <summary>
/// RIGHT JOIN (Phase H). Preserves all inner-side rows; outer side fills with
/// type-defaults for unmatched. The MV trigger still fires on inserts to the
/// FROM-clause source (the outer/left-syntactic side).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvRightJoinSourceLinqTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvRightJoinSourceLinqTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task RightJoin_PreservesInnerRows()
    {
        await using var ctx = TestContextFactory.Create<RightJoinCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Customers.AddRange(
            new Customer { Id = 1, Region = "eu" },
            new Customer { Id = 2, Region = "us" });
        await ctx.SaveChangesAsync();
        ctx.Orders.Add(new Order { Id = 10, CustomerId = 1, Amount = 10 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "RightJoinTarget");
        Assert.True(await RawClickHouse.RowCountAsync(Conn, "RightJoinTarget") > 0);
    }

    public sealed class Customer { public long Id { get; set; } public string Region { get; set; } = ""; }
    public sealed class Order { public long Id { get; set; } public long CustomerId { get; set; } public long Amount { get; set; } }
    public sealed class Revenue { public string Region { get; set; } = ""; public long Total { get; set; } }

    private static readonly IQueryable<Customer> _customers = Enumerable.Empty<Customer>().AsQueryable();

    public sealed class RightJoinCtx(DbContextOptions<RightJoinCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<Revenue> Target => Set<Revenue>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("RightJoinCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Order>(e => { e.ToTable("RightJoinOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Revenue>(e =>
            {
                e.ToTable("RightJoinTarget"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Region);

            });
            mb.MaterializedView<Revenue>().From<Order>().DefinedAs(orders => orders
                    .RightJoin(_customers, o => o.CustomerId, c => c.Id, (o, c) => new { o.Amount, c.Region })
                    .GroupBy(x => x.Region)
                    .Select(g => new Revenue { Region = g.Key, Total = g.Sum(x => x.Amount) }));
        }
    }
}
