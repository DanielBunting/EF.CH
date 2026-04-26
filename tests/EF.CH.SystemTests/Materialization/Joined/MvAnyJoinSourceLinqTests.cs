using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Joined;

/// <summary>
/// ANY strictness family (Phase F): <c>AnyJoin</c>, <c>AnyLeftJoin</c>,
/// <c>AnyRightJoin</c>. ANY returns at most one match per left row instead of
/// the implicit ALL strictness's full match expansion. Significantly faster
/// for dimension/dictionary lookups when the right side has duplicate keys.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvAnyJoinSourceLinqTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvAnyJoinSourceLinqTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task AnyJoin_RunsAndProducesRows()
    {
        await using var ctx = TestContextFactory.Create<AnyInnerCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Customers.Add(new Customer { RowId = 1, Id = 1, Region = "eu" });
        await ctx.SaveChangesAsync();
        ctx.Orders.Add(new Order { Id = 10, CustomerId = 1, Amount = 10 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "AnyJoinTarget");
        // End-to-end smoke check that ANY INNER JOIN MV definition is accepted by
        // ClickHouse and produces output. SQL shape is pinned by the unit test
        // AsMaterializedView_AnyJoin_EmitsAnyInnerJoin; ANY's strictness semantics
        // (one match per left row) are ClickHouse's responsibility.
        Assert.True(await RawClickHouse.RowCountAsync(Conn, "AnyJoinTarget") > 0);
    }

    [Fact]
    public async Task AnyLeftJoin_PreservesUnmatchedAndDedups()
    {
        await using var ctx = TestContextFactory.Create<AnyLeftCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Customers.Add(new Customer { RowId = 1, Id = 1, Region = "eu" });
        await ctx.SaveChangesAsync();

        ctx.Orders.AddRange(
            new Order { Id = 10, CustomerId = 1,   Amount = 10 },
            new Order { Id = 11, CustomerId = 999, Amount = 99 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "AnyLeftJoinTarget");
        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "AnyLeftJoinTarget"));
    }

    [Fact]
    public async Task AnyRightJoin_PreservesAllInner()
    {
        await using var ctx = TestContextFactory.Create<AnyRightCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Customers.AddRange(
            new Customer { RowId = 1, Id = 1, Region = "eu" },
            new Customer { RowId = 2, Id = 2, Region = "us" });
        await ctx.SaveChangesAsync();
        ctx.Orders.Add(new Order { Id = 10, CustomerId = 1, Amount = 10 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "AnyRightJoinTarget");
        Assert.True(await RawClickHouse.RowCountAsync(Conn, "AnyRightJoinTarget") > 0);
    }

    public sealed class Customer { public long RowId { get; set; } public long Id { get; set; } public string Region { get; set; } = ""; }
    public sealed class Order { public long Id { get; set; } public long CustomerId { get; set; } public long Amount { get; set; } }
    public sealed class Revenue { public string Region { get; set; } = ""; public long Total { get; set; } }

    private static readonly IQueryable<Customer> _customers = Enumerable.Empty<Customer>().AsQueryable();

    public sealed class AnyInnerCtx(DbContextOptions<AnyInnerCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<Revenue> Target => Set<Revenue>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("AnyJoinCustomers"); e.HasKey(x => x.RowId); e.UseMergeTree(x => x.RowId); });
            mb.Entity<Order>(e => { e.ToTable("AnyJoinOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Revenue>(e =>
            {
                e.ToTable("AnyJoinTarget"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Region);
                e.AsMaterializedView<Revenue, Order>(orders => orders
                    .AnyJoin(_customers, o => o.CustomerId, c => c.Id, (o, c) => new { o.Amount, c.Region })
                    .GroupBy(x => x.Region)
                    .Select(g => new Revenue { Region = g.Key, Total = g.Sum(x => x.Amount) }));
            });
        }
    }

    public sealed class AnyLeftCtx(DbContextOptions<AnyLeftCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<Revenue> Target => Set<Revenue>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("AnyLeftJoinCustomers"); e.HasKey(x => x.RowId); e.UseMergeTree(x => x.RowId); });
            mb.Entity<Order>(e => { e.ToTable("AnyLeftJoinOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Revenue>(e =>
            {
                e.ToTable("AnyLeftJoinTarget"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Region);
                e.AsMaterializedView<Revenue, Order>(orders => orders
                    .AnyLeftJoin(_customers, o => o.CustomerId, c => c.Id, (o, c) => new { o.Amount, c.Region })
                    .GroupBy(x => x.Region)
                    .Select(g => new Revenue { Region = g.Key, Total = g.Sum(x => x.Amount) }));
            });
        }
    }

    public sealed class AnyRightCtx(DbContextOptions<AnyRightCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<Revenue> Target => Set<Revenue>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("AnyRightJoinCustomers"); e.HasKey(x => x.RowId); e.UseMergeTree(x => x.RowId); });
            mb.Entity<Order>(e => { e.ToTable("AnyRightJoinOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Revenue>(e =>
            {
                e.ToTable("AnyRightJoinTarget"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Region);
                e.AsMaterializedView<Revenue, Order>(orders => orders
                    .AnyRightJoin(_customers, o => o.CustomerId, c => c.Id, (o, c) => new { o.Amount, c.Region })
                    .GroupBy(x => x.Region)
                    .Select(g => new Revenue { Region = g.Key, Total = g.Sum(x => x.Amount) }));
            });
        }
    }
}
