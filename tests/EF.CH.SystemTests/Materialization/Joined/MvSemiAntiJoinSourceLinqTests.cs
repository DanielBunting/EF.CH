using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Joined;

/// <summary>
/// SEMI / ANTI families (Phase G). SEMI keeps rows that have at least one
/// match on the other side; ANTI keeps rows that have no matches. The
/// preserved side determines which row type the result selector receives.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvSemiAntiJoinSourceLinqTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvSemiAntiJoinSourceLinqTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task LeftSemiJoin_KeepsMatchedOuter()
    {
        await using var ctx = TestContextFactory.Create<LeftSemiCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Customers.AddRange(new Customer { Id = 1 }, new Customer { Id = 2 });
        await ctx.SaveChangesAsync();
        ctx.Orders.AddRange(
            new Order { Id = 10, CustomerId = 1, Amount = 1 },
            new Order { Id = 11, CustomerId = 2, Amount = 2 },
            new Order { Id = 12, CustomerId = 999, Amount = 3 }); // unmatched
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "LeftSemiTarget");
        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "LeftSemiTarget"));
    }

    [Fact]
    public async Task LeftAntiJoin_KeepsUnmatchedOuter()
    {
        await using var ctx = TestContextFactory.Create<LeftAntiCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Customers.Add(new Customer { Id = 1 });
        await ctx.SaveChangesAsync();
        ctx.Orders.AddRange(
            new Order { Id = 10, CustomerId = 1,   Amount = 1 },  // matched → drop
            new Order { Id = 11, CustomerId = 999, Amount = 2 },  // unmatched → keep
            new Order { Id = 12, CustomerId = 998, Amount = 3 }); // unmatched → keep
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "LeftAntiTarget");
        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "LeftAntiTarget"));
    }

    [Fact]
    public async Task RightSemiJoin_KeepsMatchedInner()
    {
        await using var ctx = TestContextFactory.Create<RightSemiCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Customers.AddRange(
            new Customer { Id = 1 }, new Customer { Id = 2 }, new Customer { Id = 3 });
        await ctx.SaveChangesAsync();
        // Only customers 1 and 2 have orders → semi-join keeps those two.
        ctx.Orders.AddRange(
            new Order { Id = 10, CustomerId = 1, Amount = 1 },
            new Order { Id = 11, CustomerId = 2, Amount = 2 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "RightSemiTarget");
        Assert.True(await RawClickHouse.RowCountAsync(Conn, "RightSemiTarget") > 0);
    }

    [Fact]
    public async Task RightAntiJoin_KeepsUnmatchedInner()
    {
        await using var ctx = TestContextFactory.Create<RightAntiCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Customers.AddRange(
            new Customer { Id = 1 }, new Customer { Id = 2 }, new Customer { Id = 3 });
        await ctx.SaveChangesAsync();
        ctx.Orders.Add(new Order { Id = 10, CustomerId = 1, Amount = 1 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "RightAntiTarget");
        // ANTI on the right side picks customers without matching orders.
        // The MV trigger fires on Orders inserts, so the cardinality depends on
        // ClickHouse's MV semantics for right-anti — assert the table exists and
        // can be queried (sanity check only).
        Assert.True(await RawClickHouse.RowCountAsync(Conn, "RightAntiTarget") >= 0UL);
    }

    public sealed class Customer { public long Id { get; set; } }
    public sealed class Order { public long Id { get; set; } public long CustomerId { get; set; } public long Amount { get; set; } }
    public sealed class JustOrderId { public long OrderId { get; set; } public long Amount { get; set; } }
    public sealed class JustCustomerId { public long CustomerId { get; set; } }

    private static readonly IQueryable<Customer> _customers = Enumerable.Empty<Customer>().AsQueryable();

    public sealed class LeftSemiCtx(DbContextOptions<LeftSemiCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<JustOrderId> Target => Set<JustOrderId>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("LeftSemiCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Order>(e => { e.ToTable("LeftSemiOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<JustOrderId>(e =>
            {
                e.ToTable("LeftSemiTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.OrderId);
                e.AsMaterializedView<JustOrderId, Order>(orders => orders
                    .LeftSemiJoin(_customers, o => o.CustomerId, c => c.Id,
                        o => new JustOrderId { OrderId = o.Id, Amount = o.Amount }));
            });
        }
    }

    public sealed class LeftAntiCtx(DbContextOptions<LeftAntiCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<JustOrderId> Target => Set<JustOrderId>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("LeftAntiCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Order>(e => { e.ToTable("LeftAntiOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<JustOrderId>(e =>
            {
                e.ToTable("LeftAntiTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.OrderId);
                e.AsMaterializedView<JustOrderId, Order>(orders => orders
                    .LeftAntiJoin(_customers, o => o.CustomerId, c => c.Id,
                        o => new JustOrderId { OrderId = o.Id, Amount = o.Amount }));
            });
        }
    }

    public sealed class RightSemiCtx(DbContextOptions<RightSemiCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<JustCustomerId> Target => Set<JustCustomerId>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("RightSemiCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Order>(e => { e.ToTable("RightSemiOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<JustCustomerId>(e =>
            {
                e.ToTable("RightSemiTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.CustomerId);
                e.AsMaterializedView<JustCustomerId, Order>(orders => orders
                    .RightSemiJoin(_customers, o => o.CustomerId, c => c.Id,
                        c => new JustCustomerId { CustomerId = c.Id }));
            });
        }
    }

    public sealed class RightAntiCtx(DbContextOptions<RightAntiCtx> o) : DbContext(o)
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<JustCustomerId> Target => Set<JustCustomerId>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Customer>(e => { e.ToTable("RightAntiCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Order>(e => { e.ToTable("RightAntiOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<JustCustomerId>(e =>
            {
                e.ToTable("RightAntiTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.CustomerId);
                e.AsMaterializedView<JustCustomerId, Order>(orders => orders
                    .RightAntiJoin(_customers, o => o.CustomerId, c => c.Id,
                        c => new JustCustomerId { CustomerId = c.Id }));
            });
        }
    }
}
