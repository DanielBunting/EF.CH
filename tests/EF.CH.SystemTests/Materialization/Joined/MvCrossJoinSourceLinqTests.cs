using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Joined;

/// <summary>
/// CROSS JOIN (Phase J). Bare cartesian product — for each row in the outer
/// source, every row from the inner is paired. Inserting M outer rows over an
/// inner with N rows produces M*N pairs in the MV target.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvCrossJoinSourceLinqTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvCrossJoinSourceLinqTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task CrossJoin_ProducesCartesianProduct()
    {
        await using var ctx = TestContextFactory.Create<CrossCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Tags.AddRange(
            new Tag { Id = 1, Name = "a" },
            new Tag { Id = 2, Name = "b" });
        await ctx.SaveChangesAsync();
        ctx.Orders.AddRange(
            new Order { Id = 10 },
            new Order { Id = 11 },
            new Order { Id = 12 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "CrossJoinTarget");
        // 3 orders × 2 tags = 6 rows.
        Assert.Equal(6UL, await RawClickHouse.RowCountAsync(Conn, "CrossJoinTarget"));
    }

    public sealed class Order { public long Id { get; set; } }
    public sealed class Tag { public long Id { get; set; } public string Name { get; set; } = ""; }
    public sealed class OrderTag { public long OrderId { get; set; } public string TagName { get; set; } = ""; }

    private static readonly IQueryable<Tag> _tags = Enumerable.Empty<Tag>().AsQueryable();

    public sealed class CrossCtx(DbContextOptions<CrossCtx> o) : DbContext(o)
    {
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<Tag> Tags => Set<Tag>();
        public DbSet<OrderTag> Target => Set<OrderTag>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Order>(e => { e.ToTable("CrossJoinOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tag>(e => { e.ToTable("CrossJoinTags"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<OrderTag>(e =>
            {
                e.ToTable("CrossJoinTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.OrderId);

            });
            mb.MaterializedView<OrderTag>().From<Order>().DefinedAs(orders => orders
                    .CrossJoin(_tags, (o, t) => new OrderTag { OrderId = o.Id, TagName = t.Name }));
        }
    }
}
