using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// Drives the full *model → DDL generation → deployment* loop for each major
/// engine via the EF fluent API, then validates the resulting schema by
/// querying ClickHouse directly through <see cref="RawClickHouse"/>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class EnsureCreatedDeploymentTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public EnsureCreatedDeploymentTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task MergeTree_IsDeployedWithConfiguredOrderByAndPartition()
    {
        await using var ctx = TestContextFactory.Create<MergeTreeCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "Orders"));
        var engine = await RawClickHouse.EngineFullAsync(Conn, "Orders");
        Assert.Contains("MergeTree", engine);
        Assert.Contains("PARTITION BY toYYYYMM", engine);
        Assert.Contains("ORDER BY (OrderDate, Id)", engine);
    }

    [Fact]
    public async Task ReplacingMergeTree_IsDeployedWithVersionColumn()
    {
        await using var ctx = TestContextFactory.Create<ReplacingCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var engine = await RawClickHouse.EngineFullAsync(Conn, "Users");
        Assert.Contains("ReplacingMergeTree(Version)", engine);
        Assert.Contains("ORDER BY Id", engine);
    }

    [Fact]
    public async Task SummingMergeTree_IsDeployed()
    {
        await using var ctx = TestContextFactory.Create<SummingCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var engine = await RawClickHouse.EngineFullAsync(Conn, "HourlyTotals");
        Assert.Contains("SummingMergeTree", engine);
        Assert.Contains("ORDER BY (Hour, ProductId)", engine);
    }

    [Fact]
    public async Task AggregatingMergeTree_IsDeployed()
    {
        await using var ctx = TestContextFactory.Create<AggregatingCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var engine = await RawClickHouse.EngineFullAsync(Conn, "UserStates");
        Assert.Contains("AggregatingMergeTree", engine);
        Assert.Contains("ORDER BY UserId", engine);
    }

    [Fact]
    public async Task CollapsingMergeTree_IsDeployed()
    {
        await using var ctx = TestContextFactory.Create<CollapsingCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var engine = await RawClickHouse.EngineFullAsync(Conn, "Accounts");
        Assert.Contains("CollapsingMergeTree(Sign)", engine);
        Assert.Contains("ORDER BY AccountId", engine);
    }

    [Fact]
    public async Task NullEngine_IsDeployed()
    {
        await using var ctx = TestContextFactory.Create<NullCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var engine = await RawClickHouse.EngineFullAsync(Conn, "Ingest");
        Assert.Contains("Null", engine);
    }

    [Fact]
    public async Task InsertsThroughEf_PersistForDeployedEngine()
    {
        await using var ctx = TestContextFactory.Create<MergeTreeCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var now = DateTime.UtcNow;
        ctx.Orders.AddRange(Enumerable.Range(0, 25).Select(i => new OrderEntity
        {
            Id = Guid.NewGuid(),
            OrderDate = now.AddMinutes(-i),
            ProductId = i % 3,
            Revenue = 10m * i,
        }));
        await ctx.SaveChangesAsync();

        Assert.Equal(25UL, await RawClickHouse.RowCountAsync(Conn, "Orders"));
    }

    // ── Contexts ────────────────────────────────────────────────────────────

    public sealed class MergeTreeCtx(DbContextOptions<MergeTreeCtx> o) : DbContext(o)
    {
        public DbSet<OrderEntity> Orders => Set<OrderEntity>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<OrderEntity>(e =>
            {
                e.ToTable("Orders");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.OrderDate, x.Id });
                e.HasPartitionByMonth(x => x.OrderDate);
            });
    }

    public sealed class ReplacingCtx(DbContextOptions<ReplacingCtx> o) : DbContext(o)
    {
        public DbSet<UserEntity> Users => Set<UserEntity>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<UserEntity>(e =>
            {
                e.ToTable("Users"); e.HasKey(x => x.Id);
                e.UseReplacingMergeTree("Version", "Id");
            });
    }

    public sealed class SummingCtx(DbContextOptions<SummingCtx> o) : DbContext(o)
    {
        public DbSet<HourlyTotalEntity> HourlyTotals => Set<HourlyTotalEntity>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<HourlyTotalEntity>(e =>
            {
                e.ToTable("HourlyTotals"); e.HasNoKey();
                e.UseSummingMergeTree(x => new { x.Hour, x.ProductId });
            });
    }

    public sealed class AggregatingCtx(DbContextOptions<AggregatingCtx> o) : DbContext(o)
    {
        public DbSet<UserStateEntity> UserStates => Set<UserStateEntity>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<UserStateEntity>(e =>
            {
                e.ToTable("UserStates"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.UserId);
            });
    }

    public sealed class CollapsingCtx(DbContextOptions<CollapsingCtx> o) : DbContext(o)
    {
        public DbSet<AccountEntity> Accounts => Set<AccountEntity>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<AccountEntity>(e =>
            {
                e.ToTable("Accounts"); e.HasKey(x => x.AccountId);
                e.UseCollapsingMergeTree("Sign", "AccountId");
            });
    }

    public sealed class NullCtx(DbContextOptions<NullCtx> o) : DbContext(o)
    {
        public DbSet<IngestEntity> Ingest => Set<IngestEntity>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<IngestEntity>(e =>
            {
                e.ToTable("Ingest"); e.HasNoKey();
                e.UseNullEngine();
            });
    }

    // ── Entities ────────────────────────────────────────────────────────────

    public class OrderEntity { public Guid Id { get; set; } public DateTime OrderDate { get; set; } public int ProductId { get; set; } public decimal Revenue { get; set; } }
    public class UserEntity { public Guid Id { get; set; } public string Name { get; set; } = ""; public long Version { get; set; } }
    public class HourlyTotalEntity { public DateTime Hour { get; set; } public int ProductId { get; set; } public long Count { get; set; } public decimal Revenue { get; set; } }
    public class UserStateEntity { public Guid UserId { get; set; } public long VisitCount { get; set; } }
    public class AccountEntity { public Guid AccountId { get; set; } public decimal Balance { get; set; } public sbyte Sign { get; set; } }
    public class IngestEntity { public DateTime Timestamp { get; set; } public string Payload { get; set; } = ""; }
}
