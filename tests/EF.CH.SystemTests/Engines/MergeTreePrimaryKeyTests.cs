using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Engines;

/// <summary>
/// Round-trips MergeTree-family tables whose <c>PRIMARY KEY</c> is a strict prefix of <c>ORDER BY</c>,
/// confirming ClickHouse stores both keys independently as configured by <c>WithPrimaryKey</c>.
/// One test per engine variant — the API is shared via <c>MergeTreeFamilyBuilder</c>, so this is
/// guarding against any engine-specific DDL drift.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MergeTreePrimaryKeyTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MergeTreePrimaryKeyTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task AssertKeysAsync(string tableName, string expectedSorting, string expectedPrimary)
    {
        var sortingKey = await RawClickHouse.ScalarAsync<string>(Conn,
            $"SELECT sorting_key FROM system.tables "
            + $"WHERE database = currentDatabase() AND name = '{tableName}'");
        var primaryKey = await RawClickHouse.ScalarAsync<string>(Conn,
            $"SELECT primary_key FROM system.tables "
            + $"WHERE database = currentDatabase() AND name = '{tableName}'");

        Assert.Equal(expectedSorting, sortingKey);
        Assert.Equal(expectedPrimary, primaryKey);
    }

    [Fact]
    public async Task MergeTree_WithPrimaryKey_RoundTrips()
    {
        await using var ctx = TestContextFactory.Create<MergeTreeCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        await AssertKeysAsync(
            "MergeTreePk_Events",
            expectedSorting: "UserId, Timestamp, EventId",
            expectedPrimary: "UserId, Timestamp");
    }

    [Fact]
    public async Task ReplacingMergeTree_WithPrimaryKey_RoundTrips()
    {
        await using var ctx = TestContextFactory.Create<ReplacingCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        await AssertKeysAsync(
            "MergeTreePk_Replacing",
            expectedSorting: "UserId, Timestamp, EventId",
            expectedPrimary: "UserId, Timestamp");
    }

    [Fact]
    public async Task SummingMergeTree_WithPrimaryKey_RoundTrips()
    {
        await using var ctx = TestContextFactory.Create<SummingCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        await AssertKeysAsync(
            "MergeTreePk_Summing",
            expectedSorting: "Region, Hour, MetricId",
            expectedPrimary: "Region, Hour");
    }

    [Fact]
    public async Task AggregatingMergeTree_WithPrimaryKey_RoundTrips()
    {
        await using var ctx = TestContextFactory.Create<AggregatingCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        await AssertKeysAsync(
            "MergeTreePk_Aggregating",
            expectedSorting: "Region, Hour, MetricId",
            expectedPrimary: "Region, Hour");
    }

    [Fact]
    public async Task CollapsingMergeTree_WithPrimaryKey_RoundTrips()
    {
        await using var ctx = TestContextFactory.Create<CollapsingCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        await AssertKeysAsync(
            "MergeTreePk_Collapsing",
            expectedSorting: "AccountId, Version, EventId",
            expectedPrimary: "AccountId, Version");
    }

    [Fact]
    public async Task VersionedCollapsingMergeTree_WithPrimaryKey_RoundTrips()
    {
        await using var ctx = TestContextFactory.Create<VersionedCollapsingCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // VersionedCollapsingMergeTree appends Version to ORDER BY automatically.
        await AssertKeysAsync(
            "MergeTreePk_VersionedCollapsing",
            expectedSorting: "AccountId, EventId, Version",
            expectedPrimary: "AccountId");
    }

    public sealed class Event
    {
        public uint UserId { get; set; }
        public DateTime Timestamp { get; set; }
        public Guid EventId { get; set; }
        public string Payload { get; set; } = "";
    }

    public sealed class Metric
    {
        public string Region { get; set; } = "";
        public DateTime Hour { get; set; }
        public Guid MetricId { get; set; }
        public long Value { get; set; }
    }

    public sealed class CollapsingRow
    {
        public uint AccountId { get; set; }
        public ulong Version { get; set; }
        public Guid EventId { get; set; }
        public sbyte Sign { get; set; }
        public string Payload { get; set; } = "";
    }

    public sealed class MergeTreeCtx(DbContextOptions<MergeTreeCtx> o) : DbContext(o)
    {
        public DbSet<Event> Events => Set<Event>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Event>(e =>
            {
                e.ToTable("MergeTreePk_Events");
                e.HasKey(x => x.EventId);
                e.UseMergeTree(x => new { x.UserId, x.Timestamp, x.EventId })
                    .WithPrimaryKey(x => new { x.UserId, x.Timestamp });
            });
    }

    public sealed class ReplacingCtx(DbContextOptions<ReplacingCtx> o) : DbContext(o)
    {
        public DbSet<Event> Events => Set<Event>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Event>(e =>
            {
                e.ToTable("MergeTreePk_Replacing");
                e.HasKey(x => x.EventId);
                e.UseReplacingMergeTree(x => new { x.UserId, x.Timestamp, x.EventId })
                    .WithPrimaryKey(x => new { x.UserId, x.Timestamp });
            });
    }

    public sealed class SummingCtx(DbContextOptions<SummingCtx> o) : DbContext(o)
    {
        public DbSet<Metric> Metrics => Set<Metric>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Metric>(e =>
            {
                e.ToTable("MergeTreePk_Summing");
                e.HasKey(x => x.MetricId);
                e.UseSummingMergeTree(x => new { x.Region, x.Hour, x.MetricId })
                    .WithPrimaryKey(x => new { x.Region, x.Hour });
            });
    }

    public sealed class AggregatingCtx(DbContextOptions<AggregatingCtx> o) : DbContext(o)
    {
        public DbSet<Metric> Metrics => Set<Metric>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Metric>(e =>
            {
                e.ToTable("MergeTreePk_Aggregating");
                e.HasKey(x => x.MetricId);
                e.UseAggregatingMergeTree(x => new { x.Region, x.Hour, x.MetricId })
                    .WithPrimaryKey(x => new { x.Region, x.Hour });
            });
    }

    public sealed class CollapsingCtx(DbContextOptions<CollapsingCtx> o) : DbContext(o)
    {
        public DbSet<CollapsingRow> Rows => Set<CollapsingRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<CollapsingRow>(e =>
            {
                e.ToTable("MergeTreePk_Collapsing");
                e.HasKey(x => x.EventId);
                e.UseCollapsingMergeTree(x => new { x.AccountId, x.Version, x.EventId })
                    .WithSign(x => x.Sign)
                    .WithPrimaryKey(x => new { x.AccountId, x.Version });
            });
    }

    public sealed class VersionedCollapsingCtx(DbContextOptions<VersionedCollapsingCtx> o) : DbContext(o)
    {
        public DbSet<CollapsingRow> Rows => Set<CollapsingRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<CollapsingRow>(e =>
            {
                e.ToTable("MergeTreePk_VersionedCollapsing");
                e.HasKey(x => x.EventId);
                e.UseVersionedCollapsingMergeTree(x => new { x.AccountId, x.EventId })
                    .WithSign(x => x.Sign)
                    .WithVersion(x => x.Version)
                    .WithPrimaryKey(x => x.AccountId);
            });
    }
}
