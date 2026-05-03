using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Engines;

/// <summary>
/// MV target = VersionedCollapsingMergeTree(Sign, Version). Pairs cancel only when
/// both Sign opposites and Version match; the most recent Version survives.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvTargetVersionedCollapsingMergeTreeTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvTargetVersionedCollapsingMergeTreeTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task MergeTree_To_VersionedCollapsingMergeTree()
    {
        await using var ctx = TestContextFactory.Create<MtToVerCollapsing.Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new MtToVerCollapsing.Row { Id = 1, AccountId = 1, Sign =  1, Version = 1, Score = 100 },
            new MtToVerCollapsing.Row { Id = 2, AccountId = 1, Sign = -1, Version = 1, Score = 100 },
            new MtToVerCollapsing.Row { Id = 3, AccountId = 1, Sign =  1, Version = 2, Score = 200 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MtToVerCollapsingTarget");

        var summed = await RawClickHouse.ScalarAsync<long>(Conn,
            "SELECT sum(Score * Sign) FROM \"MtToVerCollapsingTarget\" FINAL");
        Assert.Equal(200L, summed);
    }

    [Fact]
    public async Task NullEngine_To_VersionedCollapsingMergeTree()
    {
        await using var ctx = TestContextFactory.Create<NullToVerCollapsing.Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"NullVerCollapsingIngest\" (\"AccountId\", \"Sign\", \"Version\", \"Score\") VALUES (1, 1, 1, 100), (1, -1, 1, 100), (1, 1, 2, 200)");

        await RawClickHouse.SettleMaterializationAsync(Conn, "NullToVerCollapsingTarget");

        var summed = await RawClickHouse.ScalarAsync<long>(Conn,
            "SELECT sum(Score * Sign) FROM \"NullToVerCollapsingTarget\" FINAL");
        Assert.Equal(200L, summed);
    }

    public static class MtToVerCollapsing
    {
        public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
        {
            public DbSet<Row> Source => Set<Row>();
            public DbSet<Target> Target => Set<Target>();
            protected override void OnModelCreating(ModelBuilder mb)
            {
                mb.Entity<Row>(e => { e.ToTable("MtToVerCollapsingSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
                mb.Entity<Target>(e =>
                {
                    e.ToTable("MtToVerCollapsingTarget"); e.HasNoKey();
                    e.UseVersionedCollapsingMergeTree(x => x.AccountId).WithSign(x => x.Sign).WithVersion(x => x.Version);
                });
                mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows
                        .Select(r => new Target { AccountId = r.AccountId, Sign = r.Sign, Version = r.Version, Score = r.Score }));
            }
        }
        public class Row { public long Id { get; set; } public long AccountId { get; set; } public sbyte Sign { get; set; } public uint Version { get; set; } public int Score { get; set; } }
        public class Target { public long AccountId { get; set; } public sbyte Sign { get; set; } public uint Version { get; set; } public int Score { get; set; } }
    }

    public static class NullToVerCollapsing
    {
        public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
        {
            public DbSet<Row> Ingest => Set<Row>();
            public DbSet<Target> Target => Set<Target>();
            protected override void OnModelCreating(ModelBuilder mb)
            {
                mb.Entity<Row>(e => { e.ToTable("NullVerCollapsingIngest"); e.HasNoKey(); e.UseNullEngine(); });
                mb.Entity<Target>(e =>
                {
                    e.ToTable("NullToVerCollapsingTarget"); e.HasNoKey();
                    e.UseVersionedCollapsingMergeTree(x => x.AccountId).WithSign(x => x.Sign).WithVersion(x => x.Version);
                });
                mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows
                        .Select(r => new Target { AccountId = r.AccountId, Sign = r.Sign, Version = r.Version, Score = r.Score }));
            }
        }
        public class Row { public long AccountId { get; set; } public sbyte Sign { get; set; } public uint Version { get; set; } public int Score { get; set; } }
        public class Target { public long AccountId { get; set; } public sbyte Sign { get; set; } public uint Version { get; set; } public int Score { get; set; } }
    }
}
