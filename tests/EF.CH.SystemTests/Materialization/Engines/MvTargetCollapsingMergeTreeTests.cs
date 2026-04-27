using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Engines;

/// <summary>
/// MV target = CollapsingMergeTree(Sign). The MV pipes a Sign-bearing source
/// through unchanged; OPTIMIZE FINAL on the target then cancels paired +1/-1
/// rows leaving the net survivor.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvTargetCollapsingMergeTreeTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvTargetCollapsingMergeTreeTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task MergeTree_To_CollapsingMergeTree_Sign()
    {
        await using var ctx = TestContextFactory.Create<MtToCollapsing.Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new MtToCollapsing.Row { Id = 1, AccountId = 1, Balance = 100, Sign =  1 },
            new MtToCollapsing.Row { Id = 2, AccountId = 1, Balance = 100, Sign = -1 },
            new MtToCollapsing.Row { Id = 3, AccountId = 1, Balance = 200, Sign =  1 },
            new MtToCollapsing.Row { Id = 4, AccountId = 2, Balance =  50, Sign =  1 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MtToCollapsingTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT AccountId, toFloat64(Balance) AS Balance FROM \"MtToCollapsingTarget\" FINAL ORDER BY AccountId");
        Assert.Equal(2, rows.Count);
        Assert.Equal(1L, Convert.ToInt64(rows[0]["AccountId"])); Assert.Equal(200.0, Convert.ToDouble(rows[0]["Balance"]));
        Assert.Equal(2L, Convert.ToInt64(rows[1]["AccountId"])); Assert.Equal( 50.0, Convert.ToDouble(rows[1]["Balance"]));
    }

    [Fact]
    public async Task NullEngine_To_CollapsingMergeTree_Sign()
    {
        await using var ctx = TestContextFactory.Create<NullToCollapsing.Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"NullCollapsingIngest\" (\"AccountId\", \"Balance\", \"Sign\") VALUES (1, 100, 1), (1, 100, -1), (1, 200, 1), (2, 50, 1)");

        await RawClickHouse.SettleMaterializationAsync(Conn, "NullToCollapsingTarget");
        Assert.Equal(0UL, await RawClickHouse.RowCountAsync(Conn, "NullCollapsingIngest"));

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT AccountId, toFloat64(Balance) AS Balance FROM \"NullToCollapsingTarget\" FINAL ORDER BY AccountId");
        Assert.Equal(2, rows.Count);
        Assert.Equal(200.0, Convert.ToDouble(rows[0]["Balance"]));
        Assert.Equal( 50.0, Convert.ToDouble(rows[1]["Balance"]));
    }

    public static class MtToCollapsing
    {
        public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
        {
            public DbSet<Row> Source => Set<Row>();
            public DbSet<Target> Target => Set<Target>();
            protected override void OnModelCreating(ModelBuilder mb)
            {
                mb.Entity<Row>(e => { e.ToTable("MtToCollapsingSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
                mb.Entity<Target>(e =>
                {
                    e.ToTable("MtToCollapsingTarget"); e.HasNoKey();
                    e.UseCollapsingMergeTree("Sign", "AccountId");

                });
                mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows
                        .Select(r => new Target { AccountId = r.AccountId, Balance = r.Balance, Sign = r.Sign }));
            }
        }
        public class Row { public long Id { get; set; } public long AccountId { get; set; } public double Balance { get; set; } public sbyte Sign { get; set; } }
        public class Target { public long AccountId { get; set; } public double Balance { get; set; } public sbyte Sign { get; set; } }
    }

    public static class NullToCollapsing
    {
        public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
        {
            public DbSet<Row> Ingest => Set<Row>();
            public DbSet<Target> Target => Set<Target>();
            protected override void OnModelCreating(ModelBuilder mb)
            {
                mb.Entity<Row>(e => { e.ToTable("NullCollapsingIngest"); e.HasNoKey(); e.UseNullEngine(); });
                mb.Entity<Target>(e =>
                {
                    e.ToTable("NullToCollapsingTarget"); e.HasNoKey();
                    e.UseCollapsingMergeTree("Sign", "AccountId");

                });
                mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows
                        .Select(r => new Target { AccountId = r.AccountId, Balance = r.Balance, Sign = r.Sign }));
            }
        }
        public class Row { public long AccountId { get; set; } public double Balance { get; set; } public sbyte Sign { get; set; } }
        public class Target { public long AccountId { get; set; } public double Balance { get; set; } public sbyte Sign { get; set; } }
    }
}
