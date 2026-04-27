using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Engines;

/// <summary>
/// MV target = StripeLog. All columns share a single file with stripes; same
/// no-OPTIMIZE/no-FINAL constraint as Log/TinyLog.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvTargetStripeLogTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvTargetStripeLogTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task MergeTree_To_StripeLog_PassThrough()
    {
        await using var ctx = TestContextFactory.Create<MtToStripeLog.Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new MtToStripeLog.Row { Id = 1, Bucket = "p", Hits = 1 },
            new MtToStripeLog.Row { Id = 2, Bucket = "p", Hits = 4 },
            new MtToStripeLog.Row { Id = 3, Bucket = "q", Hits = 7 });
        await ctx.SaveChangesAsync();

        Assert.Equal(3UL, await RawClickHouse.RowCountAsync(Conn, "MtToStripeLogTarget"));

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Bucket, toInt64(Hits) AS Hits FROM \"MtToStripeLogTarget\" ORDER BY Bucket, Hits");
        Assert.Equal(3, rows.Count);
        Assert.Equal("p", (string)rows[0]["Bucket"]!); Assert.Equal(1L, Convert.ToInt64(rows[0]["Hits"]));
        Assert.Equal("p", (string)rows[1]["Bucket"]!); Assert.Equal(4L, Convert.ToInt64(rows[1]["Hits"]));
        Assert.Equal("q", (string)rows[2]["Bucket"]!); Assert.Equal(7L, Convert.ToInt64(rows[2]["Hits"]));
    }

    public static class MtToStripeLog
    {
        public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
        {
            public DbSet<Row> Source => Set<Row>();
            public DbSet<Target> Target => Set<Target>();
            protected override void OnModelCreating(ModelBuilder mb)
            {
                mb.Entity<Row>(e => { e.ToTable("MtToStripeLogSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
                mb.Entity<Target>(e =>
                {
                    e.ToTable("MtToStripeLogTarget"); e.HasNoKey();
                    e.UseStripeLogEngine();

                });
                mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows
                        .Select(r => new Target { Bucket = r.Bucket, Hits = r.Hits }));
            }
        }
        public class Row { public long Id { get; set; } public string Bucket { get; set; } = ""; public long Hits { get; set; } }
        public class Target { public string Bucket { get; set; } = ""; public long Hits { get; set; } }
    }
}
