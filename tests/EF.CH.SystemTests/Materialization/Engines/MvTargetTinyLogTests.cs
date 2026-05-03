using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Engines;

/// <summary>
/// MV target = TinyLog. Log family engines accept MV pushes but do not support
/// OPTIMIZE FINAL or FINAL reads, so verification reads the target directly.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvTargetTinyLogTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvTargetTinyLogTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task MergeTree_To_TinyLog_PassThrough()
    {
        await using var ctx = TestContextFactory.Create<MtToTinyLog.Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new MtToTinyLog.Row { Id = 1, Level = "info",  Message = "a" },
            new MtToTinyLog.Row { Id = 2, Level = "warn",  Message = "b" },
            new MtToTinyLog.Row { Id = 3, Level = "error", Message = "c" });
        await ctx.SaveChangesAsync();

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Level, Message FROM \"MtToTinyLogTarget\" ORDER BY Message");
        Assert.Equal(3, rows.Count);
        Assert.Equal("a", (string)rows[0]["Message"]!);
        Assert.Equal("b", (string)rows[1]["Message"]!);
        Assert.Equal("c", (string)rows[2]["Message"]!);
    }

    public static class MtToTinyLog
    {
        public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
        {
            public DbSet<Row> Source => Set<Row>();
            public DbSet<Target> Target => Set<Target>();
            protected override void OnModelCreating(ModelBuilder mb)
            {
                mb.Entity<Row>(e => { e.ToTable("MtToTinyLogSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
                mb.Entity<Target>(e =>
                {
                    e.ToTable("MtToTinyLogTarget"); e.HasNoKey();
                    e.UseTinyLogEngine();

                });
                mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows
                        .Select(r => new Target { Level = r.Level, Message = r.Message }));
            }
        }
        public class Row { public long Id { get; set; } public string Level { get; set; } = ""; public string Message { get; set; } = ""; }
        public class Target { public string Level { get; set; } = ""; public string Message { get; set; } = ""; }
    }
}
