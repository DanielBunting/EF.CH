using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Engines;

/// <summary>
/// MV target = Log engine. Slightly bigger than TinyLog (separate marks file
/// for parallel reads), but otherwise has the same write/read semantics for
/// MV destinations: append-only and no FINAL/OPTIMIZE.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvTargetLogTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvTargetLogTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task MergeTree_To_Log_PassThrough()
    {
        await using var ctx = TestContextFactory.Create<MtToLog.Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new MtToLog.Row { Id = 1, Code = "x", N = 1 },
            new MtToLog.Row { Id = 2, Code = "y", N = 2 },
            new MtToLog.Row { Id = 3, Code = "z", N = 3 });
        await ctx.SaveChangesAsync();

        Assert.Equal(3UL, await RawClickHouse.RowCountAsync(Conn, "MtToLogTarget"));

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Code, toInt64(N) AS N FROM \"MtToLogTarget\" ORDER BY N");
        Assert.Equal(new[] { 1L, 2L, 3L }, rows.Select(r => Convert.ToInt64(r["N"])).ToArray());
    }

    public static class MtToLog
    {
        public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
        {
            public DbSet<Row> Source => Set<Row>();
            public DbSet<Target> Target => Set<Target>();
            protected override void OnModelCreating(ModelBuilder mb)
            {
                mb.Entity<Row>(e => { e.ToTable("MtToLogSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
                mb.Entity<Target>(e =>
                {
                    e.ToTable("MtToLogTarget"); e.HasNoKey();
                    e.UseLogEngine();
                    e.AsMaterializedView<Target, Row>(rows => rows
                        .Select(r => new Target { Code = r.Code, N = r.N }));
                });
            }
        }
        public class Row { public long Id { get; set; } public string Code { get; set; } = ""; public long N { get; set; } }
        public class Target { public string Code { get; set; } = ""; public long N { get; set; } }
    }
}
