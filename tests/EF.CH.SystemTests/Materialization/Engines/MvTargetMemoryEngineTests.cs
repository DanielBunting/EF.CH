using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Engines;

/// <summary>
/// MV target = Memory engine. Memory accepts MV pushes but is non-persistent and
/// does not support OPTIMIZE FINAL or FINAL reads, so verification is direct
/// SELECT against the target table.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvTargetMemoryEngineTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvTargetMemoryEngineTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task MergeTree_To_Memory_PassThrough()
    {
        await using var ctx = TestContextFactory.Create<PassThrough.Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new PassThrough.Row { Id = 1, Tag = "a", Value = 10 },
            new PassThrough.Row { Id = 2, Tag = "a", Value = 20 },
            new PassThrough.Row { Id = 3, Tag = "b", Value = 30 });
        await ctx.SaveChangesAsync();

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Tag, toInt64(Value) AS Value FROM \"MvMemoryPassThroughTarget\" ORDER BY Tag, Value");
        Assert.Equal(3, rows.Count);
        Assert.Equal("a", (string)rows[0]["Tag"]!); Assert.Equal(10L, Convert.ToInt64(rows[0]["Value"]));
        Assert.Equal("a", (string)rows[1]["Tag"]!); Assert.Equal(20L, Convert.ToInt64(rows[1]["Value"]));
        Assert.Equal("b", (string)rows[2]["Tag"]!); Assert.Equal(30L, Convert.ToInt64(rows[2]["Value"]));
    }

    [Fact]
    public async Task MergeTree_To_Memory_Aggregated()
    {
        await using var ctx = TestContextFactory.Create<Aggregated.Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new Aggregated.Row { Id = 1, Tag = "a", Value = 10 },
            new Aggregated.Row { Id = 2, Tag = "a", Value = 20 },
            new Aggregated.Row { Id = 3, Tag = "b", Value = 30 });
        await ctx.SaveChangesAsync();

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Tag, toInt64(Total) AS Total FROM \"MvMemoryAggregatedTarget\" ORDER BY Tag");
        Assert.Equal(2, rows.Count);
        Assert.Equal("a", (string)rows[0]["Tag"]!); Assert.Equal(30L, Convert.ToInt64(rows[0]["Total"]));
        Assert.Equal("b", (string)rows[1]["Tag"]!); Assert.Equal(30L, Convert.ToInt64(rows[1]["Total"]));
    }

    public static class PassThrough
    {
        public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
        {
            public DbSet<Row> Source => Set<Row>();
            public DbSet<Target> Target => Set<Target>();
            protected override void OnModelCreating(ModelBuilder mb)
            {
                mb.Entity<Row>(e => { e.ToTable("MvMemoryPassThroughSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
                mb.Entity<Target>(e =>
                {
                    e.ToTable("MvMemoryPassThroughTarget"); e.HasNoKey();
                    e.UseMemoryEngine();
                    e.AsMaterializedView<Target, Row>(rows => rows
                        .Select(r => new Target { Tag = r.Tag, Value = r.Value }));
                });
            }
        }
        public class Row { public long Id { get; set; } public string Tag { get; set; } = ""; public long Value { get; set; } }
        public class Target { public string Tag { get; set; } = ""; public long Value { get; set; } }
    }

    public static class Aggregated
    {
        public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
        {
            public DbSet<Row> Source => Set<Row>();
            public DbSet<Target> Target => Set<Target>();
            protected override void OnModelCreating(ModelBuilder mb)
            {
                mb.Entity<Row>(e => { e.ToTable("MvMemoryAggregatedSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
                mb.Entity<Target>(e =>
                {
                    e.ToTable("MvMemoryAggregatedTarget"); e.HasNoKey();
                    e.UseMemoryEngine();
                    e.AsMaterializedView<Target, Row>(rows => rows
                        .GroupBy(r => r.Tag)
                        .Select(g => new Target { Tag = g.Key, Total = g.Sum(r => r.Value) }));
                });
            }
        }
        public class Row { public long Id { get; set; } public string Tag { get; set; } = ""; public long Value { get; set; } }
        public class Target { public string Tag { get; set; } = ""; public long Total { get; set; } }
    }
}
