using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.ValueTypes;

/// <summary>
/// MV pass-through for a <c>Nested(...)</c> column. Defined via raw SQL because the
/// LINQ MV translator does not know about the parallel-array shape Nested produces;
/// the source side is still configured fluently with <c>HasNested</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvNestedTypeTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvNestedTypeTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Nested_PassesThrough_AsParallelArrays()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Sources.Add(new Src
        {
            Id = 1,
            Players = new List<Player>
            {
                new() { Name = "alice", Score = 10 },
                new() { Name = "bob",   Score = 20 },
            },
        });
        ctx.Sources.Add(new Src
        {
            Id = 2,
            Players = new List<Player> { new() { Name = "carol", Score = 99 } },
        });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvNestedTarget");

        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "MvNestedTarget"));

        var sumScores = await RawClickHouse.ScalarAsync<long>(Conn,
            "SELECT toInt64(sum(arraySum(\"Players.Score\"))) FROM \"MvNestedTarget\"");
        Assert.Equal(129L, sumScores);
    }

    public sealed class Player { public string Name { get; set; } = ""; public uint Score { get; set; } }

    public sealed class Src
    {
        public ulong Id { get; set; }
        public List<Player> Players { get; set; } = new();
    }

    public sealed class Tgt
    {
        public ulong Id { get; set; }
        public List<Player> Players { get; set; } = new();
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e =>
            {
                e.ToTable("MvNestedSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.HasNested<Src, Player>(x => x.Players);
            });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvNestedTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.Id);
                e.HasNested<Tgt, Player>(x => x.Players);
                // Nested produces parallel-array columns ("Players.Name", "Players.Score") that
                // the LINQ translator can't address — use raw SQL for the SELECT.
                e.AsMaterializedViewRaw(
                    sourceTable: "MvNestedSource",
                    selectSql: """
                    SELECT Id, "Players.Name" AS "Players.Name", "Players.Score" AS "Players.Score"
                    FROM "MvNestedSource"
                    """);
            });
        }
    }
}
