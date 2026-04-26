using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.ValueTypes;

/// <summary>MV pass-through for <c>Map(String, Int32)</c>.</summary>
[Collection(SingleNodeCollection.Name)]
public class MvMapRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvMapRoundTripTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Map_PassesThrough()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Sources.AddRange(
            new Src { Id = 1, Counts = new() { ["a"] = 1, ["b"] = 2 }, N = 1 },
            new Src { Id = 2, Counts = new() { ["x"] = 9 },             N = 2 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvMapTarget");

        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "MvMapTarget"));
        Assert.Equal("Map(String, Int32)", await RawClickHouse.ColumnTypeAsync(Conn, "MvMapTarget", "Counts"));
    }

    public sealed class Src { public uint Id { get; set; } public Dictionary<string, int> Counts { get; set; } = new(); public long N { get; set; } }
    public sealed class Tgt { public Dictionary<string, int> Counts { get; set; } = new(); public long N { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e =>
            {
                e.ToTable("MvMapSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Counts).HasColumnType("Map(String, Int32)");
            });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvMapTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.N);
                e.Property(x => x.Counts).HasColumnType("Map(String, Int32)");
                e.AsMaterializedView<Tgt, Src>(rows => rows
                    .Select(r => new Tgt { Counts = r.Counts, N = r.N }));
            });
        }
    }
}
