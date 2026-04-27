using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.ValueTypes;

/// <summary>MV pass-through for <c>TimeSpan</c> stored as ticks (Int64).</summary>
[Collection(SingleNodeCollection.Name)]
public class MvTimeSpanRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvTimeSpanRoundTripTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task TimeSpan_PassesThrough()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Sources.AddRange(
            new Src { Id = 1, Duration = TimeSpan.FromMinutes(5),  N = 1 },
            new Src { Id = 2, Duration = TimeSpan.FromHours(2),    N = 2 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvTimeSpanTarget");

        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "MvTimeSpanTarget"));
    }

    public sealed class Src { public uint Id { get; set; } public TimeSpan Duration { get; set; } public long N { get; set; } }
    public sealed class Tgt { public TimeSpan Duration { get; set; } public long N { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e => { e.ToTable("MvTimeSpanSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvTimeSpanTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.N);

            });
            mb.MaterializedView<Tgt>().From<Src>().DefinedAs(rows => rows
                    .Select(r => new Tgt { Duration = r.Duration, N = r.N }));
        }
    }
}
