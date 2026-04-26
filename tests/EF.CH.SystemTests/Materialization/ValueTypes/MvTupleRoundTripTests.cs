using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.ValueTypes;

/// <summary>MV pass-through for <c>Tuple(String, Int32)</c>.</summary>
[Collection(SingleNodeCollection.Name)]
public class MvTupleRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvTupleRoundTripTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Tuple_PassesThrough()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Sources.AddRange(
            new Src { Id = 1, Pair = ("alpha", 42), N = 1 },
            new Src { Id = 2, Pair = ("beta",  99), N = 2 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvTupleTarget");

        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "MvTupleTarget"));
        Assert.Equal("Tuple(String, Int32)", await RawClickHouse.ColumnTypeAsync(Conn, "MvTupleTarget", "Pair"));
    }

    public sealed class Src { public uint Id { get; set; } public (string, int) Pair { get; set; } public long N { get; set; } }
    public sealed class Tgt { public (string, int) Pair { get; set; } public long N { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e =>
            {
                e.ToTable("MvTupleSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Pair).HasColumnType("Tuple(String, Int32)");
            });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvTupleTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.N);
                e.Property(x => x.Pair).HasColumnType("Tuple(String, Int32)");
                e.AsMaterializedView<Tgt, Src>(rows => rows
                    .Select(r => new Tgt { Pair = r.Pair, N = r.N }));
            });
        }
    }
}
