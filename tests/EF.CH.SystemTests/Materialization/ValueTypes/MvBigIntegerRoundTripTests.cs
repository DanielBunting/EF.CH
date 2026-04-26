using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.ValueTypes;

/// <summary>MV pass-through for native <c>Int128</c> / <c>UInt128</c> types.</summary>
[Collection(SingleNodeCollection.Name)]
public class MvBigIntegerRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvBigIntegerRoundTripTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Int128_AndUInt128_PassThrough()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var i = (Int128)123456789012345678L * 10;
        var u = (UInt128)123456789012345678UL * 100;
        ctx.Sources.AddRange(
            new Src { Id = 1, Big = i,    Big2 = u,    N = 1 },
            new Src { Id = 2, Big = i*2, Big2 = u*2, N = 2 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvBigIntTarget");

        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "MvBigIntTarget"));
        Assert.Equal("Int128",  await RawClickHouse.ColumnTypeAsync(Conn, "MvBigIntTarget", "Big"));
        Assert.Equal("UInt128", await RawClickHouse.ColumnTypeAsync(Conn, "MvBigIntTarget", "Big2"));
    }

    public sealed class Src { public uint Id { get; set; } public Int128 Big { get; set; } public UInt128 Big2 { get; set; } public long N { get; set; } }
    public sealed class Tgt { public Int128 Big { get; set; } public UInt128 Big2 { get; set; } public long N { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e => { e.ToTable("MvBigIntSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvBigIntTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.N);
                e.AsMaterializedView<Tgt, Src>(rows => rows
                    .Select(r => new Tgt { Big = r.Big, Big2 = r.Big2, N = r.N }));
            });
        }
    }
}
