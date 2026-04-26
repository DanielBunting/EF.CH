using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.ValueTypes;

/// <summary>
/// MV round-trip for <c>FixedString(N)</c>. Verifies the column type is preserved
/// through the MV target's CREATE statement (ClickHouse infers MV column types
/// from the SELECT, so the entity's declared FixedString must survive).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvFixedStringRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvFixedStringRoundTripTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task FixedString_PassesThroughMv()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Sources.AddRange(
            new Src { Id = 1, Code = "ABCDEFGH", N = 10 },
            new Src { Id = 2, Code = "abc",      N = 20 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvFixedStringTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Code, toInt64(N) AS N FROM \"MvFixedStringTarget\" ORDER BY N");
        Assert.Equal(2, rows.Count);
        Assert.Equal("ABCDEFGH", (string)rows[0]["Code"]!);
        Assert.StartsWith("abc", (string)rows[1]["Code"]!);
    }

    public sealed class Src { public uint Id { get; set; } public string Code { get; set; } = ""; public long N { get; set; } }
    public sealed class Tgt { public string Code { get; set; } = ""; public long N { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e =>
            {
                e.ToTable("MvFixedStringSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Code).HasColumnType("FixedString(8)");
            });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvFixedStringTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.N);
                e.Property(x => x.Code).HasColumnType("FixedString(8)");
                e.AsMaterializedView<Tgt, Src>(rows => rows
                    .Select(r => new Tgt { Code = r.Code, N = r.N }));
            });
        }
    }
}
