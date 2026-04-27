using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.ValueTypes;

/// <summary>
/// MV round-trip for <c>DateTime64(P)</c> with millisecond precision. Verifies
/// sub-second values survive both the source insert and the MV-target column.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvDateTime64RoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvDateTime64RoundTripTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task DateTime64Ms_PassesThrough()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var t = new DateTime(2026, 4, 25, 10, 0, 0, 123, DateTimeKind.Utc);
        ctx.Sources.AddRange(
            new Src { Id = 1, At = t, N = 100 },
            new Src { Id = 2, At = t.AddMilliseconds(456), N = 200 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvDateTime64Target");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toString(At) AS At, toInt64(N) AS N FROM \"MvDateTime64Target\" ORDER BY N");
        Assert.Equal(2, rows.Count);
        // Source row 1: 10:00:00.123 → ".123"
        // Source row 2: 10:00:00.123 + 456 ms = 10:00:00.579 → ".579"
        Assert.EndsWith(".123", (string)rows[0]["At"]!);
        Assert.EndsWith(".579", (string)rows[1]["At"]!);

        Assert.StartsWith("DateTime64(3", await RawClickHouse.ColumnTypeAsync(Conn, "MvDateTime64Target", "At"));
    }

    public sealed class Src { public uint Id { get; set; } public DateTime At { get; set; } public long N { get; set; } }
    public sealed class Tgt { public DateTime At { get; set; } public long N { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e =>
            {
                e.ToTable("MvDateTime64Source"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.At).HasColumnType("DateTime64(3, 'UTC')");
            });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvDateTime64Target"); e.HasNoKey();
                e.UseMergeTree(x => x.N);
                e.Property(x => x.At).HasColumnType("DateTime64(3, 'UTC')");

            });
            mb.MaterializedView<Tgt>().From<Src>().DefinedAs(rows => rows
                    .Select(r => new Tgt { At = r.At, N = r.N }));
        }
    }
}
