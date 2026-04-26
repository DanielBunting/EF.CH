using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.ValueTypes;

/// <summary>
/// MV pass-through for an <c>Enum8</c> column. The .NET enum is stored by name
/// (matching the convention from EnumRoundTripTests).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvEnum8RoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvEnum8RoundTripTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Enum8_PassesThrough()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Sources.AddRange(
            new Src { Id = 1, Status = Status.Open,    N = 1 },
            new Src { Id = 2, Status = Status.Pending, N = 2 },
            new Src { Id = 3, Status = Status.Closed,  N = 3 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvEnum8Target");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Status, toInt64(N) AS N FROM \"MvEnum8Target\" ORDER BY N");
        Assert.Equal(3, rows.Count);
        Assert.Equal("Open",    (string)rows[0]["Status"]!);
        Assert.Equal("Pending", (string)rows[1]["Status"]!);
        Assert.Equal("Closed",  (string)rows[2]["Status"]!);
    }

    public enum Status { Open, Pending, Closed }

    public sealed class Src { public uint Id { get; set; } public Status Status { get; set; } public long N { get; set; } }
    public sealed class Tgt { public Status Status { get; set; } public long N { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            const string enumColumn = "Enum8('Open' = 0, 'Pending' = 1, 'Closed' = 2)";
            mb.Entity<Src>(e =>
            {
                e.ToTable("MvEnum8Source"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Status).HasColumnType(enumColumn).HasConversion<string>();
            });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvEnum8Target"); e.HasNoKey();
                e.UseMergeTree(x => x.N);
                e.Property(x => x.Status).HasColumnType(enumColumn).HasConversion<string>();
                e.AsMaterializedView<Tgt, Src>(rows => rows
                    .Select(r => new Tgt { Status = r.Status, N = r.N }));
            });
        }
    }
}
