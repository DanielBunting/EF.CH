using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Types;

/// <summary>
/// Round-trip for ClickHouse <c>Enum8</c> / <c>Enum16</c> columns mapped from a .NET enum
/// stored as the enum's string name.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class EnumRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public EnumRoundTripTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task Enum8_StoresAndReadsLiteralName()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Rows.AddRange(
            new Row { Id = 1, Status8 = Small.Open,    Status16 = Big.Created },
            new Row { Id = 2, Status8 = Small.Closed,  Status16 = Big.Archived });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var rows = await ctx.Rows.OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(Small.Open, rows[0].Status8);
        Assert.Equal(Big.Created, rows[0].Status16);
        Assert.Equal(Small.Closed, rows[1].Status8);
        Assert.Equal(Big.Archived, rows[1].Status16);
    }

    public enum Small { Open, Pending, Closed }
    public enum Big { Created, Updated, Archived }

    public sealed class Row
    {
        public uint Id { get; set; }
        public Small Status8 { get; set; }
        public Big Status16 { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("EnumRoundTrip_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Status8).HasColumnType("Enum8('Open' = 0, 'Pending' = 1, 'Closed' = 2)").HasConversion<string>();
                e.Property(x => x.Status16).HasColumnType("Enum16('Created' = 0, 'Updated' = 1, 'Archived' = 2)").HasConversion<string>();
            });
    }
}
