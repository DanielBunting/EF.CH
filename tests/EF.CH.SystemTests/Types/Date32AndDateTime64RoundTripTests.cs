using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Types;

/// <summary>
/// Round-trip for <c>Date32</c> and <c>DateTime64(P)</c> at multiple precisions.
/// Date32 supports a wider range than Date; DateTime64(P) supports sub-second precision.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class Date32AndDateTime64RoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public Date32AndDateTime64RoundTripTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task Date32_AndDateTime64_AtVariousPrecisions_RoundTrip()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var d = new DateTime(1900, 1, 15, 0, 0, 0, DateTimeKind.Utc);
        var t = new DateTime(2024, 6, 15, 12, 34, 56, 789, DateTimeKind.Utc);
        ctx.Rows.Add(new Row { Id = 1, Day = d, Ms = t, Micro = t, Nano = t });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var read = await ctx.Rows.SingleAsync(r => r.Id == 1);
        Assert.Equal(d.Date, read.Day.Date);
        Assert.Equal(t, read.Ms);

        var tDay = await RawClickHouse.ColumnTypeAsync(Conn, "Date32RoundTrip_Rows", "Day");
        var tMs  = await RawClickHouse.ColumnTypeAsync(Conn, "Date32RoundTrip_Rows", "Ms");
        var tMic = await RawClickHouse.ColumnTypeAsync(Conn, "Date32RoundTrip_Rows", "Micro");
        var tNs  = await RawClickHouse.ColumnTypeAsync(Conn, "Date32RoundTrip_Rows", "Nano");
        Assert.Equal("Date32", tDay);
        Assert.StartsWith("DateTime64(3", tMs);
        Assert.StartsWith("DateTime64(6", tMic);
        Assert.StartsWith("DateTime64(9", tNs);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public DateTime Day { get; set; }
        public DateTime Ms { get; set; }
        public DateTime Micro { get; set; }
        public DateTime Nano { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("Date32RoundTrip_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Day).HasColumnType("Date32");
                e.Property(x => x.Ms).HasColumnType("DateTime64(3, 'UTC')");
                e.Property(x => x.Micro).HasColumnType("DateTime64(6, 'UTC')");
                e.Property(x => x.Nano).HasColumnType("DateTime64(9, 'UTC')");
            });
    }
}
