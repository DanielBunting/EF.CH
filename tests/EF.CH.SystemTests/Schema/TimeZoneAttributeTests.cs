using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// <c>HasTimeZone(...)</c> emits <c>DateTime64(P, 'TZ')</c> column types. We assert
/// the rendered ClickHouse type carries the timezone literal.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class TimeZoneAttributeTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public TimeZoneAttributeTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task HasTimeZone_EmitsExactDateTime64WithZone_AndRoundTripsInstant()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var t = new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.FromHours(1));
        ctx.Rows.Add(new Row { Id = 1, At = t });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var read = await ctx.Rows.SingleAsync(r => r.Id == 1);
        // Compare the absolute instant (UtcDateTime); the offset rendering is up to
        // ClickHouse's TZ presentation, but the represented point in time must match.
        Assert.Equal(t.UtcDateTime, read.At.UtcDateTime);

        // Exact rendered column type — DateTime64(P, 'TZ'). HasTimeZone defaults precision
        // to whatever the provider chose; assert the zone literal appears in canonical form.
        var type = await RawClickHouse.ColumnTypeAsync(Conn, "TimeZoneAttr_Rows", "At");
        Assert.Matches(@"^DateTime64\(\d+, 'Europe/London'\)$", type);
    }

    /// <summary>
    /// Spring-forward boundary: at 01:30 local time on 2024-03-31,
    /// Europe/London jumps from GMT to BST so that local instant doesn't
    /// exist. The point in time is unambiguous when expressed as a
    /// DateTimeOffset (UTC=01:30 → BST would be 02:30 local). Round-tripping
    /// must preserve the absolute instant.
    /// <para>
    /// EXPECTED TO FAIL TODAY: ClickHouse.Driver 1.0.0's bind path renders
    /// DateTime values as a wall-clock string in the column's timezone via
    /// <c>FormatDateTime64InTargetTimezone</c>. On the spring-forward day
    /// the wall-clock string for the UTC instant lands in the "skipped"
    /// hour; CH then parses the ambiguous local string with the GMT offset,
    /// shifting the stored instant by one hour. Provider-side fix would
    /// require either driver-level UTC-binding (DateTime64UTC route) or
    /// SQL-level wrapping of every parameter binding with
    /// <c>fromUnixTimestamp64Milli(?, 'TZ')</c>. Tracked in
    /// <c>.tmp/notes/known-issues.md</c>.
    /// </para>
    /// </summary>
    [Fact]
    public async Task HasTimeZone_RoundTripsAcrossSpringForwardBoundary()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // 2024-03-31 01:30 UTC corresponds to 02:30 BST locally — the hour
        // 01:00–02:00 local does not exist. Use the UTC instant directly so
        // there's no .NET-side ambiguity over which local representation to
        // pick.
        var instant = new DateTimeOffset(2024, 3, 31, 1, 30, 0, TimeSpan.Zero);
        ctx.Rows.Add(new Row { Id = 1, At = instant });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var read = await ctx.Rows.SingleAsync(r => r.Id == 1);
        Assert.Equal(instant.UtcDateTime, read.At.UtcDateTime);
    }

    /// <summary>
    /// Fall-back boundary: at 01:30 local time on 2024-10-27, Europe/London
    /// shifts from BST back to GMT and the local instant 01:30 occurs twice.
    /// The DateTimeOffset carries an explicit UTC offset, so the absolute
    /// instant is unambiguous in principle; round-trip must preserve it.
    /// </summary>
    [Fact]
    public async Task HasTimeZone_RoundTripsAcrossFallBackBoundary()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var firstInstance  = new DateTimeOffset(2024, 10, 27, 0, 30, 0, TimeSpan.Zero);
        var secondInstance = new DateTimeOffset(2024, 10, 27, 1, 30, 0, TimeSpan.Zero);
        ctx.Rows.Add(new Row { Id = 1, At = firstInstance });
        ctx.Rows.Add(new Row { Id = 2, At = secondInstance });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var rows = await ctx.Rows.OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(firstInstance.UtcDateTime,  rows[0].At.UtcDateTime);
        Assert.Equal(secondInstance.UtcDateTime, rows[1].At.UtcDateTime);
        Assert.NotEqual(rows[0].At.UtcDateTime, rows[1].At.UtcDateTime);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public DateTimeOffset At { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("TimeZoneAttr_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.At).HasTimeZone("Europe/London");
            });
    }
}
