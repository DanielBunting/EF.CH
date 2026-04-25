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
