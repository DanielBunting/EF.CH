using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Types;

/// <summary>
/// Round-trip for <c>TimeSpan</c> values stored against an Int64 column
/// (typical CH analytics encoding for elapsed durations).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class TimeAndTimeSpanRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public TimeAndTimeSpanRoundTripTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task TimeSpan_AsTicks_RoundTrips()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var span = TimeSpan.FromMinutes(42) + TimeSpan.FromSeconds(7);
        ctx.Rows.Add(new Row { Id = 1, Duration = span });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var read = await ctx.Rows.SingleAsync(r => r.Id == 1);
        Assert.Equal(span, read.Duration);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public TimeSpan Duration { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("TimeSpanRoundTrip_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
