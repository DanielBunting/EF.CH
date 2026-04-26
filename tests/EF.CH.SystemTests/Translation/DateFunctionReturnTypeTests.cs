using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Round-trips every DateTime member access and ClickHouseFunctions date helper
/// through the LINQ pipeline. The risk class is "C# wrapper declares a wider
/// type than ClickHouse natively returns" (e.g. ClickHouse <c>toMonth</c> is
/// <c>UInt8</c>, but the C# member projects an <c>int</c>) — without a server-
/// side cast the driver throws <c>InvalidCastException</c> at projection time.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class DateFunctionReturnTypeTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public DateFunctionReturnTypeTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task DateTimeMembers_RoundTripWithDeclaredClrTypes()
    {
        await using var ctx = TestContextFactory.Create<DateCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var t = new DateTime(2024, 6, 15, 13, 47, 23, DateTimeKind.Utc).AddMilliseconds(500);
        ctx.Events.Add(new DateRow { Id = 1, T = t });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var q = ctx.Events
            .Select(e => new
            {
                Year = e.T.Year,
                Month = e.T.Month,
                Day = e.T.Day,
                Hour = e.T.Hour,
                Minute = e.T.Minute,
                Second = e.T.Second,
                DayOfYear = e.T.DayOfYear,
                DayOfWeek = (int)e.T.DayOfWeek,
            });
        var sql = q.ToQueryString();
        Console.WriteLine("[SQL] " + sql);
        Assert.Contains("toInt32", sql);
        var row = await q.FirstAsync();

        Assert.Equal(2024, row.Year);
        Assert.Equal(6, row.Month);
        Assert.Equal(15, row.Day);
        Assert.Equal(13, row.Hour);
        Assert.Equal(47, row.Minute);
        Assert.Equal(23, row.Second);
        Assert.Equal(t.DayOfYear, row.DayOfYear);
        Assert.Equal((int)t.DayOfWeek, row.DayOfWeek);
    }

    [Fact]
    public async Task ClickHouseFunctionsDateHelpers_RoundTripWithDeclaredClrTypes()
    {
        await using var ctx = TestContextFactory.Create<DateCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var t = new DateTime(2024, 6, 15, 13, 47, 23, DateTimeKind.Utc);
        ctx.Events.Add(new DateRow { Id = 1, T = t });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var row = await ctx.Events
            .Select(e => new
            {
                Yyyymm = e.T.ToYYYYMM(),
                Yyyymmdd = e.T.ToYYYYMMDD(),
                DayOfWeek = e.T.ToDayOfWeek(),
                DayOfYear = e.T.ToDayOfYear(),
                Quarter = e.T.ToQuarter(),
            })
            .FirstAsync();

        Assert.Equal(202406, row.Yyyymm);
        Assert.Equal(20240615, row.Yyyymmdd);
        Assert.InRange(row.DayOfWeek, 1, 7);
        Assert.Equal(t.DayOfYear, row.DayOfYear);
        Assert.Equal(2, row.Quarter);
    }

    public sealed class DateRow
    {
        public uint Id { get; set; }
        public DateTime T { get; set; }
    }

    public sealed class DateCtx(DbContextOptions<DateCtx> o) : DbContext(o)
    {
        public DbSet<DateRow> Events => Set<DateRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<DateRow>(e =>
            {
                e.ToTable("DateFnRows");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
            });
    }
}
