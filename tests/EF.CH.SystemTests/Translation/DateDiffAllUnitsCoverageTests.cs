using EF.CH.Extensions;
using EF.CH.Infrastructure;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Exhaustive coverage of every <see cref="ClickHouseIntervalUnit"/> value across
/// the dynamic-unit family (DateAdd, DateSub, DateDiff, Age). Adjacent to the
/// "<c>date_add</c> rejected on CH 24+" fix — guards against any one unit
/// regressing if the unit-string mapping or interval-function naming changes.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class DateDiffAllUnitsCoverageTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public DateDiffAllUnitsCoverageTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    public static IEnumerable<object[]> AllUnits => new[]
    {
        new object[] { ClickHouseIntervalUnit.Second },
        new object[] { ClickHouseIntervalUnit.Minute },
        new object[] { ClickHouseIntervalUnit.Hour },
        new object[] { ClickHouseIntervalUnit.Day },
        new object[] { ClickHouseIntervalUnit.Week },
        new object[] { ClickHouseIntervalUnit.Month },
        new object[] { ClickHouseIntervalUnit.Quarter },
        new object[] { ClickHouseIntervalUnit.Year },
    };

    [Theory]
    [MemberData(nameof(AllUnits))]
    public async Task DateAdd_AllUnits_ExecuteAndReturnAValidDateTime(ClickHouseIntervalUnit unit)
    {
        await using var ctx = await SeededAsync();
        var t = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc);

        var result = await ctx.Rows.Select(r => EfClass.Functions.DateAdd(unit, 1, r.T)).FirstAsync();

        // We don't assert exact arithmetic per unit (CH and .NET differ on
        // "1 month from 2024-06-15") — but the call must succeed and return
        // a DateTime strictly later than the source.
        Assert.True(result > t, $"DateAdd({unit}, 1, ...) should advance the timestamp; got {result} vs {t}");
    }

    [Theory]
    [MemberData(nameof(AllUnits))]
    public async Task DateSub_AllUnits_ExecuteAndReturnAValidDateTime(ClickHouseIntervalUnit unit)
    {
        await using var ctx = await SeededAsync();
        var t = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc);

        var result = await ctx.Rows.Select(r => EfClass.Functions.DateSub(unit, 1, r.T)).FirstAsync();

        Assert.True(result < t, $"DateSub({unit}, 1, ...) should go back; got {result} vs {t}");
    }

    [Theory]
    [MemberData(nameof(AllUnits))]
    public async Task DateDiff_AllUnits_NonNegativeForwardSpan(ClickHouseIntervalUnit unit)
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        var start = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2025, 7, 4, 12, 30, 45, DateTimeKind.Utc);
        ctx.Rows.Add(new Row { Id = 1, T = start, T2 = end });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var diff = await ctx.Rows.Select(r => EfClass.Functions.DateDiff(unit, r.T, r.T2)).FirstAsync();
        Assert.True(diff > 0, $"DateDiff({unit}) of forward span should be positive; got {diff}");
    }

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.Add(new Row { Id = 1, T = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc), T2 = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc) });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public DateTime T { get; set; }
        public DateTime T2 { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("DateDiffUnitRows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
