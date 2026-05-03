using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// <c>DateTime.DayOfWeek</c> wraps the ClickHouse <c>toDayOfWeek(x) % 7</c>
/// expression in <c>toInt32(...)</c> because ClickHouse's modulo result-type
/// inference returns the narrowest type that fits — which the driver's
/// <c>GetInt32</c> then refuses. The sibling <c>DateOnly.DayOfWeek</c> translator
/// dropped that wrap; this test pins the symmetry so the SQL emitted for both
/// member accesses contains the same explicit cast.
/// </summary>
public class DateOnlyDayOfWeekTranslationTests
{
    [Fact]
    public void DateOnly_DayOfWeek_InWhere_WrapsModuloIntoToInt32()
    {
        using var ctx = Create();
        var sql = ctx.Records.Where(r => r.Day.DayOfWeek == DayOfWeek.Monday).ToQueryString();

        // Find the segment that contains toDayOfWeek and assert the immediately
        // wrapping function is toInt32 — `Assert.Contains("toInt32")` would also
        // match a comparison-side cast that EF Core inserts for the
        // `== DayOfWeek.Monday` operand and miss the actual bug.
        var idx = sql.IndexOf("toDayOfWeek", StringComparison.Ordinal);
        Assert.True(idx >= 0, $"toDayOfWeek not found in: {sql}");

        // Look for `toInt32(toDayOfWeek(...) % 7)` — the wrap must happen on the
        // server-side modulo of the toDayOfWeek result.
        Assert.Matches(@"toInt32\(\s*toDayOfWeek\(", sql);
    }

    [Fact]
    public void DateOnly_DayOfWeek_InProjection_WrapsModuloIntoToInt32()
    {
        using var ctx = Create();
        var sql = ctx.Records.Select(r => r.Day.DayOfWeek).ToQueryString();
        Assert.True(System.Text.RegularExpressions.Regex.IsMatch(sql, @"toInt32\(\s*toDayOfWeek\("),
            $"actual SQL: {sql}");
    }

    [Fact]
    public void DateTime_DayOfWeek_RegressionPin_StillWrapsToInt32()
    {
        using var ctx = Create();
        var sql = ctx.Records.Where(r => r.Ts.DayOfWeek == DayOfWeek.Monday).ToQueryString();
        Assert.Matches(@"toInt32\(\s*toDayOfWeek\(", sql);
    }

    private static DowCtx Create() =>
        new(new DbContextOptionsBuilder<DowCtx>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options);

    public sealed class DowRecord
    {
        public Guid Id { get; set; }
        public DateOnly Day { get; set; }
        public DateTime Ts { get; set; }
    }

    public sealed class DowCtx(DbContextOptions<DowCtx> o) : DbContext(o)
    {
        public DbSet<DowRecord> Records => Set<DowRecord>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<DowRecord>(e =>
            {
                e.ToTable("dow_records");
                e.HasKey(x => x.Id);
            });
    }
}
