using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// <c>DateTime.TimeOfDay</c> previously returned <c>null</c> from the translator,
/// which caused EF Core to silently fall back to client-side evaluation — the
/// whole table is downloaded and filtered in-process. We catch this at translation
/// time so users discover the limitation immediately and choose a server-side
/// alternative (<c>.Hour</c>, <c>.Minute</c>, <c>toHour()</c> via raw SQL, etc.).
/// </summary>
public class DateTimeTimeOfDayTranslationTests
{
    [Fact]
    public void TimeOfDay_InWhere_ThrowsTranslationError()
    {
        using var ctx = Create();
        var query = ctx.Records.Where(r => r.Ts.TimeOfDay > TimeSpan.FromHours(12));

        var ex = Assert.Throws<InvalidOperationException>(() => query.ToQueryString());
        Assert.Contains("TimeOfDay", ex.Message);
    }

    [Fact]
    public void Hour_InWhere_StillTranslates()
    {
        using var ctx = Create();
        var sql = ctx.Records.Where(r => r.Ts.Hour > 12).ToQueryString();
        Assert.Contains("toHour", sql);
    }

    private static TimeOfDayCtx Create() =>
        new(new DbContextOptionsBuilder<TimeOfDayCtx>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options);

    public sealed class TimeOfDayRecord { public Guid Id { get; set; } public DateTime Ts { get; set; } }

    public sealed class TimeOfDayCtx(DbContextOptions<TimeOfDayCtx> o) : DbContext(o)
    {
        public DbSet<TimeOfDayRecord> Records => Set<TimeOfDayRecord>();
        protected override void OnModelCreating(ModelBuilder mb)
            => mb.Entity<TimeOfDayRecord>(e =>
            {
                e.ToTable("tod_records");
                e.HasKey(x => x.Id);
            });
    }
}
