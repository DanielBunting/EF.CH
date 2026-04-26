using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Nullable DateTime member access and ClickHouseFunctions helpers should
/// propagate NULL through the translation. Risk: nullable wrappers add an
/// extra Nullable&lt;T&gt; layer the cast/translator chain may strip incorrectly.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class NullableDateFunctionTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public NullableDateFunctionTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task NullableDateTimeMembers_RoundTrip()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var t = new DateTime(2024, 6, 15, 13, 47, 23, DateTimeKind.Utc);
        ctx.Rows.Add(new Row { Id = 1, T = t });
        ctx.Rows.Add(new Row { Id = 2, T = null });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var rows = await ctx.Rows.OrderBy(r => r.Id).Select(r => new
        {
            r.Id,
            Year = r.T == null ? (int?)null : r.T.Value.Year,
            Month = r.T == null ? (int?)null : r.T.Value.Month,
            Hour = r.T == null ? (int?)null : r.T.Value.Hour,
        }).ToListAsync();

        Assert.Equal(2024, rows[0].Year);
        Assert.Equal(6, rows[0].Month);
        Assert.Equal(13, rows[0].Hour);
        Assert.Null(rows[1].Year);
        Assert.Null(rows[1].Month);
        Assert.Null(rows[1].Hour);
    }

    [Fact]
    public async Task NullableClickHouseFunctionsDateHelpers_RoundTrip()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var t = new DateTime(2024, 6, 15, 13, 47, 23, DateTimeKind.Utc);
        ctx.Rows.Add(new Row { Id = 1, T = t });
        ctx.Rows.Add(new Row { Id = 2, T = null });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var rows = await ctx.Rows.OrderBy(r => r.Id).Select(r => new
        {
            r.Id,
            Yyyymm = r.T.HasValue ? (int?)r.T.Value.ToYYYYMM() : null,
            Quarter = r.T.HasValue ? (int?)r.T.Value.ToQuarter() : null,
        }).ToListAsync();

        Assert.Equal(202406, rows[0].Yyyymm);
        Assert.Equal(2, rows[0].Quarter);
        Assert.Null(rows[1].Yyyymm);
        Assert.Null(rows[1].Quarter);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public DateTime? T { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("NullableDateRows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.T).IsRequired(false);
            });
    }
}
