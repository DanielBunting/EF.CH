using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// DateOnly member access path is structurally identical to DateTime — both
/// translators wrap <c>toMonth</c>/<c>toYear</c> in <c>Convert(Function, int)</c>
/// which is a SQL no-op when source mapping is already int. Verifies the same
/// fix applied to DateTime members extends to DateOnly.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class DateOnlyMemberTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public DateOnlyMemberTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task DateOnlyMembers_RoundTripWithDeclaredClrTypes()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Rows.Add(new Row { Id = 1, D = new DateOnly(2024, 6, 15) });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var row = await ctx.Rows.Select(r => new
        {
            Year = r.D.Year,
            Month = r.D.Month,
            Day = r.D.Day,
            DayOfYear = r.D.DayOfYear,
        }).FirstAsync();

        Assert.Equal(2024, row.Year);
        Assert.Equal(6, row.Month);
        Assert.Equal(15, row.Day);
        Assert.Equal(new DateOnly(2024, 6, 15).DayOfYear, row.DayOfYear);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public DateOnly D { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("DateOnlyRows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
