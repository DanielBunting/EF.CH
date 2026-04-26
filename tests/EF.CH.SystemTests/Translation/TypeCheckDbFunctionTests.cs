using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Coverage of <c>ClickHouseTypeCheckDbFunctionsExtensions</c>: IsNaN, IsFinite, IsInfinite.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class TypeCheckDbFunctionTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public TypeCheckDbFunctionTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task IsNaN_IsFinite_IsInfinite_ClassifyValuesCorrectly()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(
            new Row { Id = 1, V = 1.0 },
            new Row { Id = 2, V = double.NaN },
            new Row { Id = 3, V = double.PositiveInfinity });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var classified = await ctx.Rows
            .OrderBy(r => r.Id)
            .Select(r => new
            {
                r.Id,
                IsNaN = EfClass.Functions.IsNaN(r.V),
                IsFinite = EfClass.Functions.IsFinite(r.V),
                IsInfinite = EfClass.Functions.IsInfinite(r.V),
            })
            .ToListAsync();

        Assert.False(classified[0].IsNaN); Assert.True(classified[0].IsFinite); Assert.False(classified[0].IsInfinite);
        Assert.True(classified[1].IsNaN);  Assert.False(classified[1].IsFinite); Assert.False(classified[1].IsInfinite);
        Assert.False(classified[2].IsNaN); Assert.False(classified[2].IsFinite); Assert.True(classified[2].IsInfinite);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public double V { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("TypeCheckFnCoverage_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
