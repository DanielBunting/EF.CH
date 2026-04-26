using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Coverage of <c>ClickHouseNullDbFunctionsExtensions</c>: IfNull, NullIf, AssumeNotNull,
/// Coalesce (2 + 3 args), IsNull, IsNotNull. Exercised against a Nullable column.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class NullCoalesceDbFunctionTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public NullCoalesceDbFunctionTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(
            new Row { Id = 1, MaybeName = "alpha" },
            new Row { Id = 2, MaybeName = null });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task IfNull_FillsNulls()
    {
        await using var ctx = await SeededAsync();
        var names = await ctx.Rows
            .OrderBy(r => r.Id)
            .Select(r => EfClass.Functions.IfNull(r.MaybeName, "default"))
            .ToListAsync();
        Assert.Equal(new[] { "alpha", "default" }, names);
    }

    [Fact]
    public async Task NullIf_TurnsMatchingValueIntoNull()
    {
        await using var ctx = await SeededAsync();
        var values = await ctx.Rows
            .OrderBy(r => r.Id)
            .Select(r => (uint?)EfClass.Functions.NullIf(r.Id, 1u))
            .ToListAsync();
        Assert.Null(values[0]);
        Assert.Equal(2u, values[1]);
    }

    [Fact]
    public async Task Coalesce_TwoAndThreeArg_PickFirstNonNull()
    {
        await using var ctx = await SeededAsync();
        var two = await ctx.Rows.OrderBy(r => r.Id)
            .Select(r => EfClass.Functions.Coalesce<string>(r.MaybeName, "fallback"))
            .ToListAsync();
        Assert.Equal(new[] { "alpha", "fallback" }, two);

        var three = await ctx.Rows.OrderBy(r => r.Id)
            .Select(r => EfClass.Functions.Coalesce<string>(r.MaybeName, null, "third"))
            .ToListAsync();
        Assert.Equal(new[] { "alpha", "third" }, three);
    }

    [Fact]
    public async Task IsNull_AndIsNotNull_PartitionRows()
    {
        await using var ctx = await SeededAsync();
        var nullCount = await ctx.Rows.CountAsync(r => EfClass.Functions.IsNull(r.MaybeName));
        var nonNullCount = await ctx.Rows.CountAsync(r => EfClass.Functions.IsNotNull(r.MaybeName));
        Assert.Equal(1, nullCount);
        Assert.Equal(1, nonNullCount);
    }

    [Fact]
    public async Task AssumeNotNull_OnGuaranteedRow_DoesNotThrow()
    {
        await using var ctx = await SeededAsync();
        var name = await ctx.Rows
            .Where(r => r.MaybeName != null)
            .Select(r => EfClass.Functions.AssumeNotNull(r.MaybeName))
            .FirstAsync();
        Assert.Equal("alpha", name);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string? MaybeName { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("NullCoalesceFnCoverage_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
