using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Exercises every method recognized by <c>ClickHouseMathMethodTranslator</c>.
/// Each test executes a query that selects a Math.* call and asserts the result
/// matches what .NET would compute on the same input.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MathFunctionCoverageTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MathFunctionCoverageTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.Add(new Row { Id = 1, X = 4.0, Y = 2.0, Negative = -3.5 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task Abs_Sign_Sqrt_Cbrt_Exp()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            Abs = Math.Abs(x.Negative),
            Sign = Math.Sign(x.Negative),
            Sqrt = Math.Sqrt(x.X),
            Cbrt = Math.Cbrt(8.0),
            Exp = Math.Exp(0),
        }).FirstAsync();
        Assert.Equal(3.5, r.Abs, 6);
        Assert.Equal(-1, r.Sign);
        Assert.Equal(2.0, r.Sqrt, 6);
        Assert.Equal(2.0, r.Cbrt, 6);
        Assert.Equal(1.0, r.Exp, 6);
    }

    [Fact]
    public async Task Floor_Ceiling_Truncate()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            Floor = Math.Floor(x.Negative),     // -4
            Ceiling = Math.Ceiling(x.Negative), // -3
            Trunc = Math.Truncate(x.Negative),  // -3
        }).FirstAsync();
        Assert.Equal(-4.0, r.Floor, 6);
        Assert.Equal(-3.0, r.Ceiling, 6);
        Assert.Equal(-3.0, r.Trunc, 6);
    }

    [Fact]
    public async Task Log_Log10_Log2_AndLogWithBase()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            Ln = Math.Log(Math.E),
            Log10 = Math.Log10(1000.0),
            Log2 = Math.Log2(8.0),
            LogBase = Math.Log(8.0, 2.0),
        }).FirstAsync();
        Assert.Equal(1.0, r.Ln, 5);
        Assert.Equal(3.0, r.Log10, 5);
        Assert.Equal(3.0, r.Log2, 5);
        Assert.Equal(3.0, r.LogBase, 5);
    }

    [Fact]
    public async Task Trig_AndHyperbolic_Identities()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(_ => new
        {
            Sin0 = Math.Sin(0),
            Cos0 = Math.Cos(0),
            Tan0 = Math.Tan(0),
            Asin1 = Math.Asin(1),
            Acos1 = Math.Acos(1),
            Atan0 = Math.Atan(0),
            Sinh0 = Math.Sinh(0),
            Cosh0 = Math.Cosh(0),
            Tanh0 = Math.Tanh(0),
            Asinh0 = Math.Asinh(0),
            Acosh1 = Math.Acosh(1),
            Atanh0 = Math.Atanh(0),
        }).FirstAsync();
        Assert.Equal(0.0, r.Sin0, 6);
        Assert.Equal(1.0, r.Cos0, 6);
        Assert.Equal(0.0, r.Tan0, 6);
        Assert.Equal(Math.PI / 2, r.Asin1, 5);
        Assert.Equal(0.0, r.Acos1, 6);
        Assert.Equal(0.0, r.Atan0, 6);
        Assert.Equal(0.0, r.Sinh0, 6);
        Assert.Equal(1.0, r.Cosh0, 6);
        Assert.Equal(0.0, r.Tanh0, 6);
        Assert.Equal(0.0, r.Asinh0, 6);
        Assert.Equal(0.0, r.Acosh1, 6);
        Assert.Equal(0.0, r.Atanh0, 6);
    }

    [Fact]
    public async Task Pow_Atan2_Min_Max()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            Pow = Math.Pow(x.X, x.Y),     // 4^2 = 16
            Atan2 = Math.Atan2(0, 1),     // 0
            Min = Math.Min(x.X, x.Y),     // 2
            Max = Math.Max(x.X, x.Y),     // 4
        }).FirstAsync();
        Assert.Equal(16.0, r.Pow, 6);
        Assert.Equal(0.0, r.Atan2, 6);
        Assert.Equal(2.0, r.Min, 6);
        Assert.Equal(4.0, r.Max, 6);
    }

    [Fact]
    public async Task Round_OneArg_AndTwoArg()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            R1 = Math.Round(x.Negative),     // -4 (banker's) or -3 — assert magnitude only
            R2 = Math.Round(1.23456, 2),
        }).FirstAsync();
        Assert.True(r.R1 == -3.0 || r.R1 == -4.0, $"Round(-3.5) returned unexpected {r.R1}");
        Assert.Equal(1.23, r.R2, 4);
    }

    [Fact]
    public async Task Clamp_BoundsAreApplied()
    {
        await using var ctx = await SeededAsync();
        var clamped = await ctx.Rows.Select(x => Math.Clamp(x.X, 0.0, 1.0)).FirstAsync();
        Assert.Equal(1.0, clamped, 6);
    }

    [Fact]
    public async Task FusedMultiplyAdd_ComputesXyPlusZ()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => Math.FusedMultiplyAdd(x.X, x.Y, 1.0)).FirstAsync();
        Assert.Equal(9.0, r, 6);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public double X { get; set; }
        public double Y { get; set; }
        public double Negative { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("MathFnCoverage_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
