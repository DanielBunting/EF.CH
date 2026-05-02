using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Windows;

/// <summary>
/// Off-by-one matrix for window frame specs. Existing
/// <see cref="WindowFrameTests"/> only covers <c>UNBOUNDED PRECEDING / CURRENT
/// ROW</c>; this file enumerates the harder boundary cases (N PRECEDING /
/// CURRENT ROW, CURRENT ROW / N FOLLOWING, N PRECEDING / N FOLLOWING, fully
/// unbounded, RANGE BETWEEN) over a known partition <c>[10, 20, 30, 40, 50]</c>
/// so each row's expected frame sum is hand-computable.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class WindowFrameMatrixTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public WindowFrameMatrixTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        // Single partition "p" with V = 10, 20, 30, 40, 50 ordered by Id 1..5.
        // Pre-computing per-row expectations is straightforward:
        //   id 1: 10
        //   id 2: 20
        //   id 3: 30
        //   id 4: 40
        //   id 5: 50
        for (uint id = 1; id <= 5; id++)
            ctx.Rows.Add(new Row { Id = id, G = "p", V = (int)(id * 10) });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    /// <summary>
    /// ROWS BETWEEN N PRECEDING AND CURRENT ROW — the most error-prone frame.
    /// For N=1 over [10,20,30,40,50] the expected sums are:
    ///   id 1: 10                 (no preceding row in partition)
    ///   id 2: 10+20 = 30
    ///   id 3: 20+30 = 50
    ///   id 4: 30+40 = 70
    ///   id 5: 40+50 = 90
    /// </summary>
    [Theory]
    [InlineData(1, new[] { 10L, 30L, 50L, 70L, 90L })]
    [InlineData(2, new[] { 10L, 30L, 60L, 90L, 120L })]
    [InlineData(3, new[] { 10L, 30L, 60L, 100L, 140L })]
    public async Task RowsBetween_NPreceding_AndCurrentRow(int n, long[] expectedPerRow)
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows.Select(r => new
        {
            r.Id,
            S = Window.Sum<long>(r.V, w => w.PartitionBy(r.G).OrderBy(r.Id)
                .Rows().Preceding(n).CurrentRow()),
        }).OrderBy(r => r.Id).ToListAsync();
        AssertRowsEqual(expectedPerRow, rows.Select(x => x.S));
    }

    /// <summary>
    /// ROWS BETWEEN CURRENT ROW AND N FOLLOWING.
    /// For N=1 over [10,20,30,40,50] the expected sums are:
    ///   id 1: 10+20 = 30
    ///   id 2: 20+30 = 50
    ///   id 3: 30+40 = 70
    ///   id 4: 40+50 = 90
    ///   id 5: 50                 (no following row in partition)
    /// </summary>
    [Theory]
    [InlineData(1, new[] { 30L, 50L, 70L, 90L, 50L })]
    [InlineData(2, new[] { 60L, 90L, 120L, 90L, 50L })]
    public async Task RowsBetween_CurrentRow_AndNFollowing(int n, long[] expectedPerRow)
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows.Select(r => new
        {
            r.Id,
            S = Window.Sum<long>(r.V, w => w.PartitionBy(r.G).OrderBy(r.Id)
                .Rows().CurrentRow().Following(n)),
        }).OrderBy(r => r.Id).ToListAsync();
        AssertRowsEqual(expectedPerRow, rows.Select(x => x.S));
    }

    /// <summary>
    /// ROWS BETWEEN N PRECEDING AND N FOLLOWING — symmetric ±1 window.
    /// Over [10,20,30,40,50] with ±1:
    ///   id 1: 10+20 = 30
    ///   id 2: 10+20+30 = 60
    ///   id 3: 20+30+40 = 90
    ///   id 4: 30+40+50 = 120
    ///   id 5: 40+50 = 90
    /// </summary>
    [Fact]
    public async Task RowsBetween_NPreceding_AndNFollowing()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows.Select(r => new
        {
            r.Id,
            S = Window.Sum<long>(r.V, w => w.PartitionBy(r.G).OrderBy(r.Id)
                .Rows().Preceding(1).Following(1)),
        }).OrderBy(r => r.Id).ToListAsync();
        AssertRowsEqual(new long[] { 30, 60, 90, 120, 90 }, rows.Select(x => x.S));
    }

    /// <summary>
    /// ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING — the partition
    /// total. Every row sees the same value (10+20+30+40+50 = 150).
    /// </summary>
    [Fact]
    public async Task RowsBetween_UnboundedPreceding_AndUnboundedFollowing()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows.Select(r => new
        {
            r.Id,
            S = Window.Sum<long>(r.V, w => w.PartitionBy(r.G).OrderBy(r.Id)
                .Rows().UnboundedPreceding().UnboundedFollowing()),
        }).OrderBy(r => r.Id).ToListAsync();
        AssertRowsEqual(new long[] { 150, 150, 150, 150, 150 }, rows.Select(x => x.S));
    }

    /// <summary>
    /// RANGE BETWEEN N PRECEDING AND CURRENT ROW. With distinct V = 10..50 and
    /// ORDER BY V, RANGE behaves like ROWS for non-tied values: the running
    /// window includes rows whose V is within [current-N*1, current] — but
    /// because ClickHouse RANGE uses the literal value as the range,
    /// <c>RANGE BETWEEN 10 PRECEDING AND CURRENT ROW</c> over V=10..50 gives:
    ///   v=10 → [10, 10] → 10
    ///   v=20 → [10, 20] → 30
    ///   v=30 → [20, 30] → 50
    ///   v=40 → [30, 40] → 70
    ///   v=50 → [40, 50] → 90
    /// </summary>
    [Fact]
    public async Task RangeBetween_NPreceding_AndCurrentRow()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows.Select(r => new
        {
            r.Id,
            S = Window.Sum<long>(r.V, w => w.PartitionBy(r.G).OrderBy(r.V)
                .Range().Preceding(10).CurrentRow()),
        }).OrderBy(r => r.Id).ToListAsync();
        AssertRowsEqual(new long[] { 10, 30, 50, 70, 90 }, rows.Select(x => x.S));
    }

    private static void AssertRowsEqual(long[] expected, IEnumerable<long> actual)
    {
        var got = actual.ToArray();
        Assert.Equal(expected.Length, got.Length);
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], got[i]);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string G { get; set; } = "";
        public int V { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("WindowFrameMatrix_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
