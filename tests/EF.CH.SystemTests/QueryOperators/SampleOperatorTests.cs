using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.QueryOperators;

/// <summary>
/// Coverage of <c>Sample(fraction)</c> and <c>Sample(fraction, offset)</c>. SAMPLE requires
/// the table to declare a <c>SAMPLE BY</c> key — built here via raw DDL since the fluent
/// surface for sample-by isn't asserted elsewhere yet. Assertions verify the rendered
/// SQL contains a SAMPLE clause and that it returns a strict subset of the population
/// — a no-op operator that returned the full table would pass a "≤ total" bound, but
/// fail "< total" with high probability.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class SampleOperatorTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public SampleOperatorTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task Sample_FractionLessThanOne_RendersSampleClause_AndReturnsStrictSubset()
    {
        await using var ctx = await PopulateAsync();
        var query = ctx.Rows.Sample(0.5);

        var sql = query.ToQueryString();
        Assert.Contains("SAMPLE", sql, StringComparison.OrdinalIgnoreCase);

        var sampled = await query.CountAsync();
        var total = await ctx.Rows.CountAsync();
        Assert.True(sampled < total, $"Sample(0.5) should be a strict subset of {total} rows; got {sampled}");
        Assert.True(sampled > 0, $"Sample(0.5) over 1000 rows should have returned some rows; got {sampled}");
    }

    [Fact]
    public async Task Sample_DifferentOffsets_ReturnDifferentRowSets()
    {
        await using var ctx = await PopulateAsync();
        var query1 = ctx.Rows.Sample(0.25, 0.0);
        var query2 = ctx.Rows.Sample(0.25, 0.5);

        var sql1 = query1.ToQueryString();
        Assert.Contains("SAMPLE", sql1, StringComparison.OrdinalIgnoreCase);

        var ids1 = (await query1.Select(r => r.Id).ToListAsync()).ToHashSet();
        var ids2 = (await query2.Select(r => r.Id).ToListAsync()).ToHashSet();

        // Two non-overlapping 25% sample windows over the same population should
        // produce different row sets — if Sample were silently dropped, both
        // queries would return the whole table and SetEquals would be true.
        Assert.False(ids1.SetEquals(ids2),
            $"Sample(0.25, 0) and Sample(0.25, 0.5) should produce different row sets; both returned {ids1.Count} == {ids2.Count} matching ids");
    }

    private async Task<Ctx> PopulateAsync()
    {
        // Drop & deploy fresh schema with an explicit SAMPLE BY clause via raw DDL —
        // the fluent API for SAMPLE BY isn't part of this test's contract.
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"SampleOpTests_Rows\"");
        await RawClickHouse.ExecuteAsync(Conn,
            "CREATE TABLE \"SampleOpTests_Rows\" (Id UInt32, Bucket UInt32) " +
            "ENGINE = MergeTree() PARTITION BY tuple() ORDER BY (Bucket, Id) SAMPLE BY Bucket");

        var values = string.Join(", ", Enumerable.Range(1, 1000).Select(i => $"({i}, {(uint)(i * 2654435761u)})"));
        await RawClickHouse.ExecuteAsync(Conn, $"INSERT INTO \"SampleOpTests_Rows\" (Id, Bucket) VALUES {values}");

        var ctx = TestContextFactory.Create<Ctx>(Conn);
        return ctx;
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public uint Bucket { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("SampleOpTests_Rows");
                e.HasKey(x => new { x.Bucket, x.Id });
                e.UseMergeTree(x => new { x.Bucket, x.Id });
            });
    }
}
