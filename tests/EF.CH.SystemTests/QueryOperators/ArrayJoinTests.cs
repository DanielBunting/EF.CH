using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.QueryOperators;

/// <summary>
/// Coverage of <c>ArrayJoin</c> (inner; rows with empty arrays disappear) and
/// <c>LeftArrayJoin</c> (rows with empty arrays preserved with default element values).
/// Also covers the two-array positional variant.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class ArrayJoinTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ArrayJoinTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(
            new Row { Id = 1, Tags = new[] { "a", "b", "c" }, Counts = new[] { 1, 2, 3 } },
            new Row { Id = 2, Tags = Array.Empty<string>(), Counts = Array.Empty<int>() },
            new Row { Id = 3, Tags = new[] { "x" }, Counts = new[] { 99 } });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task ArrayJoin_DropsRowsWithEmptyArrays()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows
            .ArrayJoin(r => r.Tags, (r, t) => new { r.Id, Tag = t })
            .ToListAsync();
        // 3 + 0 + 1 = 4 unnested rows.
        Assert.Equal(4, rows.Count);
        Assert.DoesNotContain(rows, r => r.Id == 2);
    }

    [Fact]
    public async Task LeftArrayJoin_PreservesRowsWithEmptyArrays()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows
            .LeftArrayJoin(r => r.Tags, (r, t) => new { r.Id, Tag = t })
            .ToListAsync();
        // 3 + 1 (empty preserved) + 1 = 5 rows.
        Assert.Equal(5, rows.Count);
        Assert.Contains(rows, r => r.Id == 2);
    }

    [Fact]
    public async Task ArrayJoin_TwoArrays_PositionalPairing()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows
            .Where(r => r.Id == 1)
            .ArrayJoin(r => r.Tags, r => r.Counts, (r, t, c) => new { r.Id, Tag = t, Count = c })
            .ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Contains(rows, r => r.Tag == "a" && r.Count == 1);
        Assert.Contains(rows, r => r.Tag == "b" && r.Count == 2);
        Assert.Contains(rows, r => r.Tag == "c" && r.Count == 3);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string[] Tags { get; set; } = Array.Empty<string>();
        public int[] Counts { get; set; } = Array.Empty<int>();
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("ArrayJoinOpTests_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
