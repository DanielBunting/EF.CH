using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Engines;

/// <summary>
/// Baseline coverage of vanilla <c>UseMergeTree</c>: schema, insert, and read.
/// All other engine tests build on this baseline; if it regresses, half the suite breaks.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MergeTreeBaselineTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MergeTreeBaselineTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task MergeTree_RoundTrip_AndEngineFullDeclaresMergeTree()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(
            new Row { Id = 1, Name = "a" },
            new Row { Id = 2, Name = "b" });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var n = await ctx.Rows.CountAsync();
        Assert.Equal(2, n);
        // Use the `engine` column (exact name) instead of substring-matching engine_full,
        // which would also match ReplacingMergeTree/SummingMergeTree/etc.
        var engine = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'MergeTreeBaseline_Rows'");
        Assert.Equal("MergeTree", engine);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string Name { get; set; } = "";
    }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("MergeTreeBaseline_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
