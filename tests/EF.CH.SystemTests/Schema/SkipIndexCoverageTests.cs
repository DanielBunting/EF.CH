using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// Coverage of every skip-index fluent: <c>UseMinmax</c>, <c>UseBloomFilter</c>,
/// <c>UseTokenBF</c>, <c>UseNgramBF</c>, <c>UseSet</c>, plus <c>HasGranularity</c>.
/// We assert each index appears in <c>system.data_skipping_indices</c> with the right type.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class SkipIndexCoverageTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public SkipIndexCoverageTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task EachSkipIndex_HasExpectedTypeColumnAndGranularity()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rows = await RawClickHouse.SkipIndicesAsync(Conn, "SkipIdx_Rows");
        // Build a per-target-column lookup so each fluent setting can be verified
        // against its rendered DDL. EF's auto-generated index name is
        // implementation-detail; the column it covers is what we care about.
        var byColumn = rows.ToDictionary(
            r => ((string)r["expr"]!).Trim(),
            r => (Type: (string)r["type"]!, Granularity: Convert.ToInt32(r["granularity"])));

        Assert.True(byColumn.TryGetValue("Score", out var minmax),
            "Expected a skip index on column Score (minmax)");
        Assert.StartsWith("minmax", minmax.Type, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(4, minmax.Granularity);

        Assert.True(byColumn.TryGetValue("Tag", out var bloom),
            "Expected a skip index on column Tag (bloom_filter)");
        Assert.StartsWith("bloom_filter", bloom.Type, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(4, bloom.Granularity);

        Assert.True(byColumn.TryGetValue("Document", out var tokenbf),
            "Expected a skip index on column Document (tokenbf_v1)");
        Assert.StartsWith("tokenbf_v1", tokenbf.Type, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(4, tokenbf.Granularity);

        Assert.True(byColumn.TryGetValue("Ngram", out var ngrambf),
            "Expected a skip index on column Ngram (ngrambf_v1)");
        Assert.StartsWith("ngrambf_v1", ngrambf.Type, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(4, ngrambf.Granularity);

        Assert.True(byColumn.TryGetValue("Category", out var setIdx),
            "Expected a skip index on column Category (set)");
        Assert.StartsWith("set", setIdx.Type, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(4, setIdx.Granularity);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public int Score { get; set; }
        public string Tag { get; set; } = "";
        public string Document { get; set; } = "";
        public string Ngram { get; set; } = "";
        public string Category { get; set; } = "";
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("SkipIdx_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.HasIndex(x => x.Score).UseMinmax().HasGranularity(4);
                e.HasIndex(x => x.Tag).UseBloomFilter().HasGranularity(4);
                e.HasIndex(x => x.Document).UseTokenBF(32768, 3, 0).HasGranularity(4);
                e.HasIndex(x => x.Ngram).UseNgramBF(3, 32768, 3, 0).HasGranularity(4);
                e.HasIndex(x => x.Category).UseSet(100).HasGranularity(4);
            });
    }
}
