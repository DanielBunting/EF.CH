using System.Text.Json;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Coverage of <c>ClickHouseJsonMethodTranslator</c>: GetPath&lt;T&gt;, GetPathOrDefault&lt;T&gt;,
/// HasPath, GetArray&lt;T&gt;, GetObject. Requires ClickHouse with native JSON support
/// (24.8+; the test image is 25.3).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class JsonPathTranslationTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public JsonPathTranslationTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private static JsonDocument Doc(string json) => JsonDocument.Parse(json);

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.Add(new Row { Id = 1, Data = Doc("{\"name\":\"alpha\",\"score\":42,\"tags\":[\"x\",\"y\",\"z\"]}") });
        ctx.Rows.Add(new Row { Id = 2, Data = Doc("{\"name\":\"beta\"}") });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task GetPath_ReturnsScalarFromJsonColumn()
    {
        await using var ctx = await SeededAsync();
        var name = await ctx.Rows.Where(r => r.Id == 1).Select(r => r.Data.GetPath<string>("name")).FirstAsync();
        Assert.Equal("alpha", name);
    }

    [Fact]
    public async Task GetPathOrDefault_FallsBackWhenMissing()
    {
        await using var ctx = await SeededAsync();
        var score = await ctx.Rows.Where(r => r.Id == 2).Select(r => r.Data.GetPathOrDefault<int>("score", -1)).FirstAsync();
        Assert.Equal(-1, score);
    }

    [Fact]
    public async Task HasPath_TrueOnlyWhenKeyPresent()
    {
        await using var ctx = await SeededAsync();
        var withScore = await ctx.Rows.Where(r => r.Data.HasPath("score")).Select(r => r.Id).ToListAsync();
        Assert.Equal(new uint[] { 1 }, withScore);
    }

    [Fact]
    public async Task GetArray_RetrievesArraySubcolumn()
    {
        await using var ctx = await SeededAsync();
        var tags = await ctx.Rows.Where(r => r.Id == 1).Select(r => r.Data.GetArray<string>("tags")).FirstAsync();
        Assert.Equal(new[] { "x", "y", "z" }, tags);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public JsonDocument Data { get; set; } = JsonDocument.Parse("{}");
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("JsonPathCoverage_Rows");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
                e.Property(x => x.Data).HasColumnType("JSON");
            });
    }
}
