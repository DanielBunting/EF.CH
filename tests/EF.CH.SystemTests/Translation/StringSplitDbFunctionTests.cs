using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Coverage of <see cref="ClickHouseStringSplitDbFunctionsExtensions"/>: pin
/// the translated SQL output for canonical inputs against a live ClickHouse
/// server.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class StringSplitDbFunctionTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public StringSplitDbFunctionTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.Add(new Row { Id = 1, Csv = "a,b,c", Stringly = "foo::bar::baz", Mixed = "12-34_56" });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task SplitByChar_ProducesExpectedArray()
    {
        await using var ctx = await SeededAsync();
        var arr = await ctx.Rows.Select(x => EfClass.Functions.SplitByChar(",", x.Csv)).FirstAsync();
        Assert.Equal(new[] { "a", "b", "c" }, arr);
    }

    [Fact]
    public async Task SplitByString_ProducesExpectedArray()
    {
        await using var ctx = await SeededAsync();
        var arr = await ctx.Rows.Select(x => EfClass.Functions.SplitByString("::", x.Stringly)).FirstAsync();
        Assert.Equal(new[] { "foo", "bar", "baz" }, arr);
    }

    [Fact]
    public async Task SplitByRegexp_ProducesExpectedArray()
    {
        await using var ctx = await SeededAsync();
        var arr = await ctx.Rows.Select(x => EfClass.Functions.SplitByRegexp("[-_]", x.Mixed)).FirstAsync();
        Assert.Equal(new[] { "12", "34", "56" }, arr);
    }

    [Fact]
    public async Task AlphaTokens_TokenizesAlphabeticRuns()
    {
        await using var ctx = await SeededAsync();
        var arr = await ctx.Rows.Select(x =>
            EfClass.Functions.AlphaTokens("abc 123 def_456 ghi")).FirstAsync();
        Assert.Equal(new[] { "abc", "def", "ghi" }, arr);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string Csv { get; set; } = "";
        public string Stringly { get; set; } = "";
        public string Mixed { get; set; } = "";
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("StringSplitFn_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
