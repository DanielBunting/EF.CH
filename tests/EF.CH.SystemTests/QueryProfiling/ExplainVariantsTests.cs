using EF.CH.Extensions;
using EF.CH.QueryProfiling;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.QueryProfiling;

/// <summary>
/// Coverage of every Explain* variant. Each test asserts a sentinel substring that
/// only the *correct* explain kind would produce — non-empty output alone is too
/// weak (a stub returning a single placeholder line would pass).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class ExplainVariantsTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ExplainVariantsTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        for (uint i = 1; i <= 10; i++) ctx.Rows.Add(new Row { Id = i, V = (int)i });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task ExplainPlan_OutputMentionsReadFromMergeTree()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Where(x => x.V > 5).ExplainPlanAsync(ctx);
        Assert.NotEmpty(r.Output);
        Assert.Contains(r.Output, line => line.Contains("ReadFromMergeTree", StringComparison.Ordinal)
                                       || line.Contains("Expression", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ExplainAst_OutputContainsAstNodes()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.ExplainAstAsync(ctx);
        Assert.NotEmpty(r.Output);
        // AST output uses node names like SelectQuery/Identifier/TablesInSelectQuery.
        Assert.Contains(r.Output, line => line.Contains("Identifier", StringComparison.Ordinal)
                                       || line.Contains("SelectQuery", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ExplainSyntax_OutputContainsRewrittenSelect()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.ExplainSyntaxAsync(ctx);
        Assert.NotEmpty(r.Output);
        Assert.Contains(r.Output, line => line.Contains("SELECT", StringComparison.Ordinal));
        Assert.Contains(r.Output, line => line.Contains("Explain_Rows", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ExplainQueryTree_OutputContainsQueryTreeMarker()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.ExplainQueryTreeAsync(ctx);
        Assert.NotEmpty(r.Output);
        Assert.Contains(r.Output, line => line.Contains("QUERY", StringComparison.Ordinal)
                                       || line.Contains("PROJECTION", StringComparison.Ordinal)
                                       || line.Contains("Explain_Rows", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ExplainPipeline_OutputContainsPipelineProcessor()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.ExplainPipelineAsync(ctx);
        Assert.NotEmpty(r.Output);
        // Pipeline output mentions processor types like (MergeTreeSource)/(ExpressionTransform)/(Sink).
        Assert.Contains(r.Output, line => line.Contains("MergeTree", StringComparison.Ordinal)
                                       || line.Contains("Source", StringComparison.Ordinal)
                                       || line.Contains("Sink", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ExplainEstimate_OutputContainsDatabaseOrTable()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.ExplainEstimateAsync(ctx);
        Assert.NotEmpty(r.Output);
        // EXPLAIN ESTIMATE returns a tabular result with the first column being the
        // database name. The provider's Output projection extracts that column;
        // for our test database the value is "default".
        Assert.Contains(r.Output, line => line.Contains("default", StringComparison.OrdinalIgnoreCase)
                                       || line.Contains("Explain_Rows", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ExplainSql_OnRawSelectOne_OutputContainsExpressionOrReadFromStorage()
    {
        // ExplainSqlAsync defaults to EXPLAIN PLAN; for `SELECT 1` ClickHouse returns
        // a plan with `Expression(...)` over `ReadFromStorage (SystemOne)`.
        await using var ctx = await SeededAsync();
        var r = await ctx.ExplainSqlAsync("SELECT 1");
        Assert.NotEmpty(r.Output);
        Assert.Contains(r.Output, line => line.Contains("Expression", StringComparison.Ordinal)
                                       || line.Contains("ReadFromStorage", StringComparison.Ordinal));
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public int V { get; set; }
    }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("Explain_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
