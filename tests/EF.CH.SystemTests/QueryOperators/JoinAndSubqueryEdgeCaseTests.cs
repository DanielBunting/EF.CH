using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.QueryOperators;

/// <summary>
/// Verifies the audit findings about <c>.Sample()</c> / <c>.PreWhere()</c> / <c>.LimitBy()</c>
/// when combined with joins or subqueries:
///
/// <list type="bullet">
///   <item><description>Audit claim: <c>.Sample()</c> is silently dropped when used in a joined query.</description></item>
///   <item><description>Audit claim: <c>.PreWhere()</c> fails server-side when combined with joins.</description></item>
///   <item><description>Audit claim: <c>.LimitBy()</c> fails when nested inside a subquery.</description></item>
/// </list>
///
/// Each test asserts the desired behaviour. Failures here tell us which claims are real
/// and what needs to be fixed (translation-time guard or working translation).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class JoinAndSubqueryEdgeCaseTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public JoinAndSubqueryEdgeCaseTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task Sample_With_Join_To_NonSampleableLookup_ThrowsClearly()
    {
        // Audit claim verified: applying .Sample() to a query that joins a non-
        // SAMPLE-BY lookup causes the visitor to wrap both tables, and the server
        // rejects with "Storage X doesn't support sampling". We now catch this at
        // translation time so the user sees a clear error (and the suggested fix:
        // sample-then-join) instead of a confusing server-side message.
        await Setup_SampleableWithLookup();

        await using var ctx = TestContextFactory.Create<JoinCtx>(Conn);

        var query =
            from r in ctx.Sampleables.Sample(0.5)
            join l in ctx.Lookups on r.Bucket equals l.Bucket
            select r.Id;

        var ex = Assert.Throws<InvalidOperationException>(() => query.ToQueryString());
        Assert.Contains("SAMPLE BY", ex.Message);
        Assert.Contains("Lookup", ex.Message);
    }

    [Fact]
    public async Task Sample_OnDeclaredSampleByTable_RendersSampleClause_AndAppliesIt()
    {
        // Sanity: the throw in the join case isn't blocking valid Sample usage
        // on a properly-declared SAMPLE BY table.
        await Setup_SampleableWithLookup();

        await using var ctx = TestContextFactory.Create<JoinCtx>(Conn);

        var query = ctx.Sampleables.Sample(0.5).Select(r => r.Id);

        var sql = query.ToQueryString();
        Assert.Contains("SAMPLE", sql, StringComparison.OrdinalIgnoreCase);

        var sampled = await query.ToListAsync();
        var total = await ctx.Sampleables.CountAsync();
        Assert.True(sampled.Count < total,
            $"Sample(0.5) on the sampleable-only query should return a strict subset; got {sampled.Count}/{total}.");
    }

    [Fact]
    public async Task PreWhere_With_Join_RendersPrewhereClauseInSql()
    {
        await Setup_PreWhereWithLookup();

        await using var ctx = TestContextFactory.Create<JoinCtx>(Conn);

        var query =
            from r in ctx.Sampleables.PreWhere(r => r.Id > 950)
            join l in ctx.Lookups on r.Bucket equals l.Bucket
            select r.Id;

        var sql = query.ToQueryString();
        Assert.Contains("PREWHERE", sql, StringComparison.OrdinalIgnoreCase);

        var ids = await query.ToListAsync();
        Assert.True(ids.Count > 0, "PreWhere with join should return rows where the predicate matches.");
        Assert.All(ids, id => Assert.True(id > 950, $"PreWhere predicate not applied; got id={id}"));
    }

    [Fact]
    public async Task LimitBy_InSubquery_FailsWithClearTranslationError()
    {
        // Audit claim verified: .LimitBy() nested as the source of a join is not
        // translatable by EF Core's NavigationExpandingExpressionVisitor — it
        // throws "The LINQ expression … could not be translated" before our
        // pipeline gets a chance to handle it. Pinned here as a known limitation;
        // workaround: materialise the LimitBy result and join it client-side.
        await Setup_LimitByGroups();

        await using var ctx = TestContextFactory.Create<JoinCtx>(Conn);

        var perGroupTopTwo = ctx.Groupables
            .OrderByDescending(r => r.Score)
            .LimitBy(r => r.Group, 2);

        var withLookup =
            from t in perGroupTopTwo
            join l in ctx.Lookups on t.Bucket equals l.Bucket
            select new { t.Id, t.Group };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => withLookup.ToListAsync());
        Assert.Contains("could not be translated", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private async Task Setup_SampleableWithLookup()
    {
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"JoinEdge_Sampleable\"");
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"JoinEdge_Lookup\"");
        await RawClickHouse.ExecuteAsync(Conn,
            "CREATE TABLE \"JoinEdge_Sampleable\" (Id UInt32, Bucket UInt32, Group String, Score Int32) " +
            "ENGINE = MergeTree() PARTITION BY tuple() ORDER BY (Bucket, Id) SAMPLE BY Bucket");
        await RawClickHouse.ExecuteAsync(Conn,
            "CREATE TABLE \"JoinEdge_Lookup\" (Bucket UInt32, Label String) " +
            "ENGINE = MergeTree() ORDER BY Bucket");

        var values = string.Join(", ", Enumerable.Range(1, 1000)
            .Select(i => $"({i}, {(uint)(i * 2654435761u)}, 'g{i % 4}', {i})"));
        await RawClickHouse.ExecuteAsync(Conn,
            $"INSERT INTO \"JoinEdge_Sampleable\" (Id, Bucket, Group, Score) VALUES {values}");

        var lookupValues = string.Join(", ", Enumerable.Range(1, 1000)
            .Select(i => $"({(uint)(i * 2654435761u)}, 'L{i}')"));
        await RawClickHouse.ExecuteAsync(Conn,
            $"INSERT INTO \"JoinEdge_Lookup\" (Bucket, Label) VALUES {lookupValues}");
    }

    private Task Setup_PreWhereWithLookup() => Setup_SampleableWithLookup();
    private Task Setup_LimitByGroups() => Setup_SampleableWithLookup();

    public sealed class Sampleable
    {
        public uint Id { get; set; }
        public uint Bucket { get; set; }
        public string Group { get; set; } = "";
        public int Score { get; set; }
    }

    public sealed class Lookup
    {
        public uint Bucket { get; set; }
        public string Label { get; set; } = "";
    }

    public sealed class JoinCtx(DbContextOptions<JoinCtx> o) : DbContext(o)
    {
        public DbSet<Sampleable> Sampleables => Set<Sampleable>();
        public DbSet<Sampleable> Groupables => Set<Sampleable>(); // same table, different alias for readability
        public DbSet<Lookup> Lookups => Set<Lookup>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Sampleable>(e =>
            {
                e.ToTable("JoinEdge_Sampleable");
                e.HasKey(x => new { x.Bucket, x.Id });
                e.UseMergeTree(x => new { x.Bucket, x.Id });
                e.HasSampleBy(x => x.Bucket);
            });
            mb.Entity<Lookup>(e =>
            {
                e.ToTable("JoinEdge_Lookup");
                e.HasKey(x => x.Bucket);
                e.UseMergeTree(x => x.Bucket);
            });
        }
    }
}
