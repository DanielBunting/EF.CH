using EF.CH.Extensions;
using EF.CH.QueryProfiling;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.QueryProfiling;

/// <summary>
/// Tests for query profiling functionality including EXPLAIN queries and statistics.
/// </summary>
public class QueryProfilingTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    private string GetConnectionString() => _container.GetConnectionString();

    [Fact]
    public async Task ExplainAsync_ReturnsValidPlanOutput()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        // Insert some test data
        await InsertTestDataAsync(context);

        // Execute EXPLAIN PLAN
        var result = await context.Events
            .Where(e => e.Category == "CategoryA")
            .ExplainAsync(context);

        Assert.NotNull(result);
        Assert.Equal(ExplainType.Plan, result.Type);
        Assert.True(result.Output.Count > 0);
        Assert.False(string.IsNullOrEmpty(result.FormattedOutput));
        Assert.Contains("SELECT", result.OriginalSql);
        Assert.Contains("EXPLAIN PLAN", result.ExplainSql);
        Assert.True(result.Elapsed > TimeSpan.Zero);
    }

    [Fact]
    public async Task ExplainAsync_DifferentTypesReturnDifferentOutput()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var query = context.Events.Where(e => e.Amount > 100);

        // Get different explain types
        var planResult = await query.ExplainAsync(context, opts => opts.Type = ExplainType.Plan);
        var astResult = await query.ExplainAsync(context, opts => opts.Type = ExplainType.Ast);
        var syntaxResult = await query.ExplainAsync(context, opts => opts.Type = ExplainType.Syntax);
        var pipelineResult = await query.ExplainAsync(context, opts => opts.Type = ExplainType.Pipeline);

        // Verify each type returns valid output
        Assert.Equal(ExplainType.Plan, planResult.Type);
        Assert.Equal(ExplainType.Ast, astResult.Type);
        Assert.Equal(ExplainType.Syntax, syntaxResult.Type);
        Assert.Equal(ExplainType.Pipeline, pipelineResult.Type);

        Assert.True(planResult.Output.Count > 0);
        Assert.True(astResult.Output.Count > 0);
        Assert.True(syntaxResult.Output.Count > 0);
        Assert.True(pipelineResult.Output.Count > 0);

        // Outputs should be different
        Assert.NotEqual(planResult.FormattedOutput, astResult.FormattedOutput);
        Assert.NotEqual(planResult.FormattedOutput, syntaxResult.FormattedOutput);
    }

    [Fact]
    public async Task ExplainAsync_WithJsonOption_ReturnsJson()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var result = await context.Events
            .ExplainAsync(context, opts =>
            {
                opts.Type = ExplainType.Plan;
                opts.Json = true;
            });

        Assert.NotNull(result);
        Assert.NotNull(result.JsonOutput);
        Assert.Contains("json = 1", result.ExplainSql);
        // JSON output should contain JSON structure
        Assert.Contains("{", result.JsonOutput);
    }

    [Fact]
    public async Task ExplainEstimateAsync_ReturnsRowEstimates()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        // Insert test data
        await InsertTestDataAsync(context);

        var result = await context.Events.ExplainEstimateAsync(context);

        Assert.NotNull(result);
        Assert.Equal(ExplainType.Estimate, result.Type);
        Assert.Contains("EXPLAIN ESTIMATE", result.ExplainSql);
        Assert.True(result.Output.Count > 0);
    }

    [Fact]
    public async Task ExplainPlanAsync_ConvenienceMethod_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var result = await context.Events.ExplainPlanAsync(context);

        Assert.NotNull(result);
        Assert.Equal(ExplainType.Plan, result.Type);
    }

    [Fact]
    public async Task ExplainAstAsync_ConvenienceMethod_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var result = await context.Events.ExplainAstAsync(context);

        Assert.NotNull(result);
        Assert.Equal(ExplainType.Ast, result.Type);
        // AST output typically contains SelectWithUnionQuery or similar
        Assert.True(result.FormattedOutput.Contains("Select") || result.Output.Count > 0);
    }

    [Fact]
    public async Task ExplainSyntaxAsync_ConvenienceMethod_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var result = await context.Events.ExplainSyntaxAsync(context);

        Assert.NotNull(result);
        Assert.Equal(ExplainType.Syntax, result.Type);
        // SYNTAX returns the optimized query
        Assert.Contains("SELECT", result.FormattedOutput);
    }

    [Fact]
    public async Task ExplainPipelineAsync_ConvenienceMethod_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var result = await context.Events.ExplainPipelineAsync(context);

        Assert.NotNull(result);
        Assert.Equal(ExplainType.Pipeline, result.Type);
        Assert.True(result.Output.Count > 0);
    }

    [Fact]
    public async Task ExplainQueryTreeAsync_ConvenienceMethod_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var result = await context.Events.ExplainQueryTreeAsync(context);

        Assert.NotNull(result);
        Assert.Equal(ExplainType.QueryTree, result.Type);
        Assert.True(result.Output.Count > 0);
    }

    [Fact]
    public async Task ToListWithStatsAsync_ReturnsResultsAndStatistics()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        // Insert test data
        await InsertTestDataAsync(context);

        var result = await context.Events
            .Where(e => e.Category == "CategoryA")
            .ToListWithStatsAsync(context);

        Assert.NotNull(result);
        Assert.True(result.Results.Count > 0);
        Assert.True(result.Elapsed > TimeSpan.Zero);
        Assert.Contains("SELECT", result.Sql);
        Assert.Equal(50, result.Count); // Half of 100 should be CategoryA

        // Statistics may or may not be available depending on timing
        // Just verify the result structure is correct
        if (result.Statistics != null)
        {
            Assert.True(result.Statistics.RowsRead >= 0);
            Assert.NotNull(result.Statistics.Summary);
        }
    }

    [Fact]
    public async Task ExplainSqlAsync_WorksWithRawSql()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var result = await context.ExplainSqlAsync(
            "SELECT * FROM \"Events\" WHERE \"Amount\" > 100");

        Assert.NotNull(result);
        Assert.Contains("EXPLAIN PLAN", result.ExplainSql);
        Assert.Contains("SELECT * FROM", result.OriginalSql);
        Assert.True(result.Output.Count > 0);
    }

    [Fact]
    public async Task ExplainSqlAsync_WithCustomOptions_AppliesSettings()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var result = await context.ExplainSqlAsync(
            "SELECT * FROM \"Events\"",
            opts =>
            {
                opts.Type = ExplainType.Pipeline;
                opts.Header = true;
            });

        Assert.NotNull(result);
        Assert.Equal(ExplainType.Pipeline, result.Type);
        Assert.Contains("EXPLAIN PIPELINE", result.ExplainSql);
        Assert.Contains("header = 1", result.ExplainSql);
    }

    [Fact]
    public async Task ExplainAsync_WithComplexQuery_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        await InsertTestDataAsync(context);

        // Test with a more complex query
        var result = await context.Events
            .Where(e => e.Category == "CategoryA" && e.Amount > 100)
            .OrderByDescending(e => e.Timestamp)
            .Take(10)
            .ExplainAsync(context);

        Assert.NotNull(result);
        Assert.True(result.Output.Count > 0);
        Assert.Contains("WHERE", result.OriginalSql);
        Assert.Contains("ORDER BY", result.OriginalSql);
        Assert.Contains("LIMIT", result.OriginalSql);
    }

    [Fact]
    public async Task ExplainAsync_WithGroupBy_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var result = await context.Events
            .GroupBy(e => e.Category)
            .Select(g => new { Category = g.Key, Total = g.Sum(e => e.Amount) })
            .ExplainAsync(context);

        Assert.NotNull(result);
        Assert.True(result.Output.Count > 0);
        Assert.Contains("GROUP BY", result.OriginalSql);
    }

    [Fact]
    public async Task ExplainResult_FormattedOutput_JoinsLinesCorrectly()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var result = await context.Events.ExplainAsync(context);

        // FormattedOutput should join all lines
        var expectedOutput = string.Join(Environment.NewLine, result.Output);
        Assert.Equal(expectedOutput, result.FormattedOutput);
    }

    [Fact]
    public async Task ExplainResult_ToString_ReturnsFormattedOutput()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var result = await context.Events.ExplainAsync(context);

        Assert.Equal(result.FormattedOutput, result.ToString());
    }

    [Fact]
    public async Task QueryStatistics_Summary_FormatsCorrectly()
    {
        var stats = new QueryStatistics
        {
            RowsRead = 1000,
            BytesRead = 1024 * 1024, // 1 MB
            QueryDurationMs = 150.5,
            MemoryUsage = 2 * 1024 * 1024, // 2 MB
            PeakMemoryUsage = 3 * 1024 * 1024 // 3 MB
        };

        var summary = stats.Summary;

        Assert.Contains("Rows read: 1,000", summary);
        Assert.Contains("1.00 MB", summary);
        Assert.Contains("150.50ms", summary);
        Assert.Contains("2.00 MB", summary);
        Assert.Contains("3.00 MB", summary);
    }

    private async Task InsertTestDataAsync(QueryProfilingTestDbContext context)
    {
        var events = Enumerable.Range(0, 100)
            .Select(i => new ProfilingTestEvent
            {
                Id = Guid.NewGuid(),
                Timestamp = DateTime.UtcNow.AddDays(-i),
                Category = i % 2 == 0 ? "CategoryA" : "CategoryB",
                Amount = i * 10.5m
            })
            .ToList();

        await context.BulkInsertAsync(events);
    }

    private QueryProfilingTestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<QueryProfilingTestDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new QueryProfilingTestDbContext(options);
    }
}

public class QueryProfilingTestDbContext : DbContext
{
    public QueryProfilingTestDbContext(DbContextOptions<QueryProfilingTestDbContext> options) : base(options) { }

    public DbSet<ProfilingTestEvent> Events => Set<ProfilingTestEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ProfilingTestEvent>(entity =>
        {
            entity.ToTable("Events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });
        });
    }
}

public class ProfilingTestEvent
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}
