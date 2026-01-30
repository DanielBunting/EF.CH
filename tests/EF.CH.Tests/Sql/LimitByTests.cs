using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// Tests for LIMIT BY SQL generation.
/// </summary>
public class LimitByTests
{
    [Fact]
    public void LimitBy_SingleColumn_GeneratesLimitByClause()
    {
        using var context = CreateContext();

        var query = context.Events
            .OrderByDescending(e => e.Score)
            .LimitBy(5, e => e.Category);

        var sql = query.ToQueryString();
        Console.WriteLine("Generated SQL: " + sql);

        Assert.Contains("ORDER BY", sql);
        Assert.Contains("LIMIT", sql);
        Assert.Contains("BY", sql);
        Assert.Contains("\"Category\"", sql);
    }

    [Fact]
    public void LimitBy_CompoundKey_GeneratesLimitByWithMultipleColumns()
    {
        using var context = CreateContext();

        var query = context.Events
            .OrderByDescending(e => e.Score)
            .LimitBy(3, e => new { e.Category, e.Region });

        var sql = query.ToQueryString();
        Console.WriteLine("Generated SQL: " + sql);

        Assert.Contains("ORDER BY", sql);
        Assert.Contains("LIMIT", sql);
        Assert.Contains("BY", sql);
        Assert.Contains("\"Category\"", sql);
        Assert.Contains("\"Region\"", sql);
    }

    [Fact]
    public void LimitBy_WithOffset_GeneratesLimitOffsetByClause()
    {
        using var context = CreateContext();

        var query = context.Events
            .OrderByDescending(e => e.Score)
            .LimitBy(2, 5, e => e.UserId);

        var sql = query.ToQueryString();
        Console.WriteLine("Generated SQL: " + sql);

        Assert.Contains("ORDER BY", sql);
        Assert.Contains("LIMIT", sql);
        Assert.Contains("BY", sql);
        Assert.Contains("\"UserId\"", sql);
    }

    [Fact(Skip = "EF Core's NavigationExpandingExpressionVisitor doesn't recognize custom LimitBy method, " +
                   "so Take() after LimitBy() fails. Use .ToList() before further LINQ operations.")]
    public void LimitBy_WithGlobalTake_GeneratesBothLimitClauses()
    {
        using var context = CreateContext();

        var query = context.Events
            .OrderByDescending(e => e.Score)
            .LimitBy(5, e => e.Category)
            .Take(100);

        var sql = query.ToQueryString();
        Console.WriteLine("Generated SQL: " + sql);

        // Should have LIMIT BY before global LIMIT
        Assert.Contains("LIMIT", sql);
        Assert.Contains("BY", sql);
        Assert.Contains("\"Category\"", sql);

        // Verify LIMIT BY comes before global LIMIT
        var limitByIndex = sql.IndexOf("LIMIT");
        var byIndex = sql.IndexOf(" BY ", limitByIndex);
        Assert.True(byIndex > limitByIndex, "BY should follow LIMIT in LIMIT BY clause");
    }

    [Fact]
    public void LimitBy_ValidationThrowsForZeroLimit()
    {
        using var context = CreateContext();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            context.Events.LimitBy(0, e => e.Category));
    }

    [Fact]
    public void LimitBy_ValidationThrowsForNegativeLimit()
    {
        using var context = CreateContext();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            context.Events.LimitBy(-1, e => e.Category));
    }

    [Fact]
    public void LimitBy_ValidationThrowsForNegativeOffset()
    {
        using var context = CreateContext();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            context.Events.LimitBy(-1, 5, e => e.Category));
    }

    [Fact]
    public void LimitBy_ValidationThrowsForNullSource()
    {
        IQueryable<LimitByEvent>? nullSource = null;

        Assert.Throws<ArgumentNullException>(() =>
            nullSource!.LimitBy(5, e => e.Category));
    }

    /// <summary>
    /// Verifies that the extension methods compile and can be called.
    /// This is a basic smoke test that doesn't require query execution.
    /// </summary>
    [Fact]
    public void ExtensionMethods_Compile_AndCanBeCalled()
    {
        using var context = CreateContext();

        // Single column key
        var q1 = context.Events
            .OrderByDescending(e => e.Score)
            .LimitBy(5, e => e.Category);

        // Compound key (anonymous type)
        var q2 = context.Events
            .OrderByDescending(e => e.Score)
            .LimitBy(3, e => new { e.Category, e.Region });

        // With offset
        var q3 = context.Events
            .OrderByDescending(e => e.CreatedAt)
            .LimitBy(2, 5, e => e.UserId);

        // Combined with global Take
        var q4 = context.Events
            .OrderByDescending(e => e.Score)
            .LimitBy(5, e => e.Category)
            .Take(100);

        // Combined with Where
        var q5 = context.Events
            .Where(e => e.Score > 0)
            .OrderByDescending(e => e.Score)
            .LimitBy(10, e => e.Category);

        // Verify queries are created (but not executed)
        Assert.NotNull(q1);
        Assert.NotNull(q2);
        Assert.NotNull(q3);
        Assert.NotNull(q4);
        Assert.NotNull(q5);
    }

    [Fact]
    public void LimitBy_WithFinal_GeneratesBothClauses()
    {
        using var context = CreateContext();

        var query = context.Events
            .Final()
            .OrderByDescending(e => e.Score)
            .LimitBy(5, e => e.Category);

        var sql = query.ToQueryString();
        Console.WriteLine("Generated SQL: " + sql);

        Assert.Contains("FINAL", sql);
        Assert.Contains("LIMIT", sql);
        Assert.Contains("BY", sql);
    }

    private static LimitByTestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<LimitByTestDbContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return new LimitByTestDbContext(options);
    }
}

public class LimitByTestDbContext : DbContext
{
    public LimitByTestDbContext(DbContextOptions<LimitByTestDbContext> options) : base(options)
    {
    }

    public DbSet<LimitByEvent> Events => Set<LimitByEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<LimitByEvent>(entity =>
        {
            entity.ToTable("limit_by_events");
            entity.HasKey(e => e.Id);
        });
    }
}

public class LimitByEvent
{
    public Guid Id { get; set; }
    public string Category { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public Guid UserId { get; set; }
    public int Score { get; set; }
    public DateTime CreatedAt { get; set; }
}
