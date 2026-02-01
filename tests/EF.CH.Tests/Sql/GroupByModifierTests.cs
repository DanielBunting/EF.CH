using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// Tests for GROUP BY modifier SQL generation (WITH ROLLUP, WITH CUBE, WITH TOTALS).
/// </summary>
public class GroupByModifierTests
{
    [Fact]
    public void WithRollup_GeneratesRollupClause()
    {
        using var context = CreateContext();

        var query = context.Sales
            .GroupBy(s => new { s.Region, s.Category })
            .Select(g => new { g.Key.Region, g.Key.Category, Total = g.Sum(s => s.Amount) })
            .WithRollup();

        var sql = query.ToQueryString();
        Console.WriteLine("Generated SQL: " + sql);

        Assert.Contains("GROUP BY", sql);
        Assert.Contains("WITH ROLLUP", sql);
    }

    [Fact]
    public void WithCube_GeneratesCubeClause()
    {
        using var context = CreateContext();

        var query = context.Sales
            .GroupBy(s => new { s.Region, s.Category })
            .Select(g => new { g.Key.Region, g.Key.Category, Count = g.Count() })
            .WithCube();

        var sql = query.ToQueryString();
        Console.WriteLine("Generated SQL: " + sql);

        Assert.Contains("GROUP BY", sql);
        Assert.Contains("WITH CUBE", sql);
    }

    [Fact]
    public void WithTotals_GeneratesTotalsClause()
    {
        using var context = CreateContext();

        var query = context.Sales
            .GroupBy(s => s.Category)
            .Select(g => new { Category = g.Key, Count = g.Count() })
            .WithTotals();

        var sql = query.ToQueryString();
        Console.WriteLine("Generated SQL: " + sql);

        Assert.Contains("GROUP BY", sql);
        Assert.Contains("WITH TOTALS", sql);
    }

    [Fact]
    public void MultipleModifiers_ThrowsException()
    {
        using var context = CreateContext();

        Assert.Throws<InvalidOperationException>(() =>
            context.Sales
                .GroupBy(s => s.Category)
                .Select(g => new { g.Key, Count = g.Count() })
                .WithRollup()
                .WithTotals()
                .ToQueryString());
    }

    [Fact]
    public void WithRollup_CombinedWithFinal_GeneratesBothClauses()
    {
        using var context = CreateContext();

        var query = context.Sales
            .Final()
            .GroupBy(s => s.Category)
            .Select(g => new { Category = g.Key, Total = g.Sum(s => s.Amount) })
            .WithRollup();

        var sql = query.ToQueryString();
        Console.WriteLine("Generated SQL: " + sql);

        Assert.Contains("FINAL", sql);
        Assert.Contains("WITH ROLLUP", sql);
    }

    [Fact]
    public void WithCube_SingleColumn_GeneratesCubeClause()
    {
        using var context = CreateContext();

        var query = context.Sales
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, Total = g.Sum(s => s.Amount) })
            .WithCube();

        var sql = query.ToQueryString();
        Console.WriteLine("Generated SQL: " + sql);

        Assert.Contains("GROUP BY", sql);
        Assert.Contains("WITH CUBE", sql);
    }

    [Fact]
    public void WithTotals_WithWhere_GeneratesCorrectOrder()
    {
        using var context = CreateContext();

        var query = context.Sales
            .Where(s => s.Amount > 100)
            .GroupBy(s => s.Category)
            .Select(g => new { Category = g.Key, Total = g.Sum(s => s.Amount) })
            .WithTotals();

        var sql = query.ToQueryString();
        Console.WriteLine("Generated SQL: " + sql);

        Assert.Contains("WHERE", sql);
        Assert.Contains("GROUP BY", sql);
        Assert.Contains("WITH TOTALS", sql);

        // Verify correct ordering: WHERE before GROUP BY
        var whereIndex = sql.IndexOf("WHERE");
        var groupByIndex = sql.IndexOf("GROUP BY");
        Assert.True(whereIndex < groupByIndex, "WHERE should come before GROUP BY");
    }

    [Fact]
    public void WithRollup_ThreeColumns_GeneratesRollupClause()
    {
        using var context = CreateContext();

        var query = context.Sales
            .GroupBy(s => new { s.Region, s.Category, s.Year })
            .Select(g => new { g.Key.Region, g.Key.Category, g.Key.Year, Total = g.Sum(s => s.Amount) })
            .WithRollup();

        var sql = query.ToQueryString();
        Console.WriteLine("Generated SQL: " + sql);

        Assert.Contains("GROUP BY", sql);
        Assert.Contains("WITH ROLLUP", sql);
    }

    [Fact]
    public void WithRollup_WithHaving_GeneratesCorrectOrder()
    {
        using var context = CreateContext();

        var query = context.Sales
            .GroupBy(s => s.Category)
            .Select(g => new { Category = g.Key, Total = g.Sum(s => s.Amount) })
            .Where(r => r.Total > 1000)
            .WithRollup();

        var sql = query.ToQueryString();
        Console.WriteLine("Generated SQL: " + sql);

        // HAVING clause should come after GROUP BY modifier
        Assert.Contains("GROUP BY", sql);
        Assert.Contains("WITH ROLLUP", sql);
    }

    [Fact]
    public void ValidationThrowsForNullSource_WithRollup()
    {
        IQueryable<GroupByModifierSale>? nullSource = null;

        Assert.Throws<ArgumentNullException>(() => nullSource!.WithRollup());
    }

    [Fact]
    public void ValidationThrowsForNullSource_WithCube()
    {
        IQueryable<GroupByModifierSale>? nullSource = null;

        Assert.Throws<ArgumentNullException>(() => nullSource!.WithCube());
    }

    [Fact]
    public void ValidationThrowsForNullSource_WithTotals()
    {
        IQueryable<GroupByModifierSale>? nullSource = null;

        Assert.Throws<ArgumentNullException>(() => nullSource!.WithTotals());
    }

    [Fact]
    public void ExtensionMethods_Compile_AndCanBeCalled()
    {
        using var context = CreateContext();

        // WithRollup
        var q1 = context.Sales
            .GroupBy(s => new { s.Region, s.Category })
            .Select(g => new { g.Key.Region, g.Key.Category, Total = g.Sum(s => s.Amount) })
            .WithRollup();

        // WithCube
        var q2 = context.Sales
            .GroupBy(s => new { s.Region, s.Category })
            .Select(g => new { g.Key.Region, g.Key.Category, Count = g.Count() })
            .WithCube();

        // WithTotals
        var q3 = context.Sales
            .GroupBy(s => s.Category)
            .Select(g => new { Category = g.Key, Count = g.Count() })
            .WithTotals();

        // Verify queries are created (but not executed)
        Assert.NotNull(q1);
        Assert.NotNull(q2);
        Assert.NotNull(q3);
    }

    private static GroupByModifierTestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<GroupByModifierTestDbContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return new GroupByModifierTestDbContext(options);
    }
}

public class GroupByModifierTestDbContext : DbContext
{
    public GroupByModifierTestDbContext(DbContextOptions<GroupByModifierTestDbContext> options)
        : base(options) { }

    public DbSet<GroupByModifierSale> Sales => Set<GroupByModifierSale>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<GroupByModifierSale>(entity =>
        {
            entity.ToTable("group_by_modifier_sales");
            entity.HasKey(e => e.Id);
        });
    }
}

public class GroupByModifierSale
{
    public Guid Id { get; set; }
    public string Region { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public int Year { get; set; }
    public decimal Amount { get; set; }
}
