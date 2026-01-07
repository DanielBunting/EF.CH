using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Features;

/// <summary>
/// Tests for window function SQL generation.
/// These tests verify that LINQ expressions using window functions
/// are correctly translated to ClickHouse SQL.
/// </summary>
public class WindowFunctionTests
{
    #region Ranking Function Tests

    [Fact]
    public void RowNumber_WithPartitionAndOrder_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            RowNum = Window.RowNumber()
                .PartitionBy(o.Region)
                .OrderBy(o.OrderDate)
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("row_number()", sql);
        Assert.Contains("OVER (", sql);
        Assert.Contains("PARTITION BY", sql);
        Assert.Contains("ORDER BY", sql);
    }

    [Fact]
    public void Rank_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            Ranking = Window.Rank()
                .PartitionBy(o.Region)
                .OrderBy(o.Amount)
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("rank()", sql);
        Assert.Contains("OVER (", sql);
    }

    [Fact]
    public void DenseRank_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            DenseRanking = Window.DenseRank()
                .OrderBy(o.Amount)
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("dense_rank()", sql);
        Assert.Contains("OVER (", sql);
    }

    [Fact]
    public void NTile_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            Bucket = Window.NTile(4)
                .OrderBy(o.Amount)
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("ntile(", sql);
        Assert.Contains("OVER (", sql);
    }

    #endregion

    #region Lag/Lead Tests

    [Fact]
    public void Lag_WithOffset_GeneratesLagInFrameWithFrame()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            PrevAmount = Window.Lag(o.Amount, 1)
                .OrderBy(o.OrderDate)
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("lagInFrame(", sql);
        Assert.Contains("OVER (", sql);
        Assert.Contains("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", sql);
    }

    [Fact]
    public void Lead_WithOffset_GeneratesLeadInFrameWithFrame()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            NextAmount = Window.Lead(o.Amount, 1)
                .OrderBy(o.OrderDate)
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("leadInFrame(", sql);
        Assert.Contains("OVER (", sql);
        Assert.Contains("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", sql);
    }

    [Fact]
    public void Lag_WithDefaultValue_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            PrevAmount = Window.Lag(o.Amount, 1, 0m)
                .OrderBy(o.OrderDate)
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("lagInFrame(", sql);
        // The default value should be in the arguments
        Assert.Contains("OVER (", sql);
    }

    [Fact]
    public void Lag_WithExplicitFrame_UsesProvidedFrame()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            PrevAmount = Window.Lag(o.Amount, 1)
                .OrderBy(o.OrderDate)
                .Rows().Preceding(5).CurrentRow()
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("lagInFrame(", sql);
        Assert.Contains("ROWS BETWEEN", sql);
        Assert.Contains("5 PRECEDING", sql);
        Assert.Contains("CURRENT ROW", sql);
    }

    #endregion

    #region Value Function Tests

    [Fact]
    public void FirstValue_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            FirstAmount = Window.FirstValue(o.Amount)
                .PartitionBy(o.Region)
                .OrderBy(o.OrderDate)
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("first_value(", sql);
        Assert.Contains("OVER (", sql);
    }

    [Fact]
    public void LastValue_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            LastAmount = Window.LastValue(o.Amount)
                .PartitionBy(o.Region)
                .OrderBy(o.OrderDate)
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("last_value(", sql);
        Assert.Contains("OVER (", sql);
    }

    #endregion

    #region Aggregate Window Function Tests

    [Fact]
    public void WindowSum_GeneratesCorrectFrameClause()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            RunningTotal = Window.Sum(o.Amount)
                .PartitionBy(o.Region)
                .OrderBy(o.OrderDate)
                .Rows().UnboundedPreceding().CurrentRow()
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("sum(", sql);
        Assert.Contains("OVER (", sql);
        Assert.Contains("PARTITION BY", sql);
        Assert.Contains("ORDER BY", sql);
        Assert.Contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", sql);
    }

    [Fact]
    public void WindowAvg_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            MovingAvg = Window.Avg(o.Amount)
                .OrderBy(o.OrderDate)
                .Rows().Preceding(3).CurrentRow()
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("avg(", sql);
        Assert.Contains("OVER (", sql);
        Assert.Contains("ROWS BETWEEN 3 PRECEDING AND CURRENT ROW", sql);
    }

    [Fact]
    public void WindowCount_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            RunningCount = Window.Count(o.Id)
                .OrderBy(o.OrderDate)
                .Rows().UnboundedPreceding().CurrentRow()
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("count(", sql);
        Assert.Contains("OVER (", sql);
    }

    [Fact]
    public void WindowMin_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            MinAmount = Window.Min(o.Amount)
                .PartitionBy(o.Region)
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("min(", sql);
        Assert.Contains("OVER (", sql);
    }

    [Fact]
    public void WindowMax_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            MaxAmount = Window.Max(o.Amount)
                .PartitionBy(o.Region)
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("max(", sql);
        Assert.Contains("OVER (", sql);
    }

    #endregion

    #region Complex Scenarios

    [Fact]
    public void MultipleWindowFunctions_InSameSelect_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            RowNum = Window.RowNumber()
                .PartitionBy(o.Region)
                .OrderBy(o.OrderDate)
                .Build(),
            RunningTotal = Window.Sum(o.Amount)
                .PartitionBy(o.Region)
                .OrderBy(o.OrderDate)
                .Rows().UnboundedPreceding().CurrentRow()
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("row_number()", sql);
        Assert.Contains("sum(", sql);
        // Both should have their own OVER clauses
        Assert.Contains("OVER (PARTITION BY", sql);
    }

    [Fact]
    public void WindowFunction_WithMultiplePartitionColumns_GeneratesCorrectly()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            RowNum = Window.RowNumber()
                .PartitionBy(o.Region)
                .PartitionBy(o.CustomerId)
                .OrderBy(o.OrderDate)
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("row_number()", sql);
        Assert.Contains("PARTITION BY", sql);
        // Both partition columns should be present
        Assert.Matches(@"PARTITION BY[^)]*Region", sql);
        Assert.Matches(@"PARTITION BY[^)]*CustomerId", sql);
    }

    [Fact]
    public void WindowFunction_WithDescendingOrder_GeneratesDescCorrectly()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            RowNum = Window.RowNumber()
                .OrderByDescending(o.Amount)
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("row_number()", sql);
        Assert.Contains("ORDER BY", sql);
        Assert.Contains("DESC", sql);
    }

    [Fact]
    public void WindowFunction_WithRangeFrame_GeneratesRangeClause()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            RangeSum = Window.Sum(o.Amount)
                .OrderBy(o.OrderDate)
                .Range().UnboundedPreceding().CurrentRow()
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("sum(", sql);
        Assert.Contains("RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", sql);
    }

    #endregion

    #region Lambda-Style API Tests

    [Fact]
    public void LambdaStyle_RowNumber_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            RowNum = Window.RowNumber(w => w
                .PartitionBy(o.Region)
                .OrderBy(o.OrderDate))
        });

        var sql = query.ToQueryString();

        Assert.Contains("row_number()", sql);
        Assert.Contains("OVER (", sql);
        Assert.Contains("PARTITION BY", sql);
        Assert.Contains("ORDER BY", sql);
    }

    [Fact]
    public void LambdaStyle_Lag_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            PrevAmount = Window.Lag(o.Amount, 1, w => w
                .OrderBy(o.OrderDate))
        });

        var sql = query.ToQueryString();

        Assert.Contains("lagInFrame(", sql);
        Assert.Contains("OVER (", sql);
        Assert.Contains("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", sql);
    }

    [Fact]
    public void LambdaStyle_Sum_WithFrame_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            RunningTotal = Window.Sum(o.Amount, w => w
                .PartitionBy(o.Region)
                .OrderBy(o.OrderDate)
                .Rows().UnboundedPreceding().CurrentRow())
        });

        var sql = query.ToQueryString();

        Assert.Contains("sum(", sql);
        Assert.Contains("OVER (", sql);
        Assert.Contains("PARTITION BY", sql);
        Assert.Contains("ORDER BY", sql);
        Assert.Contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", sql);
    }

    [Fact]
    public void LambdaStyle_Avg_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            MovingAvg = Window.Avg(o.Amount, w => w
                .OrderBy(o.OrderDate)
                .Rows().Preceding(3).CurrentRow())
        });

        var sql = query.ToQueryString();

        Assert.Contains("avg(", sql);
        Assert.Contains("OVER (", sql);
        Assert.Contains("ROWS BETWEEN 3 PRECEDING AND CURRENT ROW", sql);
    }

    [Fact]
    public void LambdaStyle_DenseRank_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            DenseRanking = Window.DenseRank(w => w
                .OrderBy(o.Amount))
        });

        var sql = query.ToQueryString();

        Assert.Contains("dense_rank()", sql);
        Assert.Contains("OVER (", sql);
    }

    [Fact]
    public void LambdaStyle_NTile_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            Bucket = Window.NTile(4, w => w
                .OrderBy(o.Amount))
        });

        var sql = query.ToQueryString();

        Assert.Contains("ntile(", sql);
        Assert.Contains("OVER (", sql);
    }

    [Fact]
    public void LambdaStyle_Lead_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            NextAmount = Window.Lead(o.Amount, 1, w => w
                .OrderBy(o.OrderDate))
        });

        var sql = query.ToQueryString();

        Assert.Contains("leadInFrame(", sql);
        Assert.Contains("OVER (", sql);
    }

    [Fact]
    public void LambdaStyle_FirstValue_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            FirstAmount = Window.FirstValue(o.Amount, w => w
                .PartitionBy(o.Region)
                .OrderBy(o.OrderDate))
        });

        var sql = query.ToQueryString();

        Assert.Contains("first_value(", sql);
        Assert.Contains("OVER (", sql);
    }

    [Fact]
    public void LambdaStyle_Count_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            RunningCount = Window.Count(o.Id, w => w
                .OrderBy(o.OrderDate)
                .Rows().UnboundedPreceding().CurrentRow())
        });

        var sql = query.ToQueryString();

        Assert.Contains("count(", sql);
        Assert.Contains("OVER (", sql);
    }

    [Fact]
    public void LambdaStyle_Min_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            MinAmount = Window.Min(o.Amount, w => w
                .PartitionBy(o.Region))
        });

        var sql = query.ToQueryString();

        Assert.Contains("min(", sql);
        Assert.Contains("OVER (", sql);
    }

    [Fact]
    public void LambdaStyle_Max_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            MaxAmount = Window.Max(o.Amount, w => w
                .PartitionBy(o.Region))
        });

        var sql = query.ToQueryString();

        Assert.Contains("max(", sql);
        Assert.Contains("OVER (", sql);
    }

    [Fact]
    public void LambdaStyle_OrderByDescending_GeneratesDescCorrectly()
    {
        using var context = CreateContext();

        var query = context.Orders.Select(o => new
        {
            o.Id,
            RowNum = Window.RowNumber(w => w
                .OrderByDescending(o.Amount))
        });

        var sql = query.ToQueryString();

        Assert.Contains("row_number()", sql);
        Assert.Contains("ORDER BY", sql);
        Assert.Contains("DESC", sql);
    }

    [Fact]
    public void MixedStyle_BothApisInSameQuery_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        // Mix fluent style and lambda style in the same query
        var query = context.Orders.Select(o => new
        {
            o.Id,
            // Lambda style
            RowNum = Window.RowNumber(w => w
                .PartitionBy(o.Region)
                .OrderBy(o.OrderDate)),
            // Fluent style
            RunningTotal = Window.Sum(o.Amount)
                .PartitionBy(o.Region)
                .OrderBy(o.OrderDate)
                .Rows().UnboundedPreceding().CurrentRow()
                .Build()
        });

        var sql = query.ToQueryString();

        Assert.Contains("row_number()", sql);
        Assert.Contains("sum(", sql);
        Assert.Contains("PARTITION BY", sql);
        Assert.Contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", sql);
    }

    #endregion

    #region Test Infrastructure

    private static WindowFunctionTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<WindowFunctionTestContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return new WindowFunctionTestContext(options);
    }

    #endregion
}

#region Test Entities

public class WindowOrder
{
    public Guid Id { get; set; }
    public string Region { get; set; } = string.Empty;
    public Guid CustomerId { get; set; }
    public DateTime OrderDate { get; set; }
    public decimal Amount { get; set; }
}

#endregion

#region Test Context

public class WindowFunctionTestContext : DbContext
{
    public WindowFunctionTestContext(DbContextOptions<WindowFunctionTestContext> options)
        : base(options)
    {
    }

    public DbSet<WindowOrder> Orders => Set<WindowOrder>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<WindowOrder>(entity =>
        {
            entity.ToTable("orders");
            entity.HasKey(e => e.Id);
        });
    }
}

#endregion
