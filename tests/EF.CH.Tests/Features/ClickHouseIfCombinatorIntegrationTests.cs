using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Projections;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

/// <summary>
/// End-to-end tests for the -If aggregate combinator family via the projection-DDL path
/// (the route used by materialized views and ClickHouse projections).
/// Covers the scenarios from features/01-if-aggregate-combinator.md that are reachable
/// through this path:
///   1. Single-pass sumIf/countIf emission (not sum(if(...)))
///   4. Multiple *If aggregates share a single GROUP BY pass
/// Plus an end-to-end execution test against a live ClickHouse container.
/// </summary>
public class ClickHouseIfCombinatorIntegrationTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    public async Task InitializeAsync() => await _container.StartAsync();
    public async Task DisposeAsync() => await _container.DisposeAsync();

    private string GetConnectionString() => _container.GetConnectionString();

    private TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse(GetConnectionString())
            .Options;
        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }

    [Fact]
    public void SumIf_EmitsNativeSumIf_NotSumOfIfExpression()
    {
        using var context = CreateContext<SumIfProjectionContext>();
        var projections = context.Model.FindEntityType(typeof(IfSalesRecord))!
            .FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        var sql = projections![0].SelectSql;

        Assert.Contains("sumIf(", sql);
        Assert.Contains("\"Status\" = 'paid'", sql);
        // Negative assertion: not the degraded form
        Assert.DoesNotContain("sum(if(", sql);
        Assert.DoesNotContain("sum(multiIf(", sql);
    }

    [Fact]
    public void MultipleIfAggregates_InOneProjection_ShareSingleGroupBy()
    {
        using var context = CreateContext<MultipleIfProjectionContext>();
        var projections = context.Model.FindEntityType(typeof(IfSalesRecord))!
            .FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        var sql = projections![0].SelectSql;

        // All three aggregates present in the same SELECT
        Assert.Contains("sumIf(", sql);
        Assert.Contains("countIf(", sql);
        Assert.Contains("uniqIf(", sql);

        // Single GROUP BY clause (one-pass)
        var groupByCount = CountOccurrences(sql, "GROUP BY");
        Assert.Equal(1, groupByCount);
    }

    [Fact]
    public async Task SumIf_CountIf_ExecuteCorrectly_AgainstLiveClickHouse()
    {
        await using var context = CreateContext<SumIfProjectionContext>();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS if_sales (
                Id UUID,
                Region String,
                Status String,
                Amount Decimal(18, 4)
            ) ENGINE = MergeTree()
            ORDER BY (Region, Id)
        ");

        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO if_sales (Id, Region, Status, Amount) VALUES
            (generateUUIDv4(), 'us-east', 'paid', 100.00),
            (generateUUIDv4(), 'us-east', 'paid', 200.00),
            (generateUUIDv4(), 'us-east', 'free', 0.00),
            (generateUUIDv4(), 'eu-west', 'paid', 50.00),
            (generateUUIDv4(), 'eu-west', 'error', 0.00)
        ");

        // Verify the raw -If combinator SQL executes against ClickHouse and produces correct results.
        var results = await context.Database
            .SqlQueryRaw<RegionStatsResult>(
                @"SELECT Region,
                         sumIf(Amount, Status = 'paid') AS PaidTotal,
                         countIf(Status = 'free') AS FreeCount
                  FROM if_sales
                  GROUP BY Region
                  ORDER BY Region")
            .ToListAsync();

        Assert.Equal(2, results.Count);
        var eu = results.First(r => r.Region == "eu-west");
        Assert.Equal(50m, eu.PaidTotal);
        Assert.Equal(0UL, eu.FreeCount);
        var us = results.First(r => r.Region == "us-east");
        Assert.Equal(300m, us.PaidTotal);
        Assert.Equal(1UL, us.FreeCount);
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) != -1)
        {
            count++;
            index += needle.Length;
        }
        return count;
    }
}

#region Test Entities / Result Types

public class IfSalesRecord
{
    public Guid Id { get; set; }
    public string Region { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

public class RegionStatsResult
{
    public string Region { get; set; } = string.Empty;
    public decimal PaidTotal { get; set; }
    public ulong FreeCount { get; set; }
}

#endregion

#region Test Contexts

public class SumIfProjectionContext : DbContext
{
    public SumIfProjectionContext(DbContextOptions<SumIfProjectionContext> options) : base(options) { }
    public DbSet<IfSalesRecord> Sales => Set<IfSalesRecord>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<IfSalesRecord>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("if_sales");
            entity.UseMergeTree(x => new { x.Region, x.Id });
            entity.HasProjection()
                .GroupBy(s => s.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    PaidRevenue = ClickHouseAggregates.SumIf(g, s => s.Amount, s => s.Status == "paid")
                })
                .Build();
        });
    }
}

public class MultipleIfProjectionContext : DbContext
{
    public MultipleIfProjectionContext(DbContextOptions<MultipleIfProjectionContext> options) : base(options) { }
    public DbSet<IfSalesRecord> Sales => Set<IfSalesRecord>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<IfSalesRecord>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("if_sales");
            entity.UseMergeTree(x => new { x.Region, x.Id });
            entity.HasProjection()
                .GroupBy(s => s.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    PaidRevenue = ClickHouseAggregates.SumIf(g, s => s.Amount, s => s.Status == "paid"),
                    FreeCount = ClickHouseAggregates.CountIf(g, s => s.Status == "free"),
                    UniqPaid = ClickHouseAggregates.UniqIf(g, s => s.Id, s => s.Status == "paid")
                })
                .Build();
        });
    }
}

#endregion
