using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// Verifies that the new <see cref="ClickHouseAggregates"/> extension methods
/// (<c>g.CountIf(...)</c>, <c>g.SumIf(...)</c>, <c>g.UniqCombined(...)</c>, etc.)
/// translate when used in runtime <c>IQueryable.GroupBy(...).Select(...)</c>
/// queries — the wiring lives in
/// <see cref="EF.CH.Query.Internal.ClickHouseQueryTranslationPreprocessor"/>'s
/// <c>MergeRewrites</c> table.
/// </summary>
public class RuntimeAggregateTests
{
    [Fact]
    public void CountIf_OnGrouping_TranslatesToCountIf()
    {
        using var ctx = CreateContext();
        var query = ctx.Orders
            .GroupBy(o => o.Region)
            .Select(g => new { g.Key, Errors = g.CountIf(o => o.Status == "error") });

        var sql = query.ToQueryString();

        Assert.Contains("countIf(", sql);
    }

    [Fact]
    public void SumIf_OnGrouping_TranslatesToSumIf()
    {
        using var ctx = CreateContext();
        var query = ctx.Orders
            .GroupBy(o => o.Region)
            .Select(g => new { g.Key, Paid = g.SumIf(o => o.Amount, o => o.Status == "paid") });

        var sql = query.ToQueryString();

        Assert.Contains("sumIf(", sql);
    }

    [Fact]
    public void AvgIf_OnGrouping_TranslatesToAvgIf()
    {
        using var ctx = CreateContext();
        var query = ctx.Orders
            .GroupBy(o => o.Region)
            .Select(g => new { g.Key, Avg = g.AvgIf(o => (double)o.Amount, o => o.Status == "paid") });

        var sql = query.ToQueryString();

        Assert.Contains("avgIf(", sql);
    }

    [Fact]
    public void MinIf_OnGrouping_TranslatesToMinIf()
    {
        using var ctx = CreateContext();
        var query = ctx.Orders
            .GroupBy(o => o.Region)
            .Select(g => new { g.Key, Min = g.MinIf(o => o.Amount, o => o.Status == "paid") });

        var sql = query.ToQueryString();

        Assert.Contains("minIf(", sql);
    }

    [Fact]
    public void MaxIf_OnGrouping_TranslatesToMaxIf()
    {
        using var ctx = CreateContext();
        var query = ctx.Orders
            .GroupBy(o => o.Region)
            .Select(g => new { g.Key, Max = g.MaxIf(o => o.Amount, o => o.Status == "paid") });

        var sql = query.ToQueryString();

        Assert.Contains("maxIf(", sql);
    }

    [Fact]
    public void UniqCombined_OnGrouping_TranslatesToUniqCombined()
    {
        using var ctx = CreateContext();
        var query = ctx.Orders
            .GroupBy(o => o.Region)
            .Select(g => new { g.Key, Uniq = g.UniqCombined(o => o.CustomerId) });

        var sql = query.ToQueryString();

        Assert.Contains("uniqCombined(", sql);
    }

    [Fact]
    public void CountUInt64_OnGrouping_EmitsCountReturningUInt64()
    {
        using var ctx = CreateContext();
        var query = ctx.Orders
            .GroupBy(o => o.Region)
            .Select(g => new { g.Key, N = g.CountUInt64() });

        var sql = query.ToQueryString();

        // count() (or count(1) — semantically equivalent in ClickHouse) emits to UInt64
        Assert.Contains("count(", sql);
    }

    [Fact]
    public void SumUInt64_OnGrouping_EmitsSumReturningUInt64()
    {
        using var ctx = CreateContext();
        var query = ctx.Orders
            .GroupBy(o => o.Region)
            .Select(g => new { g.Key, Total = g.SumUInt64(o => o.Quantity) });

        var sql = query.ToQueryString();

        // The runtime-LINQ path goes through EF Core's standard Sum + Convert pipeline.
        // The emitted SQL is sum-flavoured (sumOrNull) cast to UInt64; both signal that
        // the user got an explicit ulong column without paying the (ulong) cast tax.
        Assert.Contains("sum", sql);
        Assert.Contains("UInt64", sql);
    }

    [Fact]
    public void CountMerge_OnStateGrouping_TranslatesToCountMerge()
    {
        using var ctx = CreateContext();
        var query = ctx.OrderStates
            .GroupBy(o => o.Region)
            .Select(g => new { g.Key, Merged = g.CountMerge(s => s.CountSt) });

        var sql = query.ToQueryString();

        Assert.Contains("countMerge(", sql);
    }

    private static AggregateRuntimeContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<AggregateRuntimeContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;
        return new AggregateRuntimeContext(options);
    }
}

public class AggregateRuntimeContext : DbContext
{
    public AggregateRuntimeContext(DbContextOptions<AggregateRuntimeContext> options) : base(options) { }

    public DbSet<RuntimeOrder> Orders => Set<RuntimeOrder>();
    public DbSet<RuntimeOrderState> OrderStates => Set<RuntimeOrderState>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<RuntimeOrder>(e =>
        {
            e.HasKey(o => o.Id);
            e.ToTable("orders_runtime");
        });

        modelBuilder.Entity<RuntimeOrderState>(e =>
        {
            e.HasKey(o => o.Id);
            e.ToTable("orders_runtime_state");
        });
    }
}

public class RuntimeOrder
{
    public long Id { get; set; }
    public string Region { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public long CustomerId { get; set; }
    public long Amount { get; set; }
    public int Quantity { get; set; }
}

public class RuntimeOrderState
{
    public long Id { get; set; }
    public string Region { get; set; } = string.Empty;
    public byte[] CountSt { get; set; } = [];
}
