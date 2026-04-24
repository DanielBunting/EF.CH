using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization;

/// <summary>
/// Gap #12 — <c>DbSet.InsertFromQuery(IQueryable)</c> for AMT→AMT chaining and
/// the <c>-MergeState</c> combinator family (<c>countMergeState</c>,
/// <c>sumMergeState</c>, …) are not surfaced. See .tmp/notes/feature-gaps.md §12.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MergeStateAndInsertFromQueryTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MergeStateAndInsertFromQueryTests(SingleNodeClickHouseFixture fx) => _fixture = fx;

    [Fact]
    public void InsertFromQuery_ShouldBeDefined()
    {
        var found = typeof(ClickHouseEntityTypeBuilderExtensions).Assembly
            .GetTypes()
            .SelectMany(t => t.GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static))
            .Any(m => m.Name is "InsertFromQueryAsync" or "InsertFromQuery");
        Assert.True(found,
            "Expected a DbSet<T>.InsertFromQuery(IQueryable<T>) extension. " +
            "See .tmp/notes/feature-gaps.md §12.");
    }

    [Fact]
    public void CountMergeState_ShouldBeDeclaredOnClickHouseAggregates()
    {
        var method = typeof(ClickHouseAggregates).GetMethod(nameof(ClickHouseAggregates.CountMerge));
        Assert.NotNull(method);

        var mergeStateMethod = typeof(ClickHouseAggregates).GetMethod("CountMergeState");
        Assert.NotNull(mergeStateMethod);
    }

    [Fact]
    public void SumMergeState_UniqMergeState_ShouldBeDeclaredOnClickHouseAggregates()
    {
        Assert.NotNull(typeof(ClickHouseAggregates).GetMethod("SumMergeState"));
        Assert.NotNull(typeof(ClickHouseAggregates).GetMethod("UniqMergeState"));
    }
}
