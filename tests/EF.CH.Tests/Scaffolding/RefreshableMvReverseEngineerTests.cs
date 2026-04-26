using EF.CH.Scaffolding.Internal;
using Xunit;

namespace EF.CH.Tests.Scaffolding;

/// <summary>
/// Pure-unit coverage of the regex parser that pulls REFRESH clauses out of
/// <c>system.tables.engine_full</c>. The integration round-trip (real CH →
/// scaffolded model) is deferred to the SystemTests project.
/// </summary>
public class RefreshableMvReverseEngineerTests
{
    [Fact]
    public void Parses_EveryClause()
    {
        var info = ClickHouseDatabaseModelFactory.ParseRefreshClause(
            "MergeTree REFRESH EVERY 5 MINUTE ORDER BY id");
        Assert.NotNull(info);
        Assert.Equal("EVERY", info!.Kind);
        Assert.Equal("5 MINUTE", info.Interval);
    }

    [Fact]
    public void Parses_AfterClauseWithOffsetAndRandomize()
    {
        var info = ClickHouseDatabaseModelFactory.ParseRefreshClause(
            "MergeTree REFRESH AFTER 1 HOUR OFFSET 1 MINUTE RANDOMIZE FOR 30 SECOND ORDER BY id");
        Assert.NotNull(info);
        Assert.Equal("AFTER", info!.Kind);
        Assert.Equal("1 HOUR", info.Interval);
        Assert.Equal("1 MINUTE", info.Offset);
        Assert.Equal("30 SECOND", info.RandomizeFor);
    }

    [Fact]
    public void Parses_DependsOnList()
    {
        var info = ClickHouseDatabaseModelFactory.ParseRefreshClause(
            "MergeTree REFRESH EVERY 5 MINUTE DEPENDS ON db.mv_a, db.mv_b ORDER BY id");
        Assert.NotNull(info);
        Assert.NotNull(info!.DependsOn);
        Assert.Contains("db.mv_a", info.DependsOn!);
        Assert.Contains("db.mv_b", info.DependsOn!);
    }

    [Fact]
    public void Parses_AppendFlag()
    {
        var info = ClickHouseDatabaseModelFactory.ParseRefreshClause(
            "MergeTree REFRESH EVERY 5 MINUTE APPEND ORDER BY id");
        Assert.NotNull(info);
        Assert.True(info!.Append);
    }

    [Fact]
    public void Parses_ToTarget()
    {
        var info = ClickHouseDatabaseModelFactory.ParseRefreshClause(
            "REFRESH EVERY 5 MINUTE TO target_table");
        Assert.NotNull(info);
        Assert.Equal("target_table", info!.Target);
    }

    [Fact]
    public void NoRefreshClause_ReturnsNull()
    {
        var info = ClickHouseDatabaseModelFactory.ParseRefreshClause("MergeTree ORDER BY id");
        Assert.Null(info);
    }

    [Fact]
    public void EmptyEngineFull_ReturnsNull()
    {
        Assert.Null(ClickHouseDatabaseModelFactory.ParseRefreshClause(""));
    }
}
