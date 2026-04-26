using EF.CH.Metadata;
using EF.CH.Migrations.Design;
using EF.CH.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Splitter behaviour for refreshable materialized views: dependency ordering on
/// <c>DEPENDS ON</c>, on the <c>TO &lt;target&gt;</c> table, and the new
/// <see cref="MigrationPhase.ModifyMvSchedule"/> phase for
/// <see cref="ModifyRefreshOperation"/>.
/// </summary>
public class RefreshableMvSplitterTests
{
    private readonly ClickHouseMigrationsSplitter _splitter = new();

    private static CreateTableOperation MakeRefreshableMv(string name, string source, string interval, string[]? dependsOn = null, string? target = null)
    {
        var op = new CreateTableOperation { Name = name };
        op.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        op.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, source);
        op.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, $"SELECT * FROM {source}");
        op.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshKind, "EVERY");
        op.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshInterval, interval);
        if (dependsOn is { Length: > 0 })
            op.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshDependsOn, dependsOn);
        if (target is not null)
            op.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshTarget, target);
        return op;
    }

    [Fact]
    public void DependsOn_OrdersAfterReferencedMv()
    {
        var ops = new List<MigrationOperation>
        {
            MakeRefreshableMv("Mv_B", "Source", "5 MINUTE", dependsOn: new[] { "Mv_A" }),
            MakeRefreshableMv("Mv_A", "Source", "5 MINUTE"),
            new CreateTableOperation { Name = "Source" },
        };

        var result = _splitter.Split(ops);

        var indexA = result.ToList().FindIndex(s => (s.Operation as CreateTableOperation)?.Name == "Mv_A");
        var indexB = result.ToList().FindIndex(s => (s.Operation as CreateTableOperation)?.Name == "Mv_B");
        Assert.True(indexA >= 0 && indexB >= 0);
        Assert.True(indexA < indexB, "Mv_A should be created before Mv_B (which depends on it).");
    }

    [Fact]
    public void RefreshTarget_OrdersAfterTargetTable()
    {
        var ops = new List<MigrationOperation>
        {
            MakeRefreshableMv("Mv_With_Target", "Source", "5 MINUTE", target: "TargetTbl"),
            new CreateTableOperation { Name = "TargetTbl" },
            new CreateTableOperation { Name = "Source" },
        };

        var result = _splitter.Split(ops);
        var idxTarget = result.ToList().FindIndex(s => (s.Operation as CreateTableOperation)?.Name == "TargetTbl");
        var idxMv = result.ToList().FindIndex(s => (s.Operation as CreateTableOperation)?.Name == "Mv_With_Target");
        Assert.True(idxTarget < idxMv, "Target table must be created before the MV that writes into it.");
    }

    [Fact]
    public void ModifyRefresh_PlacedInLatePhase()
    {
        var ops = new List<MigrationOperation>
        {
            new ModifyRefreshOperation { Name = "SomeMv", Kind = "EVERY", Interval = "1 HOUR" },
            new CreateTableOperation { Name = "AnotherTable" },
        };

        var result = _splitter.Split(ops);
        var idxModify = result.ToList().FindIndex(s => s.Operation is ModifyRefreshOperation);
        var idxCreate = result.ToList().FindIndex(s => s.Operation is CreateTableOperation);
        Assert.True(idxCreate < idxModify, "ModifyRefresh runs after table creation.");
    }
}
