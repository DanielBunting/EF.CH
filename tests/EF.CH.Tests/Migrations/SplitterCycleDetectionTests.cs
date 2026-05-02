using EF.CH.Metadata;
using EF.CH.Migrations.Design;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Pins that the splitter detects circular MV dependencies and throws
/// a clear exception naming the involved tables, rather than silently
/// emitting an arbitrary order (Kahn's leftover-entries fallback).
/// </summary>
public class SplitterCycleDetectionTests
{
    [Fact]
    public void Split_TwoMvsDependOnEachOther_ThrowsCycleError()
    {
        var mvA = NewMvCreate("mv_a", source: "tbl_x", joined: new[] { "mv_b" });
        var mvB = NewMvCreate("mv_b", source: "tbl_y", joined: new[] { "mv_a" });

        var splitter = new ClickHouseMigrationsSplitter();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            splitter.Split(new MigrationOperation[] { mvA, mvB }));

        Assert.Contains("circular", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("mv_a", ex.Message);
        Assert.Contains("mv_b", ex.Message);
    }

    [Fact]
    public void Split_SelfReferencingMv_ThrowsCycleError()
    {
        var mv = NewMvCreate("mv_self", source: "mv_self", joined: null);

        var splitter = new ClickHouseMigrationsSplitter();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            splitter.Split(new MigrationOperation[] { mv }));

        Assert.Contains("circular", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("mv_self", ex.Message);
    }

    [Fact]
    public void Split_AcyclicChain_ProducesSourceFirstOrdering()
    {
        // tbl_x ← mv_a ← mv_b
        var src = new CreateTableOperation { Name = "tbl_x" };
        var mvA = NewMvCreate("mv_a", source: "tbl_x", joined: null);
        var mvB = NewMvCreate("mv_b", source: "mv_a", joined: null);

        var splitter = new ClickHouseMigrationsSplitter();
        // Intentionally provide in reverse order; splitter must reorder.
        var steps = splitter.Split(new MigrationOperation[] { mvB, mvA, src });

        Assert.Equal(3, steps.Count);
        var names = steps.Select(s => ((CreateTableOperation)s.Operation).Name).ToArray();
        Assert.Equal("tbl_x", names[0]);
        Assert.Equal("mv_a", names[1]);
        Assert.Equal("mv_b", names[2]);
    }

    private static CreateTableOperation NewMvCreate(string name, string source, string[]? joined)
    {
        var op = new CreateTableOperation { Name = name };
        op.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        op.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, source);
        if (joined is { Length: > 0 })
            op.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewJoinedSources, joined);
        return op;
    }
}
