using EF.CH.Extensions;
using EF.CH.Views;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Xunit;

namespace EF.CH.Tests.Features;

/// <summary>
/// Pins the ON CLUSTER classification used by <see cref="ViewSqlGenerator"/>.
/// Macros (server-side substitution targets like <c>{cluster}</c>) must be
/// emitted as single-quoted literals; plain identifiers like <c>my_cluster</c>
/// must be emitted as backticked identifiers. The same classification is used
/// elsewhere in the codebase (see <c>ClickHouseDatabaseCreator.GetOnClusterClause</c>);
/// the view generator previously emitted the value raw, which produced
/// malformed SQL for any name containing special characters and an inconsistent
/// shape from MV / migration paths.
/// </summary>
public class ViewOnClusterClassificationTests
{
    [Fact]
    public void CreateView_OnClusterMacro_EmittedAsSingleQuotedLiteral()
    {
        var meta = new ViewMetadataBase
        {
            ViewName = "v",
            ResultType = typeof(object),
            OnCluster = "{cluster}",
            RawSelectSql = "SELECT 1",
        };

        var sql = ViewSqlGenerator.GenerateCreateViewSql(BuildEmptyModel(), meta);

        Assert.Contains("ON CLUSTER '{cluster}'", sql);
    }

    [Fact]
    public void CreateView_OnClusterLiteral_EmittedAsBacktickedIdentifier()
    {
        var meta = new ViewMetadataBase
        {
            ViewName = "v",
            ResultType = typeof(object),
            OnCluster = "my_cluster",
            RawSelectSql = "SELECT 1",
        };

        var sql = ViewSqlGenerator.GenerateCreateViewSql(BuildEmptyModel(), meta);

        Assert.Contains("ON CLUSTER `my_cluster`", sql);
    }

    [Fact]
    public void DropView_OnClusterMacro_EmittedAsSingleQuotedLiteral()
    {
        var sql = ViewSqlGenerator.GenerateDropViewSql("v", schema: null, onCluster: "{cluster}");
        Assert.Contains("ON CLUSTER '{cluster}'", sql);
    }

    [Fact]
    public void DropView_OnClusterLiteral_EmittedAsBacktickedIdentifier()
    {
        var sql = ViewSqlGenerator.GenerateDropViewSql("v", schema: null, onCluster: "my_cluster");
        Assert.Contains("ON CLUSTER `my_cluster`", sql);
    }

    private static IModel BuildEmptyModel()
    {
        var ctx = new EmptyCtx(new DbContextOptionsBuilder<EmptyCtx>()
            .UseClickHouse("Host=localhost;Database=test").Options);
        return ctx.Model;
    }

    private sealed class EmptyCtx(DbContextOptions<EmptyCtx> o) : DbContext(o);
}
