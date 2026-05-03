using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Metadata.Internal;
using EF.CH.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Pins <c>ON CLUSTER</c> emission for migration operations that previously
/// dropped it. <c>ALTER TABLE … ADD INDEX</c> and <c>ALTER TABLE … ADD PROJECTION</c>
/// did not propagate the cluster annotation, so on a replicated cluster the
/// index/projection landed on a single replica only — silent replication
/// divergence. Other operations (CREATE TABLE, DROP TABLE, RENAME) already
/// emit <c>ON CLUSTER</c> via <c>GetOnClusterClause</c>; this test pins the
/// gaps closed.
/// </summary>
public class AlterTableOnClusterTests
{
    [Fact]
    public void AddIndex_OnClusteredEntity_EmitsOnCluster()
    {
        using var ctx = CreateContext();
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        var op = new CreateIndexOperation
        {
            Name = "idx_value",
            Table = "events",
            Columns = ["Value"],
            IsUnique = false,
        };
        op.AddAnnotation(ClickHouseAnnotationNames.SkipIndexType, SkipIndexType.Minmax);
        op.AddAnnotation(ClickHouseAnnotationNames.SkipIndexGranularity, 4);
        op.AddAnnotation(ClickHouseAnnotationNames.EntityClusterName, "primary_cluster");

        var sql = generator.Generate([op]).Single().CommandText;

        Assert.Contains("ALTER TABLE", sql);
        Assert.Contains("ADD INDEX", sql);
        Assert.Contains("ON CLUSTER", sql);
        Assert.Contains("primary_cluster", sql);
    }

    [Fact]
    public void AddProjection_OnClusteredEntity_EmitsOnCluster()
    {
        using var ctx = CreateContext();
        var generator = ctx.GetService<IMigrationsSqlGenerator>();

        var op = new AddProjectionOperation
        {
            Name = "proj_top_value",
            Table = "events",
            SelectSql = "SELECT * ORDER BY Value DESC",
        };
        op.AddAnnotation(ClickHouseAnnotationNames.EntityClusterName, "primary_cluster");

        var commands = generator.Generate([op]).Select(c => c.CommandText).ToList();

        // ADD PROJECTION and (when Materialize is true) MATERIALIZE PROJECTION
        // are emitted as separate commands. Both must carry ON CLUSTER.
        Assert.Contains(commands, c =>
            c.Contains("ALTER TABLE", StringComparison.Ordinal)
            && c.Contains("ADD PROJECTION", StringComparison.Ordinal)
            && c.Contains("ON CLUSTER", StringComparison.Ordinal)
            && c.Contains("primary_cluster", StringComparison.Ordinal));
    }

    private static OnClusterCtx CreateContext() =>
        new(new DbContextOptionsBuilder<OnClusterCtx>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options);

    public sealed class OnClusterCtx(DbContextOptions<OnClusterCtx> o) : DbContext(o);
}
