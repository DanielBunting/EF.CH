using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Design;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Verifies that refreshable-MV annotations survive scaffolding into the
/// <see cref="CreateTableOperation"/> shape the splitter and SQL generator consume.
/// </summary>
public class RefreshableMvScaffolderTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:25.6")
        .Build();

    public async Task InitializeAsync() => await _container.StartAsync();
    public async Task DisposeAsync() => await _container.DisposeAsync();

    private TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>().UseClickHouse(_container.GetConnectionString()).Options;
        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }

    [Fact]
    public void Entity_HasAllRefreshAnnotations()
    {
        using var ctx = CreateContext<RefreshableScaffolderContext>();
        var entity = ctx.Model.FindEntityType(typeof(ScaffolderRefreshableMv))!;

        Assert.True((bool)entity.FindAnnotation(ClickHouseAnnotationNames.MaterializedView)!.Value!);
        Assert.Equal("EVERY", entity.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshKind)!.Value);
        Assert.Equal("5 MINUTE", entity.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshInterval)!.Value);
        Assert.Equal("30 SECOND", entity.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshRandomizeFor)!.Value);
        Assert.True((bool)entity.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshAppend)!.Value!);
    }

    [Fact]
    public void Splitter_RefreshDependsOn_OrdersBeforeDependent()
    {
        // Build raw operations with annotations applied directly (mirrors how the scaffolder
        // populates them), then run them through the splitter.
        var sourceOp = new CreateTableOperation { Name = "Source" };
        var mvAOp = MakeMv("Mv_A", refreshInterval: "5 MINUTE");
        var mvBOp = MakeMv("Mv_B", refreshInterval: "5 MINUTE", refreshDependsOn: new[] { "Mv_A" });

        var splitter = new ClickHouseMigrationsSplitter();
        var ordered = splitter.Split(new List<MigrationOperation> { mvBOp, mvAOp, sourceOp });

        var idxA = ordered.ToList().FindIndex(s => (s.Operation as CreateTableOperation)?.Name == "Mv_A");
        var idxB = ordered.ToList().FindIndex(s => (s.Operation as CreateTableOperation)?.Name == "Mv_B");
        Assert.True(idxA < idxB);
    }

    private static CreateTableOperation MakeMv(string name, string refreshInterval, string[]? refreshDependsOn = null)
    {
        var op = new CreateTableOperation { Name = name };
        op.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        op.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "Source");
        op.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, "SELECT 1");
        op.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshKind, "EVERY");
        op.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshInterval, refreshInterval);
        if (refreshDependsOn is not null)
            op.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshDependsOn, refreshDependsOn);
        return op;
    }
}

public class ScaffolderRefreshableMv { public int Id { get; set; } }

public class RefreshableScaffolderContext(DbContextOptions<RefreshableScaffolderContext> opts) : DbContext(opts)
{
    public DbSet<ScaffolderRefreshableMv> Mvs => Set<ScaffolderRefreshableMv>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<ScaffolderRefreshableMv>(e =>
        {
            e.UseMergeTree(x => x.Id);
            e.AsRefreshableMaterializedViewRaw("Source", "SELECT 1 AS Id",
                r => r.Every(TimeSpan.FromMinutes(5))
                      .RandomizeFor(TimeSpan.FromSeconds(30))
                      .Append());
        });
    }
}
