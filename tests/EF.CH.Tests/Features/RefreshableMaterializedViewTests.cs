using EF.CH.Extensions;
using EF.CH.Internal.Intervals;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

public class RefreshableMaterializedViewTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:25.6")
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

    private string GenerateMvDdl<TContext>(string tableName, Type entityClrType) where TContext : DbContext
    {
        using var context = CreateContext<TContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var entityType = context.Model.FindEntityType(entityClrType);
        Assert.NotNull(entityType);

        var op = new CreateTableOperation { Name = tableName };
        op.Columns.Add(new AddColumnOperation { Name = "Id", ClrType = typeof(int), ColumnType = "Int32" });

        // Carry every refresh-related annotation onto the operation so the SQL generator
        // sees them during this DDL-only test (the scaffolder normally does this).
        foreach (var annKey in new[]
        {
            ClickHouseAnnotationNames.MaterializedView,
            ClickHouseAnnotationNames.MaterializedViewSource,
            ClickHouseAnnotationNames.MaterializedViewQuery,
            ClickHouseAnnotationNames.MaterializedViewPopulate,
            ClickHouseAnnotationNames.MaterializedViewRefreshKind,
            ClickHouseAnnotationNames.MaterializedViewRefreshInterval,
            ClickHouseAnnotationNames.MaterializedViewRefreshOffset,
            ClickHouseAnnotationNames.MaterializedViewRefreshRandomizeFor,
            ClickHouseAnnotationNames.MaterializedViewRefreshDependsOn,
            ClickHouseAnnotationNames.MaterializedViewRefreshAppend,
            ClickHouseAnnotationNames.MaterializedViewRefreshEmpty,
            ClickHouseAnnotationNames.MaterializedViewRefreshSettings,
            ClickHouseAnnotationNames.MaterializedViewRefreshTarget,
            ClickHouseAnnotationNames.Engine,
            ClickHouseAnnotationNames.OrderBy,
        })
        {
            var v = entityType!.FindAnnotation(annKey)?.Value;
            if (v != null) op.AddAnnotation(annKey, v);
        }

        var commands = generator.Generate(new[] { op }, context.Model);
        return commands.First().CommandText;
    }

    #region Builder + annotation tests

    [Fact]
    public void Builder_SetsAllAnnotationsOnEntity()
    {
        using var context = CreateContext<EveryFiveMinutesContext>();
        var entity = context.Model.FindEntityType(typeof(EveryFiveMinutesMv))!;

        Assert.True((bool)(entity.FindAnnotation(ClickHouseAnnotationNames.MaterializedView)!.Value!));
        Assert.Equal("EVERY", entity.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshKind)!.Value);
        Assert.Equal("5 MINUTE", entity.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshInterval)!.Value);
        Assert.Equal("RawEvent", entity.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewSource)!.Value);
    }

    [Fact]
    public void Builder_RawSql_SetsAllAnnotations()
    {
        using var context = CreateContext<RawRefreshableContext>();
        var entity = context.Model.FindEntityType(typeof(RawRefreshableMv))!;

        Assert.Equal("AFTER", entity.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshKind)!.Value);
        Assert.Equal("1 HOUR", entity.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshInterval)!.Value);
        Assert.Equal("RawSource", entity.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewSource)!.Value);
    }

    #endregion

    #region DDL emission tests

    [Fact]
    public void Every_EmitsRefreshClause()
    {
        var sql = GenerateMvDdl<EveryFiveMinutesContext>("EveryFiveMinutesMv", typeof(EveryFiveMinutesMv));
        Assert.Contains("CREATE MATERIALIZED VIEW", sql);
        Assert.Contains("REFRESH EVERY 5 MINUTE", sql);
        Assert.DoesNotContain("POPULATE", sql);
    }

    [Fact]
    public void After_EmitsRefreshClause()
    {
        var sql = GenerateMvDdl<RawRefreshableContext>("RawRefreshableMv", typeof(RawRefreshableMv));
        Assert.Contains("REFRESH AFTER 1 HOUR", sql);
    }

    [Fact]
    public void OffsetAndRandomize_Emit()
    {
        var sql = GenerateMvDdl<OffsetRandomizeContext>("OffsetRandomizeMv", typeof(OffsetRandomizeMv));
        Assert.Contains("OFFSET 1 MINUTE", sql);
        Assert.Contains("RANDOMIZE FOR 30 SECOND", sql);
    }

    [Fact]
    public void DependsOn_EmitsList()
    {
        var sql = GenerateMvDdl<DependsOnContext>("DependsOnMv", typeof(DependsOnMv));
        Assert.Contains("DEPENDS ON", sql);
        // OtherMv resolves to its actual table name (EF Core pluralises DbSet<OtherMv> Others → "Others").
        Assert.Contains("\"Others\"", sql);
    }

    [Fact]
    public void Append_EmitsKeyword()
    {
        var sql = GenerateMvDdl<AppendContext>("AppendMv", typeof(AppendMv));
        Assert.Contains("APPEND", sql);
    }

    [Fact]
    public void Empty_EmitsKeyword()
    {
        var sql = GenerateMvDdl<EmptyContext>("EmptyMv", typeof(EmptyMv));
        Assert.Contains("EMPTY", sql);
    }

    [Fact]
    public void WithSetting_EmitsSettingsClause()
    {
        var sql = GenerateMvDdl<SettingsContext>("SettingsMv", typeof(SettingsMv));
        Assert.Contains("SETTINGS", sql);
        Assert.Contains("refresh_retries = 3", sql);
    }

    [Fact]
    public void ToTarget_OmitsEngineAndEmitsTo()
    {
        var sql = GenerateMvDdl<ToTargetContext>("ToTargetMv", typeof(ToTargetMv));
        Assert.Contains("TO \"TargetTable\"", sql);
        Assert.DoesNotContain("ENGINE =", sql);
    }

    [Fact]
    public void Engine_PresentWhenNoTarget()
    {
        var sql = GenerateMvDdl<EveryFiveMinutesContext>("EveryFiveMinutesMv", typeof(EveryFiveMinutesMv));
        Assert.Contains("ENGINE =", sql);
    }

    #endregion

    #region Validation

    [Fact]
    public void Validation_BuilderRequiresInterval()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => new RefreshableMaterializedViewBuilder().Build());
        Assert.Contains("Every", ex.Message);
    }

    [Fact]
    public void Validation_AppendAndEmpty_Throws()
    {
        var b = new RefreshableMaterializedViewBuilder()
            .Every(TimeSpan.FromMinutes(5))
            .Append()
            .Empty();
        Assert.Throws<InvalidOperationException>(() => b.Build());
    }

    [Fact]
    public void Validation_RefreshAndPopulate_Throws()
    {
        // ClickHouseModelValidator runs when the model is finalised (first context use).
        Assert.Throws<InvalidOperationException>(() =>
        {
            using var ctx = CreateContext<RefreshAndPopulateContext>();
            _ = ctx.Model;
        });
    }

    [Fact]
    public void Validation_DependsOnUnknownEntity_Throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
        {
            using var ctx = CreateContext<UnknownDependsContext>();
            _ = ctx.Model;
        });
    }

    #endregion

    #region Interval formatter

    [Fact]
    public void IntervalFormatter_TimeSpan_RoundsToBiggestUnit()
    {
        Assert.Equal("5 MINUTE", IntervalLiteralConverter.FromTimeSpan(TimeSpan.FromMinutes(5)));
        Assert.Equal("2 HOUR", IntervalLiteralConverter.FromTimeSpan(TimeSpan.FromHours(2)));
        Assert.Equal("3 DAY", IntervalLiteralConverter.FromTimeSpan(TimeSpan.FromDays(3)));
        Assert.Equal("2 WEEK", IntervalLiteralConverter.FromTimeSpan(TimeSpan.FromDays(14)));
    }

    [Fact]
    public void IntervalFormatter_TimeSpan_RejectsSubSecond()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => IntervalLiteralConverter.FromTimeSpan(TimeSpan.FromMilliseconds(250)));
    }

    [Fact]
    public void IntervalFormatter_TryParse_RoundTrips()
    {
        var parsed = IntervalLiteralConverter.TryParse("5 MINUTE");
        Assert.NotNull(parsed);
        Assert.Equal(5, parsed!.Value.Count);
        Assert.Equal(ClickHouseIntervalUnit.Minute, parsed.Value.Unit);
    }

    [Fact]
    public void IntervalFormatter_MonthYear_AcceptedViaExplicitOverload()
    {
        Assert.Equal("1 MONTH", IntervalLiteralConverter.Format(1, ClickHouseIntervalUnit.Month));
        Assert.Equal("3 YEAR", IntervalLiteralConverter.Format(3, ClickHouseIntervalUnit.Year));
    }

    #endregion
}

#region Test entities + contexts

public class RawEvent
{
    public int Id { get; set; }
    public DateTime Hour { get; set; }
}

public class OtherMv
{
    public int Id { get; set; }
}

public class TargetTable
{
    public int Id { get; set; }
}

public class EveryFiveMinutesMv { public int Id { get; set; } }
public class RawRefreshableMv { public int Id { get; set; } }
public class OffsetRandomizeMv { public int Id { get; set; } }
public class DependsOnMv { public int Id { get; set; } }
public class AppendMv { public int Id { get; set; } }
public class EmptyMv { public int Id { get; set; } }
public class SettingsMv { public int Id { get; set; } }
public class ToTargetMv { public int Id { get; set; } }
public class RefreshAndPopulateMv { public int Id { get; set; } }
public class UnknownDependsMv { public int Id { get; set; } }

public class EveryFiveMinutesContext(DbContextOptions<EveryFiveMinutesContext> opts) : DbContext(opts)
{
    public DbSet<RawEvent> Events => Set<RawEvent>();
    public DbSet<EveryFiveMinutesMv> Mvs => Set<EveryFiveMinutesMv>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<RawEvent>(e => { e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        b.Entity<EveryFiveMinutesMv>(e =>
        {
            e.UseMergeTree(x => x.Id);
            e.AsRefreshableMaterializedViewRaw("RawEvent", "SELECT Id FROM RawEvent",
                r => r.Every(TimeSpan.FromMinutes(5)));
        });
    }
}

public class RawRefreshableContext(DbContextOptions<RawRefreshableContext> opts) : DbContext(opts)
{
    public DbSet<RawRefreshableMv> Mvs => Set<RawRefreshableMv>();
    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<RawRefreshableMv>(e =>
        {
            e.UseMergeTree(x => x.Id);
            e.AsRefreshableMaterializedViewRaw("RawSource", "SELECT 1 AS Id",
                r => r.After(TimeSpan.FromHours(1)));
        });
    }
}

public class OffsetRandomizeContext(DbContextOptions<OffsetRandomizeContext> opts) : DbContext(opts)
{
    public DbSet<OffsetRandomizeMv> Mvs => Set<OffsetRandomizeMv>();
    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<OffsetRandomizeMv>(e =>
        {
            e.UseMergeTree(x => x.Id);
            e.AsRefreshableMaterializedViewRaw("RawSource", "SELECT 1 AS Id",
                r => r.Every(TimeSpan.FromMinutes(5))
                      .Offset(TimeSpan.FromMinutes(1))
                      .RandomizeFor(TimeSpan.FromSeconds(30)));
        });
    }
}

public class DependsOnContext(DbContextOptions<DependsOnContext> opts) : DbContext(opts)
{
    public DbSet<OtherMv> Others => Set<OtherMv>();
    public DbSet<DependsOnMv> Mvs => Set<DependsOnMv>();
    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<OtherMv>(e => { e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        b.Entity<DependsOnMv>(e =>
        {
            e.UseMergeTree(x => x.Id);
            e.AsRefreshableMaterializedViewRaw("RawSource", "SELECT 1 AS Id",
                r => r.Every(TimeSpan.FromMinutes(5)).DependsOn<OtherMv>());
        });
    }
}

public class AppendContext(DbContextOptions<AppendContext> opts) : DbContext(opts)
{
    public DbSet<AppendMv> Mvs => Set<AppendMv>();
    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<AppendMv>(e =>
        {
            e.UseMergeTree(x => x.Id);
            e.AsRefreshableMaterializedViewRaw("RawSource", "SELECT 1 AS Id",
                r => r.Every(TimeSpan.FromMinutes(5)).Append());
        });
    }
}

public class EmptyContext(DbContextOptions<EmptyContext> opts) : DbContext(opts)
{
    public DbSet<EmptyMv> Mvs => Set<EmptyMv>();
    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<EmptyMv>(e =>
        {
            e.UseMergeTree(x => x.Id);
            e.AsRefreshableMaterializedViewRaw("RawSource", "SELECT 1 AS Id",
                r => r.Every(TimeSpan.FromMinutes(5)).Empty());
        });
    }
}

public class SettingsContext(DbContextOptions<SettingsContext> opts) : DbContext(opts)
{
    public DbSet<SettingsMv> Mvs => Set<SettingsMv>();
    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<SettingsMv>(e =>
        {
            e.UseMergeTree(x => x.Id);
            e.AsRefreshableMaterializedViewRaw("RawSource", "SELECT 1 AS Id",
                r => r.Every(TimeSpan.FromMinutes(5)).WithSetting("refresh_retries", "3"));
        });
    }
}

public class ToTargetContext(DbContextOptions<ToTargetContext> opts) : DbContext(opts)
{
    public DbSet<TargetTable> Targets => Set<TargetTable>();
    public DbSet<ToTargetMv> Mvs => Set<ToTargetMv>();
    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<TargetTable>(e => { e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        b.Entity<ToTargetMv>(e =>
        {
            e.AsRefreshableMaterializedViewRaw("RawSource", "SELECT 1 AS Id",
                r => r.Every(TimeSpan.FromMinutes(5)).ToTarget<TargetTable>());
        });
    }
}

public class RefreshAndPopulateContext(DbContextOptions<RefreshAndPopulateContext> opts) : DbContext(opts)
{
    public DbSet<RefreshAndPopulateMv> Mvs => Set<RefreshAndPopulateMv>();
    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<RefreshAndPopulateMv>(e =>
        {
            e.UseMergeTree(x => x.Id);
            e.AsRefreshableMaterializedViewRaw("RawSource", "SELECT 1 AS Id",
                r => r.Every(TimeSpan.FromMinutes(5)));
            // Cross-set populate to provoke validation
            e.HasAnnotation(ClickHouseAnnotationNames.MaterializedViewPopulate, true);
        });
    }
}

public class UnknownDependsContext(DbContextOptions<UnknownDependsContext> opts) : DbContext(opts)
{
    public DbSet<UnknownDependsMv> Mvs => Set<UnknownDependsMv>();
    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<UnknownDependsMv>(e =>
        {
            e.UseMergeTree(x => x.Id);
            e.AsRefreshableMaterializedViewRaw("RawSource", "SELECT 1 AS Id",
                r => r.Every(TimeSpan.FromMinutes(5)).DependsOn("EntityThatDoesNotExist"));
        });
    }
}

#endregion
