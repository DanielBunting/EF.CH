using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Projections;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

/// <summary>
/// Tests for ClickHouse approximate aggregate functions (count distinct variants,
/// quantile algorithm variants, multi-quantile, weighted top-K).
/// </summary>
public class ApproximateFunctionTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    private string GetConnectionString() => _container.GetConnectionString();

    #region Approximate Count Distinct Projection Tests

    [Fact]
    public void Projection_UniqCombined_TranslatesCorrectly()
    {
        using var context = CreateContext<UniqCombinedProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(ApproxTestEvent));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("uniqCombined(", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_UniqCombined64_TranslatesCorrectly()
    {
        using var context = CreateContext<UniqCombined64ProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(ApproxTestEvent));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("uniqCombined64(", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_UniqHLL12_TranslatesCorrectly()
    {
        using var context = CreateContext<UniqHLL12ProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(ApproxTestEvent));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("uniqHLL12(", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_UniqTheta_TranslatesCorrectly()
    {
        using var context = CreateContext<UniqThetaProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(ApproxTestEvent));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("uniqTheta(", projections[0].SelectSql);
    }

    #endregion

    #region Quantile Variant Projection Tests

    [Fact]
    public void Projection_QuantileTDigest_TranslatesCorrectly()
    {
        using var context = CreateContext<QuantileTDigestProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(ApproxMetricEvent));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("quantileTDigest(0.95)", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_QuantileDD_TranslatesCorrectly()
    {
        using var context = CreateContext<QuantileDDProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(ApproxMetricEvent));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("quantileDD(0.01, 0.95)", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_QuantileExact_TranslatesCorrectly()
    {
        using var context = CreateContext<QuantileExactProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(ApproxMetricEvent));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("quantileExact(0.95)", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_QuantileTiming_TranslatesCorrectly()
    {
        using var context = CreateContext<QuantileTimingProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(ApproxMetricEvent));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("quantileTiming(0.95)", projections[0].SelectSql);
    }

    #endregion

    #region Multi-Quantile Projection Tests

    [Fact]
    public void Projection_Quantiles_TranslatesCorrectly()
    {
        using var context = CreateContext<QuantilesProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(ApproxMetricEvent));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("quantiles(0.5, 0.9, 0.99)", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_QuantilesTDigest_TranslatesCorrectly()
    {
        using var context = CreateContext<QuantilesTDigestProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(ApproxMetricEvent));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("quantilesTDigest(0.5, 0.9, 0.99)", projections[0].SelectSql);
    }

    #endregion

    #region TopKWeighted Projection Tests

    [Fact]
    public void Projection_TopKWeighted_TranslatesCorrectly()
    {
        using var context = CreateContext<TopKWeightedProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(ApproxWeightedEvent));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("topKWeighted(5)", projections[0].SelectSql);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task UniqCombined_ExecutesSuccessfully()
    {
        await using var context = CreateContext<UniqCombinedProjectionContext>();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS approx_events (
                Id UUID,
                EventDate Date,
                UserId UInt64,
                Category String,
                Value Float64,
                Weight UInt32
            ) ENGINE = MergeTree()
            ORDER BY (EventDate, Id)
        ");

        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO approx_events (Id, EventDate, UserId, Category, Value, Weight) VALUES
            (generateUUIDv4(), '2024-01-15', 1, 'A', 10.0, 1),
            (generateUUIDv4(), '2024-01-15', 2, 'B', 20.0, 2),
            (generateUUIDv4(), '2024-01-15', 1, 'A', 30.0, 3),
            (generateUUIDv4(), '2024-01-15', 3, 'C', 40.0, 1),
            (generateUUIDv4(), '2024-01-16', 1, 'A', 50.0, 2)
        ");

        var results = await context.Database
            .SqlQueryRaw<ApproxCountResult>(
                "SELECT EventDate, uniqCombined(UserId) AS UniqueCount FROM approx_events GROUP BY EventDate ORDER BY EventDate")
            .ToListAsync();

        Assert.Equal(2, results.Count);
        var jan15 = results.First(r => r.EventDate == new DateTime(2024, 1, 15));
        Assert.True(jan15.UniqueCount >= 2 && jan15.UniqueCount <= 4); // approximate
    }

    [Fact]
    public async Task QuantileTDigest_ExecutesSuccessfully()
    {
        await using var context = CreateContext<QuantileTDigestProjectionContext>();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS approx_metrics (
                Id UUID,
                EventDate Date,
                Region String,
                Latency Float64
            ) ENGINE = MergeTree()
            ORDER BY (EventDate, Id)
        ");

        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO approx_metrics (Id, EventDate, Region, Latency) VALUES
            (generateUUIDv4(), '2024-01-15', 'US', 10.0),
            (generateUUIDv4(), '2024-01-15', 'US', 20.0),
            (generateUUIDv4(), '2024-01-15', 'US', 30.0),
            (generateUUIDv4(), '2024-01-15', 'US', 40.0),
            (generateUUIDv4(), '2024-01-15', 'US', 100.0)
        ");

        var results = await context.Database
            .SqlQueryRaw<ApproxQuantileResult>(
                "SELECT Region, quantileTDigest(0.5)(Latency) AS MedianLatency FROM approx_metrics GROUP BY Region")
            .ToListAsync();

        Assert.Single(results);
        // Median of {10, 20, 30, 40, 100} should be around 30
        Assert.True(results[0].MedianLatency >= 20 && results[0].MedianLatency <= 40);
    }

    [Fact]
    public async Task TopKWeighted_ExecutesSuccessfully()
    {
        await using var context = CreateContext<TopKWeightedProjectionContext>();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS approx_weighted (
                Id UUID,
                EventDate Date,
                Category String,
                Weight UInt32
            ) ENGINE = MergeTree()
            ORDER BY (EventDate, Id)
        ");

        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO approx_weighted (Id, EventDate, Category, Weight) VALUES
            (generateUUIDv4(), '2024-01-15', 'A', 100),
            (generateUUIDv4(), '2024-01-15', 'B', 50),
            (generateUUIDv4(), '2024-01-15', 'C', 10),
            (generateUUIDv4(), '2024-01-15', 'A', 200),
            (generateUUIDv4(), '2024-01-15', 'D', 5)
        ");

        var results = await context.Database
            .SqlQueryRaw<ApproxTopKResult>(
                "SELECT topKWeighted(3)(Category, Weight) AS TopCategories FROM approx_weighted")
            .ToListAsync();

        Assert.Single(results);
        Assert.Contains("A", results[0].TopCategories);
    }

    #endregion

    private TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }
}

#region Test Result Types

public class ApproxCountResult
{
    public DateTime EventDate { get; set; }
    public ulong UniqueCount { get; set; }
}

public class ApproxQuantileResult
{
    public string Region { get; set; } = string.Empty;
    public double MedianLatency { get; set; }
}

public class ApproxTopKResult
{
    public string[] TopCategories { get; set; } = [];
}

#endregion

#region Test Entities

public class ApproxTestEvent
{
    public Guid Id { get; set; }
    public DateTime EventDate { get; set; }
    public ulong UserId { get; set; }
    public string Category { get; set; } = string.Empty;
    public double Value { get; set; }
    public uint Weight { get; set; }
}

public class ApproxMetricEvent
{
    public Guid Id { get; set; }
    public DateTime EventDate { get; set; }
    public string Region { get; set; } = string.Empty;
    public double Latency { get; set; }
}

public class ApproxWeightedEvent
{
    public Guid Id { get; set; }
    public DateTime EventDate { get; set; }
    public string Category { get; set; } = string.Empty;
    public uint Weight { get; set; }
}

#endregion

#region Approximate Count Distinct Contexts

public class UniqCombinedProjectionContext : DbContext
{
    public UniqCombinedProjectionContext(DbContextOptions<UniqCombinedProjectionContext> options)
        : base(options) { }

    public DbSet<ApproxTestEvent> Events => Set<ApproxTestEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ApproxTestEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("approx_events");
            entity.UseMergeTree(x => new { x.EventDate, x.Id });

            entity.HasProjection()
                .GroupBy(e => e.EventDate)
                .Select(g => new
                {
                    Date = g.Key,
                    UniqueUsers = ClickHouseAggregates.UniqCombined(g, e => e.UserId)
                })
                .Build();
        });
    }
}

public class UniqCombined64ProjectionContext : DbContext
{
    public UniqCombined64ProjectionContext(DbContextOptions<UniqCombined64ProjectionContext> options)
        : base(options) { }

    public DbSet<ApproxTestEvent> Events => Set<ApproxTestEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ApproxTestEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("approx_events");
            entity.UseMergeTree(x => new { x.EventDate, x.Id });

            entity.HasProjection()
                .GroupBy(e => e.EventDate)
                .Select(g => new
                {
                    Date = g.Key,
                    UniqueUsers = ClickHouseAggregates.UniqCombined64(g, e => e.UserId)
                })
                .Build();
        });
    }
}

public class UniqHLL12ProjectionContext : DbContext
{
    public UniqHLL12ProjectionContext(DbContextOptions<UniqHLL12ProjectionContext> options)
        : base(options) { }

    public DbSet<ApproxTestEvent> Events => Set<ApproxTestEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ApproxTestEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("approx_events");
            entity.UseMergeTree(x => new { x.EventDate, x.Id });

            entity.HasProjection()
                .GroupBy(e => e.EventDate)
                .Select(g => new
                {
                    Date = g.Key,
                    UniqueUsers = ClickHouseAggregates.UniqHLL12(g, e => e.UserId)
                })
                .Build();
        });
    }
}

public class UniqThetaProjectionContext : DbContext
{
    public UniqThetaProjectionContext(DbContextOptions<UniqThetaProjectionContext> options)
        : base(options) { }

    public DbSet<ApproxTestEvent> Events => Set<ApproxTestEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ApproxTestEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("approx_events");
            entity.UseMergeTree(x => new { x.EventDate, x.Id });

            entity.HasProjection()
                .GroupBy(e => e.EventDate)
                .Select(g => new
                {
                    Date = g.Key,
                    UniqueUsers = ClickHouseAggregates.UniqTheta(g, e => e.UserId)
                })
                .Build();
        });
    }
}

#endregion

#region Quantile Variant Contexts

public class QuantileTDigestProjectionContext : DbContext
{
    public QuantileTDigestProjectionContext(DbContextOptions<QuantileTDigestProjectionContext> options)
        : base(options) { }

    public DbSet<ApproxMetricEvent> Metrics => Set<ApproxMetricEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ApproxMetricEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("approx_metrics");
            entity.UseMergeTree(x => new { x.EventDate, x.Id });

            entity.HasProjection()
                .GroupBy(e => e.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    P95Latency = ClickHouseAggregates.QuantileTDigest(g, 0.95, e => e.Latency)
                })
                .Build();
        });
    }
}

public class QuantileDDProjectionContext : DbContext
{
    public QuantileDDProjectionContext(DbContextOptions<QuantileDDProjectionContext> options)
        : base(options) { }

    public DbSet<ApproxMetricEvent> Metrics => Set<ApproxMetricEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ApproxMetricEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("approx_metrics");
            entity.UseMergeTree(x => new { x.EventDate, x.Id });

            entity.HasProjection()
                .GroupBy(e => e.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    P95Latency = ClickHouseAggregates.QuantileDD(g, 0.01, 0.95, e => e.Latency)
                })
                .Build();
        });
    }
}

public class QuantileExactProjectionContext : DbContext
{
    public QuantileExactProjectionContext(DbContextOptions<QuantileExactProjectionContext> options)
        : base(options) { }

    public DbSet<ApproxMetricEvent> Metrics => Set<ApproxMetricEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ApproxMetricEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("approx_metrics");
            entity.UseMergeTree(x => new { x.EventDate, x.Id });

            entity.HasProjection()
                .GroupBy(e => e.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    P95Latency = ClickHouseAggregates.QuantileExact(g, 0.95, e => e.Latency)
                })
                .Build();
        });
    }
}

public class QuantileTimingProjectionContext : DbContext
{
    public QuantileTimingProjectionContext(DbContextOptions<QuantileTimingProjectionContext> options)
        : base(options) { }

    public DbSet<ApproxMetricEvent> Metrics => Set<ApproxMetricEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ApproxMetricEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("approx_metrics");
            entity.UseMergeTree(x => new { x.EventDate, x.Id });

            entity.HasProjection()
                .GroupBy(e => e.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    P95Latency = ClickHouseAggregates.QuantileTiming(g, 0.95, e => e.Latency)
                })
                .Build();
        });
    }
}

#endregion

#region Multi-Quantile Contexts

public class QuantilesProjectionContext : DbContext
{
    public QuantilesProjectionContext(DbContextOptions<QuantilesProjectionContext> options)
        : base(options) { }

    public DbSet<ApproxMetricEvent> Metrics => Set<ApproxMetricEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ApproxMetricEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("approx_metrics");
            entity.UseMergeTree(x => new { x.EventDate, x.Id });

            entity.HasProjection()
                .GroupBy(e => e.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    Percentiles = ClickHouseAggregates.Quantiles(g, new[] { 0.5, 0.9, 0.99 }, e => e.Latency)
                })
                .Build();
        });
    }
}

public class QuantilesTDigestProjectionContext : DbContext
{
    public QuantilesTDigestProjectionContext(DbContextOptions<QuantilesTDigestProjectionContext> options)
        : base(options) { }

    public DbSet<ApproxMetricEvent> Metrics => Set<ApproxMetricEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ApproxMetricEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("approx_metrics");
            entity.UseMergeTree(x => new { x.EventDate, x.Id });

            entity.HasProjection()
                .GroupBy(e => e.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    Percentiles = ClickHouseAggregates.QuantilesTDigest(g, new[] { 0.5, 0.9, 0.99 }, e => e.Latency)
                })
                .Build();
        });
    }
}

#endregion

#region TopKWeighted Context

public class TopKWeightedProjectionContext : DbContext
{
    public TopKWeightedProjectionContext(DbContextOptions<TopKWeightedProjectionContext> options)
        : base(options) { }

    public DbSet<ApproxWeightedEvent> Events => Set<ApproxWeightedEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ApproxWeightedEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("approx_weighted");
            entity.UseMergeTree(x => new { x.EventDate, x.Id });

            entity.HasProjection()
                .GroupBy(e => e.EventDate)
                .Select(g => new
                {
                    Date = g.Key,
                    TopCategories = ClickHouseAggregates.TopKWeighted(g, 5, e => e.Category, e => e.Weight)
                })
                .Build();
        });
    }
}

#endregion
