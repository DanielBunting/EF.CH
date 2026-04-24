using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Projections;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

/// <summary>
/// Tests for ClickHouse-specific aggregate functions in projections.
/// </summary>
public class ClickHouseAggregatesTests : IAsyncLifetime
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

    #region Uniq/UniqExact Tests

    [Fact]
    public void Projection_Uniq_TranslatesCorrectly()
    {
        using var context = CreateContext<UniqProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(ClickEvent));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("uniq(", projections[0].SelectSql);
        Assert.Contains("\"UserId\"", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_UniqExact_TranslatesCorrectly()
    {
        using var context = CreateContext<UniqExactProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(ClickEvent));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("uniqExact(", projections[0].SelectSql);
    }

    #endregion

    #region ArgMax/ArgMin Tests

    [Fact]
    public void Projection_ArgMax_TranslatesCorrectly()
    {
        using var context = CreateContext<ArgMaxProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(PriceHistory));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("argMax(", projections[0].SelectSql);
        Assert.Contains("\"Price\"", projections[0].SelectSql);
        Assert.Contains("\"Timestamp\"", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_ArgMin_TranslatesCorrectly()
    {
        using var context = CreateContext<ArgMinProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(PriceHistory));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("argMin(", projections[0].SelectSql);
    }

    #endregion

    #region AnyValue/AnyLastValue Tests

    [Fact]
    public void Projection_AnyValue_TranslatesCorrectly()
    {
        using var context = CreateContext<AnyProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(ClickEvent));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("any(", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_AnyLastValue_TranslatesCorrectly()
    {
        using var context = CreateContext<AnyLastProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(ClickEvent));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("anyLast(", projections[0].SelectSql);
    }

    #endregion

    #region Statistical Aggregate Tests

    [Fact]
    public void Projection_Median_TranslatesCorrectly()
    {
        using var context = CreateContext<MedianProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(SalesRecord));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("median(", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_Quantile_TranslatesCorrectly()
    {
        using var context = CreateContext<QuantileProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(SalesRecord));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("quantile(0.95)", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_StddevPop_TranslatesCorrectly()
    {
        using var context = CreateContext<StddevPopProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(SalesRecord));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("stddevPop(", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_StddevSamp_TranslatesCorrectly()
    {
        using var context = CreateContext<StddevSampProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(SalesRecord));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("stddevSamp(", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_VarPop_TranslatesCorrectly()
    {
        using var context = CreateContext<VarPopProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(SalesRecord));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("varPop(", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_VarSamp_TranslatesCorrectly()
    {
        using var context = CreateContext<VarSampProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(SalesRecord));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("varSamp(", projections[0].SelectSql);
    }

    #endregion

    #region Array Aggregate Tests

    [Fact]
    public void Projection_GroupArray_TranslatesCorrectly()
    {
        using var context = CreateContext<GroupArrayProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(AggOrderItem));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("groupArray(", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_GroupArrayWithLimit_TranslatesCorrectly()
    {
        using var context = CreateContext<GroupArrayLimitedProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(AggOrderItem));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("groupArray(10)", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_GroupUniqArray_TranslatesCorrectly()
    {
        using var context = CreateContext<GroupUniqArrayProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(AggOrderItem));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("groupUniqArray(", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_TopK_TranslatesCorrectly()
    {
        using var context = CreateContext<TopKProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(AggOrderItem));
        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("topK(5)", projections[0].SelectSql);
    }

    #endregion

    #region If Combinator Projection Tests

    [Fact]
    public void Projection_ArgMaxIf_TranslatesCorrectly()
    {
        using var context = CreateContext<ArgMaxIfProjectionContext>();
        var projections = context.Model.FindEntityType(typeof(PriceHistory))!
            .FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Contains("argMaxIf(", projections[0].SelectSql);
        Assert.Contains("\"Price\"", projections[0].SelectSql);
        Assert.Contains("\"Timestamp\"", projections[0].SelectSql);
    }

    [Fact]
    public void Projection_ArgMinIf_TranslatesCorrectly()
    {
        using var context = CreateContext<ArgMinIfProjectionContext>();
        var projections = context.Model.FindEntityType(typeof(PriceHistory))!
            .FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Contains("argMinIf(", projections![0].SelectSql);
    }

    [Fact]
    public void Projection_TopKIf_TranslatesCorrectly()
    {
        using var context = CreateContext<TopKIfProjectionContext>();
        var projections = context.Model.FindEntityType(typeof(AggOrderItem))!
            .FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Contains("topKIf(5)(", projections![0].SelectSql);
    }

    [Fact]
    public void Projection_TopKWeightedIf_TranslatesCorrectly()
    {
        using var context = CreateContext<TopKWeightedIfProjectionContext>();
        var projections = context.Model.FindEntityType(typeof(AggOrderItem))!
            .FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Contains("topKWeightedIf(3)(", projections![0].SelectSql);
    }

    [Fact]
    public void Projection_GroupArrayIf_TranslatesCorrectly()
    {
        using var context = CreateContext<GroupArrayIfProjectionContext>();
        var projections = context.Model.FindEntityType(typeof(AggOrderItem))!
            .FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Contains("groupArrayIf(", projections![0].SelectSql);
    }

    [Fact]
    public void Projection_GroupArrayIfWithLimit_TranslatesCorrectly()
    {
        using var context = CreateContext<GroupArrayIfLimitedProjectionContext>();
        var projections = context.Model.FindEntityType(typeof(AggOrderItem))!
            .FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Contains("groupArrayIf(10)(", projections![0].SelectSql);
    }

    [Fact]
    public void Projection_GroupUniqArrayIf_TranslatesCorrectly()
    {
        using var context = CreateContext<GroupUniqArrayIfProjectionContext>();
        var projections = context.Model.FindEntityType(typeof(AggOrderItem))!
            .FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Contains("groupUniqArrayIf(", projections![0].SelectSql);
    }

    [Fact]
    public void Projection_MedianIf_TranslatesCorrectly()
    {
        using var context = CreateContext<MedianIfProjectionContext>();
        var projections = context.Model.FindEntityType(typeof(SalesRecord))!
            .FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Contains("medianIf(", projections![0].SelectSql);
    }

    [Fact]
    public void Projection_StddevPopIf_TranslatesCorrectly()
    {
        using var context = CreateContext<StddevPopIfProjectionContext>();
        var projections = context.Model.FindEntityType(typeof(SalesRecord))!
            .FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Contains("stddevPopIf(", projections![0].SelectSql);
    }

    [Fact]
    public void Projection_UniqCombinedIf_TranslatesCorrectly()
    {
        using var context = CreateContext<UniqCombinedIfProjectionContext>();
        var projections = context.Model.FindEntityType(typeof(ClickEvent))!
            .FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Contains("uniqCombinedIf(", projections![0].SelectSql);
    }

    [Fact]
    public void Projection_QuantileTDigestIf_TranslatesCorrectly()
    {
        using var context = CreateContext<QuantileTDigestIfProjectionContext>();
        var projections = context.Model.FindEntityType(typeof(SalesRecord))!
            .FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Contains("quantileTDigestIf(0.95)(", projections![0].SelectSql);
    }

    [Fact]
    public void Projection_QuantileDDIf_TranslatesCorrectly()
    {
        using var context = CreateContext<QuantileDDIfProjectionContext>();
        var projections = context.Model.FindEntityType(typeof(SalesRecord))!
            .FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Contains("quantileDDIf(0.01, 0.95)(", projections![0].SelectSql);
    }

    [Fact]
    public void Projection_QuantilesIf_TranslatesCorrectly()
    {
        using var context = CreateContext<QuantilesIfProjectionContext>();
        var projections = context.Model.FindEntityType(typeof(SalesRecord))!
            .FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Contains("quantilesIf(0.5, 0.9, 0.99)(", projections![0].SelectSql);
    }

    [Fact]
    public void Projection_MultipleIfAggregates_ShareSingleGroupBy()
    {
        using var context = CreateContext<MultiIfProjectionContext>();
        var projections = context.Model.FindEntityType(typeof(SalesRecord))!
            .FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        var sql = projections![0].SelectSql;

        Assert.Contains("sumIf(", sql);
        Assert.Contains("countIf(", sql);
        Assert.Contains("uniqIf(", sql);

        var groupByCount = 0;
        var idx = 0;
        while ((idx = sql.IndexOf("GROUP BY", idx, StringComparison.Ordinal)) != -1)
        {
            groupByCount++;
            idx += "GROUP BY".Length;
        }
        Assert.Equal(1, groupByCount);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task Projection_WithClickHouseAggregates_ExecutesSuccessfully()
    {
        await using var context = CreateContext<UniqProjectionContext>();

        // Create table with projection
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS click_events (
                Id UUID,
                EventDate Date,
                UserId UInt64,
                EventType String
            ) ENGINE = MergeTree()
            ORDER BY (EventDate, Id)
        ");

        // Add projection using ClickHouse aggregates
        await context.Database.ExecuteSqlRawAsync(@"
            ALTER TABLE click_events ADD PROJECTION IF NOT EXISTS prj_daily_uniq
            (
                SELECT EventDate, uniq(UserId) AS UniqueUsers, count() AS TotalEvents
                GROUP BY EventDate
            )
        ");

        // Insert data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO click_events (Id, EventDate, UserId, EventType) VALUES
            (generateUUIDv4(), '2024-01-15', 1, 'click'),
            (generateUUIDv4(), '2024-01-15', 2, 'click'),
            (generateUUIDv4(), '2024-01-15', 1, 'view'),
            (generateUUIDv4(), '2024-01-16', 3, 'click')
        ");

        // Query
        var results = await context.Database
            .SqlQueryRaw<DailyUniqResult>(
                "SELECT EventDate, uniq(UserId) AS UniqueUsers, count() AS TotalEvents FROM click_events GROUP BY EventDate ORDER BY EventDate")
            .ToListAsync();

        Assert.Equal(2, results.Count);
        var jan15 = results.First(r => r.EventDate == new DateTime(2024, 1, 15));
        Assert.Equal(2UL, jan15.UniqueUsers); // 2 unique users
        Assert.Equal(3UL, jan15.TotalEvents); // 3 total events
    }

    [Fact]
    public async Task Projection_ArgMax_ExecutesSuccessfully()
    {
        await using var context = CreateContext<ArgMaxProjectionContext>();

        // Create table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS price_history (
                Id UUID,
                ProductId UInt64,
                Price Decimal(18, 4),
                Timestamp DateTime64(3)
            ) ENGINE = MergeTree()
            ORDER BY (ProductId, Timestamp)
        ");

        // Insert price history
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO price_history (Id, ProductId, Price, Timestamp) VALUES
            (generateUUIDv4(), 1, 10.00, '2024-01-15 10:00:00'),
            (generateUUIDv4(), 1, 15.00, '2024-01-15 12:00:00'),
            (generateUUIDv4(), 1, 12.00, '2024-01-15 14:00:00'),
            (generateUUIDv4(), 2, 20.00, '2024-01-15 09:00:00'),
            (generateUUIDv4(), 2, 25.00, '2024-01-15 15:00:00')
        ");

        // Query using argMax to get latest price
        var results = await context.Database
            .SqlQueryRaw<LatestPriceResult>(
                "SELECT ProductId, argMax(Price, Timestamp) AS LatestPrice FROM price_history GROUP BY ProductId ORDER BY ProductId")
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal(12.00m, results[0].LatestPrice); // Product 1: latest at 14:00
        Assert.Equal(25.00m, results[1].LatestPrice); // Product 2: latest at 15:00
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

public class DailyUniqResult
{
    public DateTime EventDate { get; set; }
    public ulong UniqueUsers { get; set; }
    public ulong TotalEvents { get; set; }
}

public class LatestPriceResult
{
    public ulong ProductId { get; set; }
    public decimal LatestPrice { get; set; }
}

#endregion

#region Test Entities

public class ClickEvent
{
    public Guid Id { get; set; }
    public DateTime EventDate { get; set; }
    public ulong UserId { get; set; }
    public string EventType { get; set; } = string.Empty;
}

public class PriceHistory
{
    public Guid Id { get; set; }
    public ulong ProductId { get; set; }
    public decimal Price { get; set; }
    public DateTime Timestamp { get; set; }
}

public class SalesRecord
{
    public Guid Id { get; set; }
    public DateTime SaleDate { get; set; }
    public string Region { get; set; } = string.Empty;
    public double Amount { get; set; }
}

public class AggOrderItem
{
    public Guid Id { get; set; }
    public Guid OrderId { get; set; }
    public string ProductName { get; set; } = string.Empty;
    public int Quantity { get; set; }
}

#endregion

#region Test Contexts - Phase 1

public class UniqProjectionContext : DbContext
{
    public UniqProjectionContext(DbContextOptions<UniqProjectionContext> options)
        : base(options) { }

    public DbSet<ClickEvent> Events => Set<ClickEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ClickEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("click_events");
            entity.UseMergeTree(x => new { x.EventDate, x.Id });

            entity.HasProjection()
                .GroupBy(e => e.EventDate)
                .Select(g => new
                {
                    Date = g.Key,
                    UniqueUsers = ClickHouseAggregates.Uniq(g, e => e.UserId),
                    TotalEvents = g.Count()
                })
                .Build();
        });
    }
}

public class UniqExactProjectionContext : DbContext
{
    public UniqExactProjectionContext(DbContextOptions<UniqExactProjectionContext> options)
        : base(options) { }

    public DbSet<ClickEvent> Events => Set<ClickEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ClickEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("click_events");
            entity.UseMergeTree(x => new { x.EventDate, x.Id });

            entity.HasProjection()
                .GroupBy(e => e.EventDate)
                .Select(g => new
                {
                    Date = g.Key,
                    ExactUsers = ClickHouseAggregates.UniqExact(g, e => e.UserId)
                })
                .Build();
        });
    }
}

public class ArgMaxProjectionContext : DbContext
{
    public ArgMaxProjectionContext(DbContextOptions<ArgMaxProjectionContext> options)
        : base(options) { }

    public DbSet<PriceHistory> Prices => Set<PriceHistory>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PriceHistory>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("price_history");
            entity.UseMergeTree(x => new { x.ProductId, x.Timestamp });

            entity.HasProjection()
                .GroupBy(p => p.ProductId)
                .Select(g => new
                {
                    ProductId = g.Key,
                    LatestPrice = ClickHouseAggregates.ArgMax(g, p => p.Price, p => p.Timestamp)
                })
                .Build();
        });
    }
}

public class ArgMinProjectionContext : DbContext
{
    public ArgMinProjectionContext(DbContextOptions<ArgMinProjectionContext> options)
        : base(options) { }

    public DbSet<PriceHistory> Prices => Set<PriceHistory>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PriceHistory>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("price_history");
            entity.UseMergeTree(x => new { x.ProductId, x.Timestamp });

            entity.HasProjection()
                .GroupBy(p => p.ProductId)
                .Select(g => new
                {
                    ProductId = g.Key,
                    EarliestPrice = ClickHouseAggregates.ArgMin(g, p => p.Price, p => p.Timestamp)
                })
                .Build();
        });
    }
}

public class AnyProjectionContext : DbContext
{
    public AnyProjectionContext(DbContextOptions<AnyProjectionContext> options)
        : base(options) { }

    public DbSet<ClickEvent> Events => Set<ClickEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ClickEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("click_events");
            entity.UseMergeTree(x => new { x.EventDate, x.Id });

            entity.HasProjection()
                .GroupBy(e => e.EventDate)
                .Select(g => new
                {
                    Date = g.Key,
                    SampleEventType = ClickHouseAggregates.AnyValue(g, e => e.EventType)
                })
                .Build();
        });
    }
}

public class AnyLastProjectionContext : DbContext
{
    public AnyLastProjectionContext(DbContextOptions<AnyLastProjectionContext> options)
        : base(options) { }

    public DbSet<ClickEvent> Events => Set<ClickEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ClickEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("click_events");
            entity.UseMergeTree(x => new { x.EventDate, x.Id });

            entity.HasProjection()
                .GroupBy(e => e.EventDate)
                .Select(g => new
                {
                    Date = g.Key,
                    LastEventType = ClickHouseAggregates.AnyLastValue(g, e => e.EventType)
                })
                .Build();
        });
    }
}

#endregion

#region Test Contexts - Phase 2 (Statistical)

public class MedianProjectionContext : DbContext
{
    public MedianProjectionContext(DbContextOptions<MedianProjectionContext> options)
        : base(options) { }

    public DbSet<SalesRecord> Sales => Set<SalesRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SalesRecord>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("sales");
            entity.UseMergeTree(x => new { x.SaleDate, x.Id });

            entity.HasProjection()
                .GroupBy(s => s.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    MedianAmount = ClickHouseAggregates.Median(g, s => s.Amount)
                })
                .Build();
        });
    }
}

public class QuantileProjectionContext : DbContext
{
    public QuantileProjectionContext(DbContextOptions<QuantileProjectionContext> options)
        : base(options) { }

    public DbSet<SalesRecord> Sales => Set<SalesRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SalesRecord>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("sales");
            entity.UseMergeTree(x => new { x.SaleDate, x.Id });

            entity.HasProjection()
                .GroupBy(s => s.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    P95Amount = ClickHouseAggregates.Quantile(g, 0.95, s => s.Amount)
                })
                .Build();
        });
    }
}

public class StddevPopProjectionContext : DbContext
{
    public StddevPopProjectionContext(DbContextOptions<StddevPopProjectionContext> options)
        : base(options) { }

    public DbSet<SalesRecord> Sales => Set<SalesRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SalesRecord>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("sales");
            entity.UseMergeTree(x => new { x.SaleDate, x.Id });

            entity.HasProjection()
                .GroupBy(s => s.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    StdDev = ClickHouseAggregates.StddevPop(g, s => s.Amount)
                })
                .Build();
        });
    }
}

public class StddevSampProjectionContext : DbContext
{
    public StddevSampProjectionContext(DbContextOptions<StddevSampProjectionContext> options)
        : base(options) { }

    public DbSet<SalesRecord> Sales => Set<SalesRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SalesRecord>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("sales");
            entity.UseMergeTree(x => new { x.SaleDate, x.Id });

            entity.HasProjection()
                .GroupBy(s => s.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    StdDevSamp = ClickHouseAggregates.StddevSamp(g, s => s.Amount)
                })
                .Build();
        });
    }
}

public class VarPopProjectionContext : DbContext
{
    public VarPopProjectionContext(DbContextOptions<VarPopProjectionContext> options)
        : base(options) { }

    public DbSet<SalesRecord> Sales => Set<SalesRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SalesRecord>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("sales");
            entity.UseMergeTree(x => new { x.SaleDate, x.Id });

            entity.HasProjection()
                .GroupBy(s => s.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    VarPop = ClickHouseAggregates.VarPop(g, s => s.Amount)
                })
                .Build();
        });
    }
}

public class VarSampProjectionContext : DbContext
{
    public VarSampProjectionContext(DbContextOptions<VarSampProjectionContext> options)
        : base(options) { }

    public DbSet<SalesRecord> Sales => Set<SalesRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SalesRecord>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("sales");
            entity.UseMergeTree(x => new { x.SaleDate, x.Id });

            entity.HasProjection()
                .GroupBy(s => s.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    Variance = ClickHouseAggregates.VarSamp(g, s => s.Amount)
                })
                .Build();
        });
    }
}

#endregion

#region Test Contexts - Phase 3 (Arrays)

public class GroupArrayProjectionContext : DbContext
{
    public GroupArrayProjectionContext(DbContextOptions<GroupArrayProjectionContext> options)
        : base(options) { }

    public DbSet<AggOrderItem> Items => Set<AggOrderItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AggOrderItem>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("order_items");
            entity.UseMergeTree(x => new { x.OrderId, x.Id });

            entity.HasProjection()
                .GroupBy(i => i.OrderId)
                .Select(g => new
                {
                    OrderId = g.Key,
                    Products = ClickHouseAggregates.GroupArray(g, i => i.ProductName)
                })
                .Build();
        });
    }
}

public class GroupArrayLimitedProjectionContext : DbContext
{
    public GroupArrayLimitedProjectionContext(DbContextOptions<GroupArrayLimitedProjectionContext> options)
        : base(options) { }

    public DbSet<AggOrderItem> Items => Set<AggOrderItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AggOrderItem>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("order_items");
            entity.UseMergeTree(x => new { x.OrderId, x.Id });

            entity.HasProjection()
                .GroupBy(i => i.OrderId)
                .Select(g => new
                {
                    OrderId = g.Key,
                    Top10Products = ClickHouseAggregates.GroupArray(g, 10, i => i.ProductName)
                })
                .Build();
        });
    }
}

public class GroupUniqArrayProjectionContext : DbContext
{
    public GroupUniqArrayProjectionContext(DbContextOptions<GroupUniqArrayProjectionContext> options)
        : base(options) { }

    public DbSet<AggOrderItem> Items => Set<AggOrderItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AggOrderItem>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("order_items");
            entity.UseMergeTree(x => new { x.OrderId, x.Id });

            entity.HasProjection()
                .GroupBy(i => i.OrderId)
                .Select(g => new
                {
                    OrderId = g.Key,
                    UniqueProducts = ClickHouseAggregates.GroupUniqArray(g, i => i.ProductName)
                })
                .Build();
        });
    }
}

public class TopKProjectionContext : DbContext
{
    public TopKProjectionContext(DbContextOptions<TopKProjectionContext> options)
        : base(options) { }

    public DbSet<AggOrderItem> Items => Set<AggOrderItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AggOrderItem>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("order_items");
            entity.UseMergeTree(x => new { x.OrderId, x.Id });

            entity.HasProjection()
                .GroupBy(i => i.OrderId)
                .Select(g => new
                {
                    OrderId = g.Key,
                    TopProducts = ClickHouseAggregates.TopK(g, 5, i => i.ProductName)
                })
                .Build();
        });
    }
}

#endregion

#region Test Contexts - If Combinators

public class ArgMaxIfProjectionContext : DbContext
{
    public ArgMaxIfProjectionContext(DbContextOptions<ArgMaxIfProjectionContext> options) : base(options) { }
    public DbSet<PriceHistory> Prices => Set<PriceHistory>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PriceHistory>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("price_history");
            entity.UseMergeTree(x => new { x.ProductId, x.Timestamp });
            entity.HasProjection()
                .GroupBy(p => p.ProductId)
                .Select(g => new
                {
                    ProductId = g.Key,
                    LatestPositive = ClickHouseAggregates.ArgMaxIf(g, p => p.Price, p => p.Timestamp, p => p.Price > 0m)
                })
                .Build();
        });
    }
}

public class ArgMinIfProjectionContext : DbContext
{
    public ArgMinIfProjectionContext(DbContextOptions<ArgMinIfProjectionContext> options) : base(options) { }
    public DbSet<PriceHistory> Prices => Set<PriceHistory>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PriceHistory>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("price_history");
            entity.UseMergeTree(x => new { x.ProductId, x.Timestamp });
            entity.HasProjection()
                .GroupBy(p => p.ProductId)
                .Select(g => new
                {
                    ProductId = g.Key,
                    EarliestPositive = ClickHouseAggregates.ArgMinIf(g, p => p.Price, p => p.Timestamp, p => p.Price > 0m)
                })
                .Build();
        });
    }
}

public class TopKIfProjectionContext : DbContext
{
    public TopKIfProjectionContext(DbContextOptions<TopKIfProjectionContext> options) : base(options) { }
    public DbSet<AggOrderItem> Items => Set<AggOrderItem>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AggOrderItem>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("order_items");
            entity.UseMergeTree(x => new { x.OrderId, x.Id });
            entity.HasProjection()
                .GroupBy(i => i.OrderId)
                .Select(g => new
                {
                    OrderId = g.Key,
                    TopBulk = ClickHouseAggregates.TopKIf(g, 5, i => i.ProductName, i => i.Quantity > 1)
                })
                .Build();
        });
    }
}

public class TopKWeightedIfProjectionContext : DbContext
{
    public TopKWeightedIfProjectionContext(DbContextOptions<TopKWeightedIfProjectionContext> options) : base(options) { }
    public DbSet<AggOrderItem> Items => Set<AggOrderItem>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AggOrderItem>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("order_items");
            entity.UseMergeTree(x => new { x.OrderId, x.Id });
            entity.HasProjection()
                .GroupBy(i => i.OrderId)
                .Select(g => new
                {
                    OrderId = g.Key,
                    TopWeighted = ClickHouseAggregates.TopKWeightedIf(g, 3, i => i.ProductName, i => i.Quantity, i => i.Quantity > 0)
                })
                .Build();
        });
    }
}

public class GroupArrayIfProjectionContext : DbContext
{
    public GroupArrayIfProjectionContext(DbContextOptions<GroupArrayIfProjectionContext> options) : base(options) { }
    public DbSet<AggOrderItem> Items => Set<AggOrderItem>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AggOrderItem>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("order_items");
            entity.UseMergeTree(x => new { x.OrderId, x.Id });
            entity.HasProjection()
                .GroupBy(i => i.OrderId)
                .Select(g => new
                {
                    OrderId = g.Key,
                    BulkProducts = ClickHouseAggregates.GroupArrayIf(g, i => i.ProductName, i => i.Quantity > 1)
                })
                .Build();
        });
    }
}

public class GroupArrayIfLimitedProjectionContext : DbContext
{
    public GroupArrayIfLimitedProjectionContext(DbContextOptions<GroupArrayIfLimitedProjectionContext> options) : base(options) { }
    public DbSet<AggOrderItem> Items => Set<AggOrderItem>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AggOrderItem>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("order_items");
            entity.UseMergeTree(x => new { x.OrderId, x.Id });
            entity.HasProjection()
                .GroupBy(i => i.OrderId)
                .Select(g => new
                {
                    OrderId = g.Key,
                    BulkFirstTen = ClickHouseAggregates.GroupArrayIf(g, 10, i => i.ProductName, i => i.Quantity > 1)
                })
                .Build();
        });
    }
}

public class GroupUniqArrayIfProjectionContext : DbContext
{
    public GroupUniqArrayIfProjectionContext(DbContextOptions<GroupUniqArrayIfProjectionContext> options) : base(options) { }
    public DbSet<AggOrderItem> Items => Set<AggOrderItem>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AggOrderItem>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("order_items");
            entity.UseMergeTree(x => new { x.OrderId, x.Id });
            entity.HasProjection()
                .GroupBy(i => i.OrderId)
                .Select(g => new
                {
                    OrderId = g.Key,
                    UniqBulk = ClickHouseAggregates.GroupUniqArrayIf(g, i => i.ProductName, i => i.Quantity > 1)
                })
                .Build();
        });
    }
}

public class MedianIfProjectionContext : DbContext
{
    public MedianIfProjectionContext(DbContextOptions<MedianIfProjectionContext> options) : base(options) { }
    public DbSet<SalesRecord> Sales => Set<SalesRecord>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SalesRecord>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("sales");
            entity.UseMergeTree(x => new { x.SaleDate, x.Id });
            entity.HasProjection()
                .GroupBy(s => s.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    MedianPositive = ClickHouseAggregates.MedianIf(g, s => s.Amount, s => s.Amount > 0)
                })
                .Build();
        });
    }
}

public class StddevPopIfProjectionContext : DbContext
{
    public StddevPopIfProjectionContext(DbContextOptions<StddevPopIfProjectionContext> options) : base(options) { }
    public DbSet<SalesRecord> Sales => Set<SalesRecord>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SalesRecord>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("sales");
            entity.UseMergeTree(x => new { x.SaleDate, x.Id });
            entity.HasProjection()
                .GroupBy(s => s.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    StdDevPositive = ClickHouseAggregates.StddevPopIf(g, s => s.Amount, s => s.Amount > 0)
                })
                .Build();
        });
    }
}

public class UniqCombinedIfProjectionContext : DbContext
{
    public UniqCombinedIfProjectionContext(DbContextOptions<UniqCombinedIfProjectionContext> options) : base(options) { }
    public DbSet<ClickEvent> Events => Set<ClickEvent>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ClickEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("click_events");
            entity.UseMergeTree(x => new { x.EventDate, x.Id });
            entity.HasProjection()
                .GroupBy(e => e.EventDate)
                .Select(g => new
                {
                    Date = g.Key,
                    ClickUsers = ClickHouseAggregates.UniqCombinedIf(g, e => e.UserId, e => e.EventType == "click")
                })
                .Build();
        });
    }
}

public class QuantileTDigestIfProjectionContext : DbContext
{
    public QuantileTDigestIfProjectionContext(DbContextOptions<QuantileTDigestIfProjectionContext> options) : base(options) { }
    public DbSet<SalesRecord> Sales => Set<SalesRecord>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SalesRecord>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("sales");
            entity.UseMergeTree(x => new { x.SaleDate, x.Id });
            entity.HasProjection()
                .GroupBy(s => s.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    P95Positive = ClickHouseAggregates.QuantileTDigestIf(g, 0.95, s => s.Amount, s => s.Amount > 0)
                })
                .Build();
        });
    }
}

public class QuantileDDIfProjectionContext : DbContext
{
    public QuantileDDIfProjectionContext(DbContextOptions<QuantileDDIfProjectionContext> options) : base(options) { }
    public DbSet<SalesRecord> Sales => Set<SalesRecord>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SalesRecord>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("sales");
            entity.UseMergeTree(x => new { x.SaleDate, x.Id });
            entity.HasProjection()
                .GroupBy(s => s.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    P95 = ClickHouseAggregates.QuantileDDIf(g, 0.01, 0.95, s => s.Amount, s => s.Amount > 0)
                })
                .Build();
        });
    }
}

public class QuantilesIfProjectionContext : DbContext
{
    public QuantilesIfProjectionContext(DbContextOptions<QuantilesIfProjectionContext> options) : base(options) { }
    public DbSet<SalesRecord> Sales => Set<SalesRecord>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SalesRecord>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("sales");
            entity.UseMergeTree(x => new { x.SaleDate, x.Id });
            entity.HasProjection()
                .GroupBy(s => s.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    Quantiles = ClickHouseAggregates.QuantilesIf(g, new[] { 0.5, 0.9, 0.99 }, s => s.Amount, s => s.Amount > 0)
                })
                .Build();
        });
    }
}

public class MultiIfProjectionContext : DbContext
{
    public MultiIfProjectionContext(DbContextOptions<MultiIfProjectionContext> options) : base(options) { }
    public DbSet<SalesRecord> Sales => Set<SalesRecord>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SalesRecord>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("sales");
            entity.UseMergeTree(x => new { x.SaleDate, x.Id });
            entity.HasProjection()
                .GroupBy(s => s.Region)
                .Select(g => new
                {
                    Region = g.Key,
                    PositiveTotal = ClickHouseAggregates.SumIf(g, s => s.Amount, s => s.Amount > 0),
                    ZeroCount = ClickHouseAggregates.CountIf(g, s => s.Amount == 0),
                    PositiveUniq = ClickHouseAggregates.UniqIf(g, s => s.Id, s => s.Amount > 0)
                })
                .Build();
        });
    }
}

#endregion
