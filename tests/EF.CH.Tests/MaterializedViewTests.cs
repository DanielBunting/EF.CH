using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests;

public class MaterializedViewTests : IAsyncLifetime
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

    #region DDL Generation Tests

    [Fact]
    public void MigrationsSqlGenerator_GeneratesCreateMaterializedView_RawSql()
    {
        using var context = CreateContext<RawSqlMaterializedViewContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var entityType = model.FindEntityType(typeof(MvDailySummary));
        Assert.NotNull(entityType);

        // Create a CreateTableOperation that represents the materialized view
        var createTableOp = new CreateTableOperation
        {
            Name = "DailySummary_MV",
            Columns =
            {
                new AddColumnOperation { Name = "Date", ClrType = typeof(DateTime), ColumnType = "Date" },
                new AddColumnOperation { Name = "ProductId", ClrType = typeof(int), ColumnType = "Int32" },
                new AddColumnOperation { Name = "TotalQuantity", ClrType = typeof(decimal), ColumnType = "Decimal(18,4)" },
                new AddColumnOperation { Name = "TotalRevenue", ClrType = typeof(decimal), ColumnType = "Decimal(18,4)" }
            }
        };

        // Add materialized view annotations
        createTableOp.AddAnnotation("ClickHouse:MaterializedView", true);
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewSource", "Orders");
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewQuery", @"
            SELECT
                toDate(OrderDate) AS Date,
                ProductId,
                sum(Quantity) AS TotalQuantity,
                sum(Revenue) AS TotalRevenue
            FROM Orders
            GROUP BY Date, ProductId");
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewPopulate", false);
        createTableOp.AddAnnotation("ClickHouse:Engine", "SummingMergeTree");
        createTableOp.AddAnnotation("ClickHouse:OrderBy", new[] { "Date", "ProductId" });

        var commands = generator.Generate(new[] { createTableOp }, model);
        var sql = commands.First().CommandText;

        // Verify CREATE MATERIALIZED VIEW syntax
        Assert.Contains("CREATE MATERIALIZED VIEW", sql);
        Assert.Contains("\"DailySummary_MV\"", sql);
        Assert.Contains("ENGINE = SummingMergeTree()", sql);
        Assert.Contains("ORDER BY", sql);
        Assert.Contains("AS", sql);
        Assert.Contains("SELECT", sql);
        Assert.Contains("toDate(OrderDate)", sql);
        Assert.Contains("sum(Quantity)", sql);
        Assert.DoesNotContain("POPULATE", sql);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesCreateMaterializedView_WithPopulate()
    {
        using var context = CreateContext<RawSqlMaterializedViewContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var createTableOp = new CreateTableOperation
        {
            Name = "DailySummary_MV",
            Columns =
            {
                new AddColumnOperation { Name = "Date", ClrType = typeof(DateTime), ColumnType = "Date" },
                new AddColumnOperation { Name = "ProductId", ClrType = typeof(int), ColumnType = "Int32" },
                new AddColumnOperation { Name = "TotalQuantity", ClrType = typeof(decimal), ColumnType = "Decimal(18,4)" }
            }
        };

        createTableOp.AddAnnotation("ClickHouse:MaterializedView", true);
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewSource", "Orders");
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewQuery", "SELECT toDate(OrderDate) AS Date, ProductId, sum(Quantity) AS TotalQuantity FROM Orders GROUP BY Date, ProductId");
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewPopulate", true);
        createTableOp.AddAnnotation("ClickHouse:Engine", "SummingMergeTree");
        createTableOp.AddAnnotation("ClickHouse:OrderBy", new[] { "Date", "ProductId" });

        var commands = generator.Generate(new[] { createTableOp }, model);
        var sql = commands.First().CommandText;

        Assert.Contains("POPULATE", sql);
    }

    [Fact]
    public void MigrationsSqlGenerator_ThrowsWhenNoQueryDefined()
    {
        using var context = CreateContext<RawSqlMaterializedViewContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var createTableOp = new CreateTableOperation
        {
            Name = "BadView",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(int), ColumnType = "Int32" }
            }
        };

        createTableOp.AddAnnotation("ClickHouse:MaterializedView", true);
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewSource", "SomeTable");
        // No query annotation

        var ex = Assert.Throws<InvalidOperationException>(
            () => generator.Generate(new[] { createTableOp }, model));

        Assert.Contains("must have a view query defined", ex.Message);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesSimpleProjectionMV()
    {
        using var context = CreateContext<SimpleProjectionMaterializedViewContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var entityType = model.FindEntityType(typeof(MvProcessedEvent));
        Assert.NotNull(entityType);

        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;

        var createTableOp = new CreateTableOperation
        {
            Name = "ProcessedEvents_MV",
            Columns =
            {
                new AddColumnOperation { Name = "EventNameId", ClrType = typeof(ulong), ColumnType = "UInt64" },
                new AddColumnOperation { Name = "EventTime", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
                new AddColumnOperation { Name = "Version", ClrType = typeof(long), ColumnType = "Int64" },
                new AddColumnOperation { Name = "Value", ClrType = typeof(decimal), ColumnType = "Decimal(18,4)" },
                new AddColumnOperation { Name = "IsActive", ClrType = typeof(byte), ColumnType = "UInt8" }
            }
        };

        createTableOp.AddAnnotation("ClickHouse:MaterializedView", true);
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewSource", "RawEvents");
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewQuery", query);
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewPopulate", false);
        createTableOp.AddAnnotation("ClickHouse:Engine", "ReplacingMergeTree");
        createTableOp.AddAnnotation("ClickHouse:OrderBy", new[] { "EventNameId", "EventTime" });
        createTableOp.AddAnnotation("ClickHouse:VersionColumn", "Version");

        var commands = generator.Generate(new[] { createTableOp }, model);
        var sql = commands.First().CommandText;

        // Verify CREATE MATERIALIZED VIEW syntax
        Assert.Contains("CREATE MATERIALIZED VIEW", sql);
        Assert.Contains("\"ProcessedEvents_MV\"", sql);
        Assert.Contains("ENGINE = ReplacingMergeTree(\"Version\")", sql);
        Assert.Contains("ORDER BY", sql);
        Assert.Contains("AS", sql);

        // Verify ClickHouse functions are in the query
        Assert.Contains("cityHash64", sql);
        Assert.Contains("toUnixTimestamp64Milli", sql);

        // Verify no GROUP BY (simple projection)
        Assert.DoesNotContain("GROUP BY", sql);
        Assert.DoesNotContain("POPULATE", sql);
    }

    #endregion

    #region Fluent API Tests

    [Fact]
    public void AsMaterializedViewRaw_SetsCorrectAnnotations()
    {
        using var context = CreateContext<RawSqlMaterializedViewContext>();
        var entityType = context.Model.FindEntityType(typeof(MvDailySummary));

        Assert.NotNull(entityType);
        Assert.True((bool?)entityType.FindAnnotation("ClickHouse:MaterializedView")?.Value);
        Assert.Equal("Orders", entityType.FindAnnotation("ClickHouse:MaterializedViewSource")?.Value);
        Assert.NotNull(entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value);
        Assert.False((bool?)entityType.FindAnnotation("ClickHouse:MaterializedViewPopulate")?.Value);
    }

    [Fact]
    public void AsMaterializedView_Linq_SetsCorrectAnnotations()
    {
        using var context = CreateContext<LinqMaterializedViewContext>();
        var entityType = context.Model.FindEntityType(typeof(MvHourlySummary));

        Assert.NotNull(entityType);
        Assert.True((bool?)entityType.FindAnnotation("ClickHouse:MaterializedView")?.Value);
        Assert.NotNull(entityType.FindAnnotation("ClickHouse:MaterializedViewSource")?.Value);
        Assert.False((bool?)entityType.FindAnnotation("ClickHouse:MaterializedViewPopulate")?.Value);

        // LINQ expression is translated to SQL at configuration time
        var queryAnnotation = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery");
        Assert.NotNull(queryAnnotation?.Value);

        var query = queryAnnotation.Value as string;
        Assert.NotNull(query);
        Assert.Contains("GROUP BY", query);  // Confirms LINQ GroupBy was translated
    }

    [Fact]
    public void AsMaterializedView_SimpleProjection_SetsCorrectAnnotations()
    {
        using var context = CreateContext<SimpleProjectionMaterializedViewContext>();
        var entityType = context.Model.FindEntityType(typeof(MvProcessedEvent));

        Assert.NotNull(entityType);
        Assert.True((bool?)entityType.FindAnnotation("ClickHouse:MaterializedView")?.Value);
        Assert.Equal("RawEvents", entityType.FindAnnotation("ClickHouse:MaterializedViewSource")?.Value);
        Assert.False((bool?)entityType.FindAnnotation("ClickHouse:MaterializedViewPopulate")?.Value);

        // Verify the SQL query was generated (not stored as expression)
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        // Simple projection should contain CityHash64 and toUnixTimestamp64Milli
        Assert.Contains("cityHash64", query);
        Assert.Contains("toUnixTimestamp64Milli", query);

        // Simple projection should NOT contain GROUP BY
        Assert.DoesNotContain("GROUP BY", query);
    }

    #endregion

    #region LINQ Translation Tests

    [Fact]
    public void AsMaterializedView_AllAggregates_TranslatesWithColumnReferences()
    {
        // Tests Fix 1: Sum, Max, Min, Avg should include column references
        using var context = CreateContext<AllAggregatesContext>();
        var entityType = context.Model.FindEntityType(typeof(MvAllAggregatesSummary));

        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        // All aggregates should have column references, not empty parentheses
        Assert.Contains("sum(\"Amount\")", query);
        Assert.Contains("max(\"Amount\")", query);
        Assert.Contains("min(\"Amount\")", query);
        Assert.Contains("avg(\"Amount\")", query);
        Assert.Contains("count()", query);

        // Should NOT have empty aggregate calls (except count())
        Assert.DoesNotContain("sum()", query);
        Assert.DoesNotContain("max()", query);
        Assert.DoesNotContain("min()", query);
        Assert.DoesNotContain("avg()", query);
    }

    [Fact]
    public void AsMaterializedView_SingleKey_TranslatesDirectKeyAccess()
    {
        // Tests Fix 3: Single-value group key should translate g.Key directly
        using var context = CreateContext<SingleKeyGroupByContext>();
        var entityType = context.Model.FindEntityType(typeof(MvDailyStats));

        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        // Should have toDate("EventTime") for the Date column, not just "Key"
        Assert.Contains("toDate(\"EventTime\")", query);
        Assert.DoesNotContain("\"Key\"", query);

        // Should have GROUP BY with the date expression
        Assert.Contains("GROUP BY", query);
    }

    [Fact]
    public void AsMaterializedView_CountIf_TranslatesConditionalCount()
    {
        // Tests Fix 2: Count with predicate should become countIf
        using var context = CreateContext<CountIfContext>();
        var entityType = context.Model.FindEntityType(typeof(MvCountIfSummary));

        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        // Should have countIf with the condition
        Assert.Contains("countIf", query);
        Assert.Contains("\"Amount\"", query);
        Assert.Contains("1000", query);

        // Regular count should still work
        Assert.Contains("count()", query);
    }

    [Fact]
    public void AsMaterializedView_ClickHouseAggregates_TranslatesCorrectly()
    {
        // Tests ClickHouse-specific aggregates: Uniq, AnyValue
        using var context = CreateContext<ClickHouseAggregatesContext>();
        var entityType = context.Model.FindEntityType(typeof(MvClickHouseAggSummary));

        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        // Should have uniq and any functions
        Assert.Contains("uniq(\"UserId\")", query);
        Assert.Contains("any(\"EventName\")", query);
    }

    [Fact]
    public void AsMaterializedView_ToStartOfHour_TranslatesDateTimeFunction()
    {
        // Tests Fix 4: Method call group key (ToStartOfHour) should be captured
        using var context = CreateContext<DateTimeFunctionsContext>();
        var entityType = context.Model.FindEntityType(typeof(MvHourlyEventStats));

        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        // Should have toStartOfHour function
        Assert.Contains("toStartOfHour(\"EventTime\")", query);

        // Should NOT have "Key" as a literal
        Assert.DoesNotContain("\"Key\"", query);

        // Should have GROUP BY with the function
        Assert.Contains("GROUP BY", query);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task CreateMaterializedView_RawSql_ExecutesSuccessfully()
    {
        await using var context = CreateContext<RawSqlMaterializedViewContext>();

        // Create source table first
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS Orders (
                OrderId Int32,
                OrderDate DateTime64(3),
                ProductId Int32,
                Quantity Decimal(18, 4),
                Revenue Decimal(18, 4)
            ) ENGINE = MergeTree()
            ORDER BY (OrderDate, OrderId)
        ");

        // Create the materialized view
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE MATERIALIZED VIEW IF NOT EXISTS DailySummary_MV
            ENGINE = SummingMergeTree()
            ORDER BY (Date, ProductId)
            AS
            SELECT
                toDate(OrderDate) AS Date,
                ProductId,
                sum(Quantity) AS TotalQuantity,
                sum(Revenue) AS TotalRevenue
            FROM Orders
            GROUP BY Date, ProductId
        ");

        // Insert some data into source table
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO Orders (OrderId, OrderDate, ProductId, Quantity, Revenue) VALUES
            (1, '2024-01-15 10:00:00', 100, 5, 99.99),
            (2, '2024-01-15 11:00:00', 100, 3, 59.97),
            (3, '2024-01-15 12:00:00', 200, 2, 39.98)
        ");

        // Query the materialized view
        var results = await context.Database.SqlQueryRaw<MvDailySummaryResult>(
            "SELECT Date, ProductId, TotalQuantity, TotalRevenue FROM DailySummary_MV FINAL ORDER BY Date, ProductId"
        ).ToListAsync();

        Assert.Equal(2, results.Count);

        var product100 = results.First(r => r.ProductId == 100);
        Assert.Equal(8m, product100.TotalQuantity);
        Assert.Equal(159.96m, product100.TotalRevenue);
    }

    [Fact]
    public async Task CreateMaterializedView_SimpleProjection_ExecutesSuccessfully()
    {
        await using var context = CreateContext<SimpleProjectionMaterializedViewContext>();

        // Create source table first
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS RawEvents (
                EventName String,
                EventTime DateTime64(3),
                UserId String,
                Value Decimal(18, 4)
            ) ENGINE = MergeTree()
            ORDER BY (EventName, EventTime)
        ");

        // Get the translated SQL query from the model
        var entityType = context.Model.FindEntityType(typeof(MvProcessedEvent));
        var selectSql = entityType?.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(selectSql);

        // Create the materialized view with simple projection
        await context.Database.ExecuteSqlRawAsync($@"
            CREATE MATERIALIZED VIEW IF NOT EXISTS ProcessedEvents_MV
            ENGINE = ReplacingMergeTree(""Version"")
            ORDER BY (""EventNameId"", ""EventTime"")
            AS
            {selectSql}
        ");

        // Insert some data into source table
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO RawEvents (EventName, EventTime, UserId, Value) VALUES
            ('UserLogin', '2024-01-15 10:00:00', 'user1', 1.0),
            ('UserLogin', '2024-01-15 10:05:00', 'user2', 1.0),
            ('Purchase', '2024-01-15 11:00:00', 'user1', 99.99)
        ");

        // Query the materialized view
        var results = await context.Database.SqlQueryRaw<MvProcessedEventResult>(
            "SELECT EventNameId, EventTime, Version, Value, IsActive FROM ProcessedEvents_MV ORDER BY EventTime"
        ).ToListAsync();

        Assert.Equal(3, results.Count);

        // Verify cityHash64 was applied (all EventNameId values should be non-zero)
        Assert.All(results, r => Assert.NotEqual(0UL, r.EventNameId));

        // Verify toUnixTimestamp64Milli was applied (Version > 0)
        Assert.All(results, r => Assert.True(r.Version > 0));

        // Verify constant IsActive = 1
        Assert.All(results, r => Assert.Equal((byte)1, r.IsActive));
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

#region Test Entities

public class MvOrder
{
    public int OrderId { get; set; }
    public DateTime OrderDate { get; set; }
    public int ProductId { get; set; }
    public decimal Quantity { get; set; }
    public decimal Revenue { get; set; }
}

// Entities for simple projection MV tests (without GroupBy)
public class MvRawEvent
{
    public string EventName { get; set; } = string.Empty;
    public DateTime EventTime { get; set; }
    public string UserId { get; set; } = string.Empty;
    public decimal Value { get; set; }
}

public class MvProcessedEvent
{
    public ulong EventNameId { get; set; }          // cityHash64(EventName)
    public DateTime EventTime { get; set; }
    public long Version { get; set; }               // toUnixTimestamp64Milli(EventTime)
    public decimal Value { get; set; }
    public byte IsActive { get; set; }              // Constant = 1
}

public class MvDailySummary
{
    public DateTime Date { get; set; }
    public int ProductId { get; set; }
    public decimal TotalQuantity { get; set; }
    public decimal TotalRevenue { get; set; }
}

public class MvHourlySummary
{
    public DateTime Hour { get; set; }
    public int ProductId { get; set; }
    public int OrderCount { get; set; }
    public decimal TotalRevenue { get; set; }
}

public class MvDailySummaryResult
{
    public DateTime Date { get; set; }
    public int ProductId { get; set; }
    public decimal TotalQuantity { get; set; }
    public decimal TotalRevenue { get; set; }
}

public class MvProcessedEventResult
{
    public ulong EventNameId { get; set; }
    public DateTime EventTime { get; set; }
    public long Version { get; set; }
    public decimal Value { get; set; }
    public byte IsActive { get; set; }
}

#endregion

#region Test Contexts

public class RawSqlMaterializedViewContext : DbContext
{
    public RawSqlMaterializedViewContext(DbContextOptions<RawSqlMaterializedViewContext> options)
        : base(options) { }

    public DbSet<MvOrder> Orders => Set<MvOrder>();
    public DbSet<MvDailySummary> DailySummaries => Set<MvDailySummary>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MvOrder>(entity =>
        {
            entity.HasKey(e => e.OrderId);
            entity.ToTable("Orders");
            entity.UseMergeTree(x => new { x.OrderDate, x.OrderId });
        });

        modelBuilder.Entity<MvDailySummary>(entity =>
        {
            entity.ToTable("DailySummary_MV");
            entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });
            entity.AsMaterializedViewRaw(
                sourceTable: "Orders",
                selectSql: @"
                    SELECT
                        toDate(OrderDate) AS Date,
                        ProductId,
                        sum(Quantity) AS TotalQuantity,
                        sum(Revenue) AS TotalRevenue
                    FROM Orders
                    GROUP BY Date, ProductId
                ",
                populate: false
            );
        });
    }
}

public class LinqMaterializedViewContext : DbContext
{
    public LinqMaterializedViewContext(DbContextOptions<LinqMaterializedViewContext> options)
        : base(options) { }

    public DbSet<MvOrder> Orders => Set<MvOrder>();
    public DbSet<MvHourlySummary> HourlySummaries => Set<MvHourlySummary>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MvOrder>(entity =>
        {
            entity.HasKey(e => e.OrderId);
            entity.ToTable("Orders");
            entity.UseMergeTree(x => new { x.OrderDate, x.OrderId });
        });

        modelBuilder.Entity<MvHourlySummary>(entity =>
        {
            entity.ToTable("HourlySummary_MV");
            entity.UseSummingMergeTree(x => new { x.Hour, x.ProductId });
            entity.AsMaterializedView<MvHourlySummary, MvOrder>(
                query: orders => orders
                    .GroupBy(o => new { Hour = o.OrderDate.Date, o.ProductId })
                    .Select(g => new MvHourlySummary
                    {
                        Hour = g.Key.Hour,
                        ProductId = g.Key.ProductId,
                        OrderCount = g.Count(),
                        TotalRevenue = g.Sum(o => o.Revenue)
                    }),
                populate: false
            );
        });
    }
}

/// <summary>
/// Context for testing simple projection MV (Select without GroupBy).
/// Demonstrates data transformation with CityHash64, ToUnixTimestamp64Milli, and constants.
/// </summary>
public class SimpleProjectionMaterializedViewContext : DbContext
{
    public SimpleProjectionMaterializedViewContext(DbContextOptions<SimpleProjectionMaterializedViewContext> options)
        : base(options) { }

    public DbSet<MvRawEvent> RawEvents => Set<MvRawEvent>();
    public DbSet<MvProcessedEvent> ProcessedEvents => Set<MvProcessedEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Source table
        modelBuilder.Entity<MvRawEvent>(entity =>
        {
            entity.ToTable("RawEvents");
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.EventName, x.EventTime });
        });

        // MV with simple projection (no GroupBy)
        modelBuilder.Entity<MvProcessedEvent>(entity =>
        {
            entity.ToTable("ProcessedEvents_MV");
            entity.UseReplacingMergeTree(
                x => x.Version,
                x => new { x.EventNameId, x.EventTime });

            entity.AsMaterializedView<MvProcessedEvent, MvRawEvent>(
                query: raw => raw.Select(r => new MvProcessedEvent
                {
                    EventNameId = r.EventName.CityHash64(),
                    EventTime = r.EventTime,
                    Version = r.EventTime.ToUnixTimestamp64Milli(),
                    Value = r.Value,
                    IsActive = 1
                }),
                populate: false);
        });
    }
}

#region Test Contexts for LINQ Translation Tests

// Entity for all aggregates test
public class MvAllAggregatesSource
{
    public DateTime EventTime { get; set; }
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

public class MvAllAggregatesSummary
{
    public DateTime Date { get; set; }
    public string Category { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public decimal MaxAmount { get; set; }
    public decimal MinAmount { get; set; }
    public decimal AvgAmount { get; set; }
    public int EventCount { get; set; }
}

public class AllAggregatesContext : DbContext
{
    public AllAggregatesContext(DbContextOptions<AllAggregatesContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MvAllAggregatesSource>(entity =>
        {
            entity.ToTable("events");
            entity.HasNoKey();
            entity.UseMergeTree(x => x.EventTime);
        });

        modelBuilder.Entity<MvAllAggregatesSummary>(entity =>
        {
            entity.ToTable("all_aggregates_mv");
            entity.UseSummingMergeTree(x => new { x.Date, x.Category });
            entity.AsMaterializedView<MvAllAggregatesSummary, MvAllAggregatesSource>(
                query: events => events
                    .GroupBy(e => new { Date = e.EventTime.Date, e.Category })
                    .Select(g => new MvAllAggregatesSummary
                    {
                        Date = g.Key.Date,
                        Category = g.Key.Category,
                        TotalAmount = g.Sum(e => e.Amount),
                        MaxAmount = g.Max(e => e.Amount),
                        MinAmount = g.Min(e => e.Amount),
                        AvgAmount = (decimal)g.Average(e => e.Amount),
                        EventCount = g.Count()
                    }),
                populate: false);
        });
    }
}

// Entity for single key test
public class MvDailyStats
{
    public DateTime Date { get; set; }
    public int TotalEvents { get; set; }
}

public class SingleKeyGroupByContext : DbContext
{
    public SingleKeyGroupByContext(DbContextOptions<SingleKeyGroupByContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MvAllAggregatesSource>(entity =>
        {
            entity.ToTable("events");
            entity.HasNoKey();
            entity.UseMergeTree(x => x.EventTime);
        });

        modelBuilder.Entity<MvDailyStats>(entity =>
        {
            entity.ToTable("daily_stats_mv");
            entity.UseSummingMergeTree(x => x.Date);
            entity.AsMaterializedView<MvDailyStats, MvAllAggregatesSource>(
                query: events => events
                    .GroupBy(e => e.EventTime.Date)  // Single-value key
                    .Select(g => new MvDailyStats
                    {
                        Date = g.Key,  // Direct g.Key access
                        TotalEvents = g.Count()
                    }),
                populate: false);
        });
    }
}

// Entity for CountIf test
public class MvCountIfSummary
{
    public DateTime Date { get; set; }
    public int TotalCount { get; set; }
    public int HighValueCount { get; set; }
}

public class CountIfContext : DbContext
{
    public CountIfContext(DbContextOptions<CountIfContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MvAllAggregatesSource>(entity =>
        {
            entity.ToTable("events");
            entity.HasNoKey();
            entity.UseMergeTree(x => x.EventTime);
        });

        modelBuilder.Entity<MvCountIfSummary>(entity =>
        {
            entity.ToTable("countif_summary_mv");
            entity.UseSummingMergeTree(x => x.Date);
            entity.AsMaterializedView<MvCountIfSummary, MvAllAggregatesSource>(
                query: events => events
                    .GroupBy(e => e.EventTime.Date)
                    .Select(g => new MvCountIfSummary
                    {
                        Date = g.Key,
                        TotalCount = g.Count(),
                        HighValueCount = g.Count(e => e.Amount > 1000)  // CountIf
                    }),
                populate: false);
        });
    }
}

// Entity for ClickHouse aggregates test
public class MvClickHouseAggSource
{
    public DateTime EventTime { get; set; }
    public string EventName { get; set; } = string.Empty;
    public string UserId { get; set; } = string.Empty;
}

public class MvClickHouseAggSummary
{
    public DateTime Date { get; set; }
    public ulong UniqueUsers { get; set; }
    public string SampleEventName { get; set; } = string.Empty;
}

public class ClickHouseAggregatesContext : DbContext
{
    public ClickHouseAggregatesContext(DbContextOptions<ClickHouseAggregatesContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MvClickHouseAggSource>(entity =>
        {
            entity.ToTable("events");
            entity.HasNoKey();
            entity.UseMergeTree(x => x.EventTime);
        });

        modelBuilder.Entity<MvClickHouseAggSummary>(entity =>
        {
            entity.ToTable("ch_agg_summary_mv");
            entity.UseSummingMergeTree(x => x.Date);
            entity.AsMaterializedView<MvClickHouseAggSummary, MvClickHouseAggSource>(
                query: events => events
                    .GroupBy(e => e.EventTime.Date)
                    .Select(g => new MvClickHouseAggSummary
                    {
                        Date = g.Key,
                        UniqueUsers = ClickHouseAggregates.Uniq(g, e => e.UserId),
                        SampleEventName = ClickHouseAggregates.AnyValue(g, e => e.EventName)
                    }),
                populate: false);
        });
    }
}

// Entity for ToStartOfHour test
public class MvHourlyEventStats
{
    public DateTime Hour { get; set; }
    public int EventCount { get; set; }
}

public class DateTimeFunctionsContext : DbContext
{
    public DateTimeFunctionsContext(DbContextOptions<DateTimeFunctionsContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MvAllAggregatesSource>(entity =>
        {
            entity.ToTable("events");
            entity.HasNoKey();
            entity.UseMergeTree(x => x.EventTime);
        });

        modelBuilder.Entity<MvHourlyEventStats>(entity =>
        {
            entity.ToTable("hourly_stats_mv");
            entity.UseSummingMergeTree(x => x.Hour);
            entity.AsMaterializedView<MvHourlyEventStats, MvAllAggregatesSource>(
                query: events => events
                    .GroupBy(e => e.EventTime.ToStartOfHour())  // Method call group key
                    .Select(g => new MvHourlyEventStats
                    {
                        Hour = g.Key,  // Direct g.Key access to method call result
                        EventCount = g.Count()
                    }),
                populate: false);
        });
    }
}

#endregion

#endregion
