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

        // The expression should be stored for later translation
        var expressionAnnotation = entityType.FindAnnotation("ClickHouse:MaterializedViewExpression");
        Assert.NotNull(expressionAnnotation?.Value);
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

#endregion
