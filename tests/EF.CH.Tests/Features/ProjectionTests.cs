using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Operations;
using EF.CH.Projections;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

public class ProjectionTests : IAsyncLifetime
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

    #region Fluent API Tests

    [Fact]
    public void HasProjection_RawSql_SetsCorrectAnnotations()
    {
        using var context = CreateContext<RawSqlProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(PrjOrder));

        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Equal("prj_by_region", projections[0].Name);
        Assert.Equal(ProjectionType.Raw, projections[0].Type);
        Assert.Contains("ORDER BY", projections[0].SelectSql);
        Assert.True(projections[0].Materialize);
    }

    [Fact]
    public void HasProjection_SortOrder_SetsCorrectAnnotations()
    {
        using var context = CreateContext<SortOrderProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(PrjOrder));

        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        // Auto-generated name: {table_name}__prj_ord__{column1}__{column2}
        Assert.Equal("orders__prj_ord__customer_id__order_date", projections[0].Name);
        Assert.Equal(ProjectionType.SortOrder, projections[0].Type);
        Assert.Contains("ORDER BY", projections[0].SelectSql);
        Assert.Contains("CustomerId", projections[0].SelectSql);
        Assert.Contains("OrderDate", projections[0].SelectSql);
    }

    [Fact]
    public void HasProjection_Aggregation_SetsCorrectAnnotations()
    {
        using var context = CreateContext<AggregationProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(PrjOrder));

        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        // Auto-generated name: {table_name}__prj_agg__{field1}__{field2}__{field3}
        Assert.Equal("orders__prj_agg__date__total_amount__order_count", projections[0].Name);
        Assert.Equal(ProjectionType.Aggregation, projections[0].Type);
        Assert.Contains("GROUP BY", projections[0].SelectSql);
        Assert.Contains("sum", projections[0].SelectSql);
        Assert.Contains("count()", projections[0].SelectSql);
    }

    [Fact]
    public void HasProjection_MultipleProjections_AllStored()
    {
        using var context = CreateContext<MultipleProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(PrjOrder));

        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Equal(2, projections.Count);
        // Auto-generated name for sort-order, explicit name for raw SQL
        Assert.Contains(projections, p => p.Name == "orders__prj_ord__customer_id");
        Assert.Contains(projections, p => p.Name == "prj_by_region");
    }

    [Fact]
    public void RemoveProjection_RemovesFromList()
    {
        using var context = CreateContext<RemoveProjectionContext>();
        var entityType = context.Model.FindEntityType(typeof(PrjOrder));

        Assert.NotNull(entityType);

        var projections = entityType.FindAnnotation(ClickHouseAnnotationNames.Projections)?.Value
            as List<ProjectionDefinition>;

        Assert.NotNull(projections);
        Assert.Single(projections);
        Assert.Equal("prj_by_customer", projections[0].Name);
        Assert.DoesNotContain(projections, p => p.Name == "prj_to_remove");
    }

    [Fact]
    public void HasProjection_DuplicateName_ThrowsInvalidOperationException()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
        {
            using var context = CreateContext<DuplicateProjectionContext>();
            // Force model building
            _ = context.Model;
        });

        Assert.Contains("already exists", ex.Message);
        Assert.Contains("duplicate_name", ex.Message);
    }

    #endregion

    #region DDL Generation Tests

    [Fact]
    public void MigrationsSqlGenerator_GeneratesAddProjection()
    {
        using var context = CreateContext<SortOrderProjectionContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var entityType = model.FindEntityType(typeof(PrjOrder));
        Assert.NotNull(entityType);

        var createTableOp = new CreateTableOperation
        {
            Name = "orders",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "OrderDate", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
                new AddColumnOperation { Name = "CustomerId", ClrType = typeof(string), ColumnType = "String" },
                new AddColumnOperation { Name = "Amount", ClrType = typeof(decimal), ColumnType = "Decimal(18,4)" }
            }
        };

        createTableOp.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        createTableOp.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "OrderDate", "Id" });

        var commands = generator.Generate(new[] { createTableOp }, model);
        var allSql = string.Join("\n", commands.Select(c => c.CommandText));

        // Check CREATE TABLE is generated
        Assert.Contains("CREATE TABLE", allSql);
        Assert.Contains("ENGINE = MergeTree()", allSql);

        // Check ADD PROJECTION is generated (auto-generated name)
        Assert.Contains("ALTER TABLE", allSql);
        Assert.Contains("ADD PROJECTION", allSql);
        Assert.Contains("\"orders__prj_ord__customer_id__order_date\"", allSql);
        Assert.Contains("ORDER BY", allSql);

        // Check MATERIALIZE PROJECTION is generated
        Assert.Contains("MATERIALIZE PROJECTION", allSql);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesDropProjection()
    {
        using var context = CreateContext<RawSqlProjectionContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var operation = new DropProjectionOperation
        {
            Table = "orders",
            Name = "prj_old",
            IfExists = true
        };

        var commands = generator.Generate(new[] { operation }, model);
        var sql = commands.First().CommandText;

        Assert.Contains("ALTER TABLE", sql);
        Assert.Contains("DROP PROJECTION", sql);
        Assert.Contains("IF EXISTS", sql);
        Assert.Contains("\"prj_old\"", sql);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesMaterializeProjection()
    {
        using var context = CreateContext<RawSqlProjectionContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var operation = new MaterializeProjectionOperation
        {
            Table = "orders",
            Name = "prj_by_region"
        };

        var commands = generator.Generate(new[] { operation }, model);
        var sql = commands.First().CommandText;

        Assert.Contains("ALTER TABLE", sql);
        Assert.Contains("MATERIALIZE PROJECTION", sql);
        Assert.Contains("\"prj_by_region\"", sql);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesMaterializeProjectionInPartition()
    {
        using var context = CreateContext<RawSqlProjectionContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var operation = new MaterializeProjectionOperation
        {
            Table = "orders",
            Name = "prj_by_region",
            InPartition = "202401"
        };

        var commands = generator.Generate(new[] { operation }, model);
        var sql = commands.First().CommandText;

        Assert.Contains("ALTER TABLE", sql);
        Assert.Contains("MATERIALIZE PROJECTION", sql);
        Assert.Contains("IN PARTITION", sql);
        Assert.Contains("202401", sql);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task CreateTable_WithProjection_ExecutesSuccessfully()
    {
        await using var context = CreateContext<SortOrderProjectionContext>();

        // Create source table with projection
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS orders (
                Id UUID,
                OrderDate DateTime64(3),
                CustomerId String,
                Region String,
                Amount Decimal(18, 4)
            ) ENGINE = MergeTree()
            ORDER BY (OrderDate, Id)
        ");

        // Add projection (ClickHouse projections don't support ASC/DESC)
        await context.Database.ExecuteSqlRawAsync(@"
            ALTER TABLE orders ADD PROJECTION IF NOT EXISTS prj_by_customer
            (SELECT * ORDER BY (""CustomerId"", ""OrderDate""))
        ");

        // Verify projection exists
        var result = await context.Database
            .SqlQueryRaw<ProjectionInfo>(
                "SELECT name, type FROM system.projections WHERE table = 'orders' AND database = currentDatabase()")
            .ToListAsync();

        Assert.Contains(result, p => p.name == "prj_by_customer");
    }

    [Fact]
    public async Task Projection_MaterializesExistingData()
    {
        await using var context = CreateContext<RawSqlProjectionContext>();

        // Create table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS orders_mat (
                Id UUID,
                OrderDate DateTime64(3),
                CustomerId String,
                Region String,
                Amount Decimal(18, 4)
            ) ENGINE = MergeTree()
            ORDER BY (OrderDate, Id)
        ");

        // Insert some data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO orders_mat (Id, OrderDate, CustomerId, Region, Amount) VALUES
            (generateUUIDv4(), now(), 'CUST001', 'US', 100.00),
            (generateUUIDv4(), now(), 'CUST002', 'EU', 200.00),
            (generateUUIDv4(), now(), 'CUST001', 'US', 150.00)
        ");

        // Add projection
        await context.Database.ExecuteSqlRawAsync(@"
            ALTER TABLE orders_mat ADD PROJECTION prj_by_cust
            (SELECT * ORDER BY (""CustomerId""))
        ");

        // Materialize the projection
        await context.Database.ExecuteSqlRawAsync(@"
            ALTER TABLE orders_mat MATERIALIZE PROJECTION prj_by_cust
        ");

        // Wait for materialization
        await Task.Delay(1000);

        // Verify projection exists and has parts
        var projections = await context.Database
            .SqlQueryRaw<ProjectionInfo>(
                "SELECT name, type FROM system.projections WHERE table = 'orders_mat' AND database = currentDatabase()")
            .ToListAsync();

        Assert.Contains(projections, p => p.name == "prj_by_cust");
    }

    [Fact]
    public async Task Projection_AggregationQuery_ExecutesSuccessfully()
    {
        await using var context = CreateContext<AggregationProjectionContext>();

        // Create table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS orders_agg (
                Id UUID,
                OrderDate Date,
                CustomerId String,
                Amount Decimal(18, 4)
            ) ENGINE = MergeTree()
            ORDER BY (OrderDate, Id)
        ");

        // Add aggregation projection
        await context.Database.ExecuteSqlRawAsync(@"
            ALTER TABLE orders_agg ADD PROJECTION prj_daily
            (
                SELECT OrderDate, sum(Amount) AS TotalAmount, count() AS OrderCount
                GROUP BY OrderDate
            )
        ");

        // Insert data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO orders_agg (Id, OrderDate, CustomerId, Amount) VALUES
            (generateUUIDv4(), '2024-01-15', 'CUST001', 100.00),
            (generateUUIDv4(), '2024-01-15', 'CUST002', 200.00),
            (generateUUIDv4(), '2024-01-16', 'CUST001', 150.00)
        ");

        // Materialize
        await context.Database.ExecuteSqlRawAsync(@"
            ALTER TABLE orders_agg MATERIALIZE PROJECTION prj_daily
        ");

        await Task.Delay(1000);

        // Query aggregated data
        var results = await context.Database
            .SqlQueryRaw<DailyTotalResult>(
                "SELECT OrderDate, sum(Amount) AS TotalAmount, count() AS OrderCount FROM orders_agg GROUP BY OrderDate ORDER BY OrderDate")
            .ToListAsync();

        Assert.Equal(2, results.Count);
        var jan15 = results.First(r => r.OrderDate == new DateTime(2024, 1, 15));
        Assert.Equal(300.00m, jan15.TotalAmount);
        Assert.Equal(2UL, jan15.OrderCount);
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

public class ProjectionInfo
{
    public string name { get; set; } = string.Empty;
    public string type { get; set; } = string.Empty;
}

public class DailyTotalResult
{
    public DateTime OrderDate { get; set; }
    public decimal TotalAmount { get; set; }
    public ulong OrderCount { get; set; }
}

#endregion

#region Test Entities

public class PrjOrder
{
    public Guid Id { get; set; }
    public DateTime OrderDate { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

#endregion

#region Test Contexts

public class RawSqlProjectionContext : DbContext
{
    public RawSqlProjectionContext(DbContextOptions<RawSqlProjectionContext> options)
        : base(options) { }

    public DbSet<PrjOrder> Orders => Set<PrjOrder>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PrjOrder>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("orders");
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });

            entity.HasProjection("prj_by_region",
                "SELECT * ORDER BY (\"Region\", \"OrderDate\")");
        });
    }
}

public class SortOrderProjectionContext : DbContext
{
    public SortOrderProjectionContext(DbContextOptions<SortOrderProjectionContext> options)
        : base(options) { }

    public DbSet<PrjOrder> Orders => Set<PrjOrder>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PrjOrder>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("orders");
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });

            // New fluent API - auto-generates name: orders__prj_ord__customer_id__order_date
            entity.HasProjection()
                .OrderBy(x => x.CustomerId)
                .ThenBy(x => x.OrderDate)
                .Build();
        });
    }
}

public class AggregationProjectionContext : DbContext
{
    public AggregationProjectionContext(DbContextOptions<AggregationProjectionContext> options)
        : base(options) { }

    public DbSet<PrjOrder> Orders => Set<PrjOrder>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PrjOrder>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("orders");
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });

            // New fluent API with anonymous type - auto-generates name: orders__prj_agg__date__total_amount__order_count
            entity.HasProjection()
                .GroupBy(o => o.OrderDate.Date)
                .Select(g => new
                {
                    Date = g.Key,
                    TotalAmount = g.Sum(o => o.Amount),
                    OrderCount = g.Count()
                })
                .Build();
        });
    }
}

public class MultipleProjectionContext : DbContext
{
    public MultipleProjectionContext(DbContextOptions<MultipleProjectionContext> options)
        : base(options) { }

    public DbSet<PrjOrder> Orders => Set<PrjOrder>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PrjOrder>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("orders");
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });

            // Sort-order projection with auto-generated name
            entity.HasProjection()
                .OrderBy(x => x.CustomerId)
                .Build();

            // Raw SQL projection (still uses string name)
            entity.HasProjection("prj_by_region",
                "SELECT * ORDER BY (\"Region\")");
        });
    }
}

public class RemoveProjectionContext : DbContext
{
    public RemoveProjectionContext(DbContextOptions<RemoveProjectionContext> options)
        : base(options) { }

    public DbSet<PrjOrder> Orders => Set<PrjOrder>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PrjOrder>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("orders");
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });

            entity.HasProjection("prj_by_customer")
                .OrderBy(x => x.CustomerId)
                .Build();

            entity.HasProjection("prj_to_remove",
                "SELECT * ORDER BY (\"Region\")");

            entity.RemoveProjection("prj_to_remove");
        });
    }
}

public class DuplicateProjectionContext : DbContext
{
    public DuplicateProjectionContext(DbContextOptions<DuplicateProjectionContext> options)
        : base(options) { }

    public DbSet<PrjOrder> Orders => Set<PrjOrder>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PrjOrder>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("orders");
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });

            // First projection with this name
            entity.HasProjection("duplicate_name",
                "SELECT * ORDER BY (\"Region\")");

            // Second projection with same name - should throw
            entity.HasProjection("duplicate_name",
                "SELECT * ORDER BY (\"CustomerId\")");
        });
    }
}

#endregion
