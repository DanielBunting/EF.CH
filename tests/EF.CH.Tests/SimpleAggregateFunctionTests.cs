using EF.CH.Extensions;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests;

public class SimpleAggregateFunctionTests : IAsyncLifetime
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

    #region Type Mapping Unit Tests

    [Fact]
    public void TypeMapping_GeneratesCorrectStoreType_Max()
    {
        var underlyingMapping = new ClickHouseFloat64TypeMapping();
        var mapping = new ClickHouseSimpleAggregateFunctionTypeMapping("max", underlyingMapping);

        Assert.Equal("SimpleAggregateFunction(max, Float64)", mapping.StoreType);
        Assert.Equal(typeof(double), mapping.ClrType);
        Assert.Equal("max", mapping.FunctionName);
    }

    [Fact]
    public void TypeMapping_GeneratesCorrectStoreType_Sum()
    {
        var underlyingMapping = new ClickHouseInt64TypeMapping();
        var mapping = new ClickHouseSimpleAggregateFunctionTypeMapping("sum", underlyingMapping);

        Assert.Equal("SimpleAggregateFunction(sum, Int64)", mapping.StoreType);
        Assert.Equal(typeof(long), mapping.ClrType);
    }

    [Fact]
    public void TypeMapping_GeneratesCorrectStoreType_Min()
    {
        var underlyingMapping = new ClickHouseInt32TypeMapping();
        var mapping = new ClickHouseSimpleAggregateFunctionTypeMapping("min", underlyingMapping);

        Assert.Equal("SimpleAggregateFunction(min, Int32)", mapping.StoreType);
        Assert.Equal(typeof(int), mapping.ClrType);
    }

    [Fact]
    public void TypeMapping_GeneratesCorrectLiteral()
    {
        var underlyingMapping = new ClickHouseFloat64TypeMapping();
        var mapping = new ClickHouseSimpleAggregateFunctionTypeMapping("max", underlyingMapping);

        var literal = mapping.GenerateSqlLiteral(123.45);

        Assert.Equal("123.45", literal);
    }

    [Fact]
    public void TypeMapping_GeneratesCorrectLiteral_Integer()
    {
        var underlyingMapping = new ClickHouseInt64TypeMapping();
        var mapping = new ClickHouseSimpleAggregateFunctionTypeMapping("sum", underlyingMapping);

        var literal = mapping.GenerateSqlLiteral(42L);

        Assert.Equal("42", literal);
    }

    #endregion

    #region Fluent API Unit Tests

    [Fact]
    public void FluentApi_SetsCorrectColumnType_Max()
    {
        var options = new DbContextOptionsBuilder<MaxTestContext>()
            .UseClickHouse("Host=localhost")
            .Options;

        using var context = new MaxTestContext(options);

        var property = context.Model.FindEntityType(typeof(DailyStats))!
            .FindProperty(nameof(DailyStats.MaxOrderValue))!;

        Assert.Equal("SimpleAggregateFunction(max, Float64)", property.GetColumnType());
    }

    [Fact]
    public void FluentApi_SetsCorrectColumnType_Sum()
    {
        var options = new DbContextOptionsBuilder<SumTestContext>()
            .UseClickHouse("Host=localhost")
            .Options;

        using var context = new SumTestContext(options);

        var property = context.Model.FindEntityType(typeof(DailyStats))!
            .FindProperty(nameof(DailyStats.TotalQuantity))!;

        Assert.Equal("SimpleAggregateFunction(sum, Int64)", property.GetColumnType());
    }

    [Fact]
    public void FluentApi_ThrowsForUnsupportedFunction()
    {
        // Test the extension method directly - it should throw immediately when called
        var options = new DbContextOptionsBuilder<UnsupportedFunctionTestContext>()
            .UseClickHouse("Host=localhost")
            .Options;

        Assert.Throws<ArgumentException>(() =>
        {
            using var context = new UnsupportedFunctionTestContext(options);
            // Access the model to trigger OnModelCreating and the exception
            _ = context.Model;
        });
    }

    [Fact]
    public void FluentApi_SupportsCaseInsensitiveFunctionNames()
    {
        var options = new DbContextOptionsBuilder<CaseInsensitiveTestContext>()
            .UseClickHouse("Host=localhost")
            .Options;

        using var context = new CaseInsensitiveTestContext(options);

        var property = context.Model.FindEntityType(typeof(DailyStats))!
            .FindProperty(nameof(DailyStats.MaxOrderValue))!;

        // Function name case is preserved as provided (MAX stays uppercase)
        var columnType = property.GetColumnType();
        Assert.StartsWith("SimpleAggregateFunction(", columnType);
        Assert.Contains("Float64", columnType);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task CanCreateTableWithSimpleAggregateFunction()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "DailyStats" (
                "Date" Date,
                "MaxOrderValue" SimpleAggregateFunction(max, Float64),
                "TotalQuantity" SimpleAggregateFunction(sum, Int64),
                "MinPrice" SimpleAggregateFunction(min, Float64)
            )
            ENGINE = AggregatingMergeTree()
            ORDER BY "Date"
            """);

        // Verify table exists by inserting data
        var today = DateOnly.FromDateTime(DateTime.Today);
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""DailyStats"" (""Date"", ""MaxOrderValue"", ""TotalQuantity"", ""MinPrice"")
              VALUES ('" + today.ToString("yyyy-MM-dd") + @"', 150.0, 10, 25.0)");

        var count = await context.Database.SqlQueryRaw<ulong>(
            @"SELECT count() AS ""Value"" FROM ""DailyStats"""
        ).FirstOrDefaultAsync();

        Assert.Equal(1UL, count);
    }

    [Fact]
    public async Task CanInsertAndQueryMaxFunction()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "MaxTest" (
                "Date" Date,
                "MaxValue" SimpleAggregateFunction(max, Float64)
            )
            ENGINE = AggregatingMergeTree()
            ORDER BY "Date"
            """);

        var today = DateOnly.FromDateTime(DateTime.Today);

        // Insert multiple values - AggregatingMergeTree will keep the max
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""MaxTest"" (""Date"", ""MaxValue"")
              VALUES ('" + today.ToString("yyyy-MM-dd") + @"', 100.0)");
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""MaxTest"" (""Date"", ""MaxValue"")
              VALUES ('" + today.ToString("yyyy-MM-dd") + @"', 200.0)");
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""MaxTest"" (""Date"", ""MaxValue"")
              VALUES ('" + today.ToString("yyyy-MM-dd") + @"', 150.0)");

        // Query max value - use max() aggregate to get final result
        var maxValue = await context.Database.SqlQueryRaw<double>(
            @"SELECT max(""MaxValue"") AS ""Value"" FROM ""MaxTest"" WHERE ""Date"" = '" + today.ToString("yyyy-MM-dd") + @"'"
        ).FirstOrDefaultAsync();

        Assert.Equal(200.0, maxValue);
    }

    [Fact]
    public async Task CanInsertAndQuerySumFunction()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "SumTest" (
                "Date" Date,
                "TotalAmount" SimpleAggregateFunction(sum, Int64)
            )
            ENGINE = AggregatingMergeTree()
            ORDER BY "Date"
            """);

        var today = DateOnly.FromDateTime(DateTime.Today);

        // Insert multiple values - AggregatingMergeTree will sum them
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""SumTest"" (""Date"", ""TotalAmount"")
              VALUES ('" + today.ToString("yyyy-MM-dd") + @"', 10)");
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""SumTest"" (""Date"", ""TotalAmount"")
              VALUES ('" + today.ToString("yyyy-MM-dd") + @"', 20)");
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""SumTest"" (""Date"", ""TotalAmount"")
              VALUES ('" + today.ToString("yyyy-MM-dd") + @"', 30)");

        // Query sum value
        var sumValue = await context.Database.SqlQueryRaw<long>(
            @"SELECT sum(""TotalAmount"") AS ""Value"" FROM ""SumTest"" WHERE ""Date"" = '" + today.ToString("yyyy-MM-dd") + @"'"
        ).FirstOrDefaultAsync();

        Assert.Equal(60L, sumValue);
    }

    [Fact]
    public async Task CanFilterBySimpleAggregateFunctionColumn()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "FilterTest" (
                "Id" Int32,
                "MaxScore" SimpleAggregateFunction(max, Float64)
            )
            ENGINE = AggregatingMergeTree()
            ORDER BY "Id"
            """);

        // Insert test data
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""FilterTest"" (""Id"", ""MaxScore"") VALUES (1, 75.0)");
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""FilterTest"" (""Id"", ""MaxScore"") VALUES (2, 85.0)");
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""FilterTest"" (""Id"", ""MaxScore"") VALUES (3, 95.0)");

        // Filter by SimpleAggregateFunction column
        var highScores = await context.Database.SqlQueryRaw<ulong>(
            @"SELECT count() AS ""Value"" FROM ""FilterTest"" WHERE ""MaxScore"" > 80"
        ).FirstOrDefaultAsync();

        Assert.Equal(2UL, highScores);
    }

    [Fact]
    public async Task CanOrderBySimpleAggregateFunctionColumn()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "OrderByTest" (
                "Name" String,
                "TotalSales" SimpleAggregateFunction(sum, Int64)
            )
            ENGINE = AggregatingMergeTree()
            ORDER BY "Name"
            """);

        // Insert test data
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""OrderByTest"" (""Name"", ""TotalSales"") VALUES ('Alice', 100)");
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""OrderByTest"" (""Name"", ""TotalSales"") VALUES ('Bob', 300)");
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""OrderByTest"" (""Name"", ""TotalSales"") VALUES ('Charlie', 200)");

        // Order by SimpleAggregateFunction column descending
        var topSeller = await context.Database.SqlQueryRaw<string>(
            @"SELECT ""Name"" AS ""Value"" FROM ""OrderByTest"" ORDER BY ""TotalSales"" DESC LIMIT 1"
        ).FirstOrDefaultAsync();

        Assert.Equal("Bob", topSeller);
    }

    #endregion

    private SimpleAggregateFunctionTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<SimpleAggregateFunctionTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new SimpleAggregateFunctionTestContext(options);
    }
}

#region Test Entities and Context

public class DailyStats
{
    public DateOnly Date { get; set; }
    public double MaxOrderValue { get; set; }
    public long TotalQuantity { get; set; }
    public double MinPrice { get; set; }
}

public class SimpleAggregateTestDbContext : DbContext
{
    private readonly Action<ModelBuilder>? _modelBuilderAction;

    public SimpleAggregateTestDbContext(DbContextOptions options, Action<ModelBuilder>? modelBuilderAction = null)
        : base(options)
    {
        _modelBuilderAction = modelBuilderAction;
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        _modelBuilderAction?.Invoke(modelBuilder);
    }
}

public class SimpleAggregateFunctionTestContext : DbContext
{
    public SimpleAggregateFunctionTestContext(DbContextOptions<SimpleAggregateFunctionTestContext> options)
        : base(options) { }
}

/// <summary>
/// Context specifically for testing unsupported function validation.
/// </summary>
public class UnsupportedFunctionTestContext : DbContext
{
    public UnsupportedFunctionTestContext(DbContextOptions<UnsupportedFunctionTestContext> options)
        : base(options) { }

    public DbSet<DailyStats> DailyStats { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DailyStats>(entity =>
        {
            entity.HasKey(e => e.Date);
            entity.Property(e => e.MaxOrderValue)
                .HasSimpleAggregateFunction("unsupported_func"); // This should throw
        });
    }
}

/// <summary>
/// Context for testing Sum SimpleAggregateFunction.
/// </summary>
public class SumTestContext : DbContext
{
    public SumTestContext(DbContextOptions<SumTestContext> options)
        : base(options) { }

    public DbSet<DailyStats> DailyStats { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DailyStats>(entity =>
        {
            entity.HasKey(e => e.Date);
            entity.Property(e => e.TotalQuantity)
                .HasSimpleAggregateFunction("sum");
        });
    }
}

/// <summary>
/// Context for testing Max SimpleAggregateFunction.
/// </summary>
public class MaxTestContext : DbContext
{
    public MaxTestContext(DbContextOptions<MaxTestContext> options)
        : base(options) { }

    public DbSet<DailyStats> DailyStats { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DailyStats>(entity =>
        {
            entity.HasKey(e => e.Date);
            entity.Property(e => e.MaxOrderValue)
                .HasSimpleAggregateFunction("max");
        });
    }
}

/// <summary>
/// Context for testing case-insensitive function names.
/// </summary>
public class CaseInsensitiveTestContext : DbContext
{
    public CaseInsensitiveTestContext(DbContextOptions<CaseInsensitiveTestContext> options)
        : base(options) { }

    public DbSet<DailyStats> DailyStats { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DailyStats>(entity =>
        {
            entity.HasKey(e => e.Date);
            entity.Property(e => e.MaxOrderValue)
                .HasSimpleAggregateFunction("MAX"); // uppercase - should work
        });
    }
}

#endregion
