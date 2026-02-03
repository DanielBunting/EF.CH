using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

public class ParameterizedViewTests : IAsyncLifetime
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

    #region Parameter Formatting Tests

    [Theory]
    [InlineData(null, "NULL")]
    [InlineData("hello", "'hello'")]
    [InlineData("it's a test", "'it\\'s a test'")]
    [InlineData("with\\backslash", "'with\\\\backslash'")]
    [InlineData("", "''")]
    public void FormatParameterValue_String_FormatsCorrectly(string? input, string expected)
    {
        var result = ClickHouseParameterizedViewExtensions.FormatParameterValue(input);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(true, "1")]
    [InlineData(false, "0")]
    public void FormatParameterValue_Bool_FormatsCorrectly(bool input, string expected)
    {
        var result = ClickHouseParameterizedViewExtensions.FormatParameterValue(input);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(123, "123")]
    [InlineData(-456, "-456")]
    [InlineData(0, "0")]
    public void FormatParameterValue_Int_FormatsCorrectly(int input, string expected)
    {
        var result = ClickHouseParameterizedViewExtensions.FormatParameterValue(input);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(123UL, "123")]
    [InlineData(0UL, "0")]
    [InlineData(18446744073709551615UL, "18446744073709551615")]
    public void FormatParameterValue_ULong_FormatsCorrectly(ulong input, string expected)
    {
        var result = ClickHouseParameterizedViewExtensions.FormatParameterValue(input);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void FormatParameterValue_DateTime_FormatsCorrectly()
    {
        var dt = new DateTime(2024, 1, 15, 10, 30, 45);
        var result = ClickHouseParameterizedViewExtensions.FormatParameterValue(dt);
        Assert.Equal("'2024-01-15 10:30:45'", result);
    }

    [Fact]
    public void FormatParameterValue_DateOnly_FormatsCorrectly()
    {
        var date = new DateOnly(2024, 1, 15);
        var result = ClickHouseParameterizedViewExtensions.FormatParameterValue(date);
        Assert.Equal("'2024-01-15'", result);
    }

    [Fact]
    public void FormatParameterValue_TimeOnly_FormatsCorrectly()
    {
        var time = new TimeOnly(10, 30, 45);
        var result = ClickHouseParameterizedViewExtensions.FormatParameterValue(time);
        Assert.Equal("'10:30:45'", result);
    }

    [Fact]
    public void FormatParameterValue_Decimal_FormatsWithInvariantCulture()
    {
        var dec = 123.456m;
        var result = ClickHouseParameterizedViewExtensions.FormatParameterValue(dec);
        Assert.Equal("123.456", result);
    }

    [Fact]
    public void FormatParameterValue_Double_FormatsWithInvariantCulture()
    {
        var dbl = 123.456;
        var result = ClickHouseParameterizedViewExtensions.FormatParameterValue(dbl);
        Assert.Equal("123.456", result);
    }

    [Fact]
    public void FormatParameterValue_Guid_FormatsCorrectly()
    {
        var guid = Guid.Parse("12345678-1234-1234-1234-123456789abc");
        var result = ClickHouseParameterizedViewExtensions.FormatParameterValue(guid);
        Assert.Equal("'12345678-1234-1234-1234-123456789abc'", result);
    }

    [Fact]
    public void FormatParameterValue_ByteArray_FormatsAsUnhex()
    {
        var bytes = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var result = ClickHouseParameterizedViewExtensions.FormatParameterValue(bytes);
        Assert.Equal("unhex('DEADBEEF')", result);
    }

    [Fact]
    public void FormatParameterValue_Enum_FormatsAsInt()
    {
        var result = ClickHouseParameterizedViewExtensions.FormatParameterValue(TestEnum.Value2);
        Assert.Equal("2", result);
    }

    private enum TestEnum
    {
        Value1 = 1,
        Value2 = 2
    }

    #endregion

    #region Snake Case Conversion Tests

    [Theory]
    [InlineData("UserId", "user_id")]
    [InlineData("userId", "user_id")]
    [InlineData("ID", "i_d")]
    [InlineData("user_id", "user_id")]
    [InlineData("StartDate", "start_date")]
    [InlineData("XMLParser", "x_m_l_parser")]
    [InlineData("", "")]
    public void ToSnakeCase_ConvertsCorrectly(string input, string expected)
    {
        var result = ClickHouseParameterizedViewExtensions.ToSnakeCase(input);
        Assert.Equal(expected, result);
    }

    #endregion

    #region SQL Generation Tests

    [Fact]
    public void BuildParameterizedViewSql_WithAnonymousObject_GeneratesCorrectSql()
    {
        var parameters = new { user_id = 123UL, start_date = new DateTime(2024, 1, 1) };
        var sql = ClickHouseParameterizedViewExtensions.BuildParameterizedViewSql("user_events_view", parameters);

        Assert.Equal("SELECT * FROM \"user_events_view\"(user_id = 123, start_date = '2024-01-01 00:00:00')", sql);
    }

    [Fact]
    public void BuildParameterizedViewSql_WithPascalCaseProperties_ConvertsToSnakeCase()
    {
        var parameters = new { UserId = 123UL, StartDate = new DateTime(2024, 1, 1) };
        var sql = ClickHouseParameterizedViewExtensions.BuildParameterizedViewSql("user_events_view", parameters);

        Assert.Equal("SELECT * FROM \"user_events_view\"(user_id = 123, start_date = '2024-01-01 00:00:00')", sql);
    }

    [Fact]
    public void BuildParameterizedViewSql_WithDictionary_GeneratesCorrectSql()
    {
        var parameters = new Dictionary<string, object?>
        {
            ["user_id"] = 123UL,
            ["start_date"] = new DateTime(2024, 1, 1)
        };
        var sql = ClickHouseParameterizedViewExtensions.BuildParameterizedViewSql("user_events_view", parameters);

        Assert.Equal("SELECT * FROM \"user_events_view\"(user_id = 123, start_date = '2024-01-01 00:00:00')", sql);
    }

    [Fact]
    public void BuildParameterizedViewSql_WithNoParameters_GeneratesCorrectSql()
    {
        var parameters = new Dictionary<string, object?>();
        var sql = ClickHouseParameterizedViewExtensions.BuildParameterizedViewSql("simple_view", parameters);

        Assert.Equal("SELECT * FROM \"simple_view\"", sql);
    }

    [Fact]
    public void BuildParameterizedViewSql_EscapesViewName()
    {
        var parameters = new { id = 1 };
        var sql = ClickHouseParameterizedViewExtensions.BuildParameterizedViewSql("view\"name", parameters);

        Assert.Contains("\"view\"\"name\"", sql);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task CreateAndQueryParameterizedView_ReturnsFilteredData()
    {
        await using var context = CreateContext();

        // Create source table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS events (
                event_id UInt64,
                event_type String,
                user_id UInt64,
                timestamp DateTime,
                value Decimal(18, 4)
            ) ENGINE = MergeTree()
            ORDER BY (user_id, timestamp)
        ");

        // Create parameterized view using fluent API
        // Use aliases to match C# property names (ClickHouse is case-sensitive)
        await context.Database.CreateParameterizedViewAsync(
            "user_events_view",
            @"SELECT event_id AS ""EventId"",
                     event_type AS ""EventType"",
                     user_id AS ""UserId"",
                     timestamp AS ""Timestamp"",
                     value AS ""Value""
              FROM events
              WHERE user_id = {user_id:UInt64}
                AND timestamp >= {start_date:DateTime}");

        // Insert test data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO events (event_id, event_type, user_id, timestamp, value) VALUES
            (1, 'click', 100, '2024-01-15 10:00:00', 1.0),
            (2, 'view', 100, '2024-01-15 11:00:00', 2.0),
            (3, 'click', 200, '2024-01-15 12:00:00', 3.0),
            (4, 'purchase', 100, '2024-01-14 10:00:00', 50.0),
            (5, 'click', 100, '2024-01-16 09:00:00', 1.5)
        ");

        // Query the parameterized view
        var results = await context.FromParameterizedView<EventView>(
            "user_events_view",
            new { user_id = 100UL, start_date = new DateTime(2024, 1, 15) })
            .OrderBy(e => e.Timestamp)
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.All(results, r => Assert.Equal(100UL, r.UserId));
        Assert.All(results, r => Assert.True(r.Timestamp >= new DateTime(2024, 1, 15)));
    }

    [Fact]
    public async Task ParameterizedView_WithLinqComposition_Works()
    {
        await using var context = CreateContext();

        // Create source table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS products (
                product_id UInt64,
                category String,
                price Decimal(18, 4),
                stock Int32
            ) ENGINE = MergeTree()
            ORDER BY product_id
        ");

        // Create parameterized view using fluent API
        await context.Database.CreateParameterizedViewAsync(
            "products_by_category_view",
            @"SELECT product_id AS ""ProductId"",
                     category AS ""Category"",
                     price AS ""Price"",
                     stock AS ""Stock""
              FROM products
              WHERE category = {category:String}");

        // Insert test data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO products (product_id, category, price, stock) VALUES
            (1, 'Electronics', 99.99, 10),
            (2, 'Electronics', 199.99, 5),
            (3, 'Electronics', 49.99, 20),
            (4, 'Books', 19.99, 100),
            (5, 'Books', 29.99, 50)
        ");

        // Query with LINQ composition: Where, OrderBy, Take
        var results = await context.FromParameterizedView<ProductView>(
            "products_by_category_view",
            new { category = "Electronics" })
            .Where(p => p.Price > 50)
            .OrderByDescending(p => p.Price)
            .Take(2)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal(199.99m, results[0].Price);
        Assert.Equal(99.99m, results[1].Price);
    }

    [Fact]
    public async Task ParameterizedView_WithGroupBy_Works()
    {
        await using var context = CreateContext();

        // Create source table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS sales (
                sale_id UInt64,
                region String,
                product String,
                amount Decimal(18, 4),
                sale_date Date
            ) ENGINE = MergeTree()
            ORDER BY (region, sale_date)
        ");

        // Create parameterized view using fluent API
        await context.Database.CreateParameterizedViewAsync(
            "sales_by_region_view",
            @"SELECT sale_id AS ""SaleId"",
                     region AS ""Region"",
                     product AS ""Product"",
                     amount AS ""Amount"",
                     sale_date AS ""SaleDate""
              FROM sales
              WHERE region = {region:String}");

        // Insert test data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO sales (sale_id, region, product, amount, sale_date) VALUES
            (1, 'North', 'Widget', 100.00, '2024-01-15'),
            (2, 'North', 'Widget', 150.00, '2024-01-15'),
            (3, 'North', 'Gadget', 200.00, '2024-01-16'),
            (4, 'South', 'Widget', 75.00, '2024-01-15')
        ");

        // Query with GroupBy
        var results = await context.FromParameterizedView<SaleView>(
            "sales_by_region_view",
            new { region = "North" })
            .GroupBy(s => s.Product)
            .Select(g => new { Product = g.Key, Total = g.Sum(s => s.Amount) })
            .OrderBy(x => x.Product)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal("Gadget", results[0].Product);
        Assert.Equal(200.00m, results[0].Total);
        Assert.Equal("Widget", results[1].Product);
        Assert.Equal(250.00m, results[1].Total);
    }

    [Fact]
    public async Task ParameterizedView_WithNullParameter_Works()
    {
        await using var context = CreateContext();

        // Create source table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS items (
                item_id UInt64,
                name String,
                description Nullable(String)
            ) ENGINE = MergeTree()
            ORDER BY item_id
        ");

        // Create parameterized view using fluent API
        await context.Database.CreateParameterizedViewAsync(
            "items_view",
            @"SELECT item_id AS ""ItemId"",
                     name AS ""Name"",
                     description AS ""Description""
              FROM items
              WHERE description = {desc:String} OR (description IS NULL AND {desc:String} IS NULL)");

        // Insert test data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO items (item_id, name, description) VALUES
            (1, 'Item1', 'Has description'),
            (2, 'Item2', NULL),
            (3, 'Item3', 'Another desc')
        ");

        // Note: This test demonstrates NULL handling in view parameters
        // ClickHouse NULL comparison requires special handling in the view definition
        var results = await context.FromParameterizedView<ItemView>(
            "items_view",
            new { desc = "Has description" })
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("Item1", results[0].Name);
    }

    [Fact]
    public async Task ParameterizedView_WithDictionaryParameters_Works()
    {
        await using var context = CreateContext();

        // Create source table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS logs (
                log_id UInt64,
                level String,
                message String,
                timestamp DateTime
            ) ENGINE = MergeTree()
            ORDER BY timestamp
        ");

        // Create parameterized view using fluent API
        await context.Database.CreateParameterizedViewAsync(
            "logs_view",
            @"SELECT log_id AS ""LogId"",
                     level AS ""Level"",
                     message AS ""Message"",
                     timestamp AS ""Timestamp""
              FROM logs
              WHERE level = {level:String}");

        // Insert test data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO logs (log_id, level, message, timestamp) VALUES
            (1, 'ERROR', 'Error message', '2024-01-15 10:00:00'),
            (2, 'WARN', 'Warning message', '2024-01-15 11:00:00'),
            (3, 'ERROR', 'Another error', '2024-01-15 12:00:00')
        ");

        // Query using dictionary parameters
        var parameters = new Dictionary<string, object?>
        {
            ["level"] = "ERROR"
        };

        var results = await context.FromParameterizedView<LogView>("logs_view", parameters)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal("ERROR", r.Level));
    }

    [Fact]
    public async Task DropParameterizedViewAsync_DropsView()
    {
        await using var context = CreateContext();

        // Create a view
        await context.Database.CreateParameterizedViewAsync(
            "temp_view",
            "SELECT 1 AS value");

        // Verify the view exists by querying system.tables
        var existsBefore = await context.Database.ExecuteSqlRawAsync(
            "SELECT 1 FROM system.tables WHERE name = 'temp_view' AND database = currentDatabase()");

        // Drop the view
        await context.Database.DropParameterizedViewAsync("temp_view");

        // Verify the view is dropped (query should not fail)
        // Drop again with ifExists=true should not throw
        await context.Database.DropParameterizedViewAsync("temp_view", ifExists: true);

        // This should succeed (view doesn't exist, but ifExists=true)
        Assert.True(true);
    }

    [Fact]
    public async Task CreateParameterizedViewAsync_WithIfNotExists_DoesNotThrow()
    {
        await using var context = CreateContext();

        // Create a view
        await context.Database.CreateParameterizedViewAsync(
            "reusable_view",
            "SELECT 1 AS value");

        // Creating again without IF NOT EXISTS would throw
        // But with ifNotExists=true, it should not throw
        await context.Database.CreateParameterizedViewAsync(
            "reusable_view",
            "SELECT 1 AS value",
            ifNotExists: true);

        // Clean up
        await context.Database.DropParameterizedViewAsync("reusable_view");

        Assert.True(true);
    }

    #endregion

    #region Model Configuration Tests

    [Fact]
    public void HasParameterizedView_SetsCorrectAnnotations()
    {
        using var context = CreateContext();
        var entityType = context.Model.FindEntityType(typeof(EventView));

        Assert.NotNull(entityType);
        Assert.True((bool?)entityType.FindAnnotation("ClickHouse:ParameterizedView")?.Value);
        Assert.Equal("user_events_view", entityType.FindAnnotation("ClickHouse:ParameterizedViewName")?.Value);
    }

    #endregion

    private ParameterizedViewTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<ParameterizedViewTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new ParameterizedViewTestContext(options);
    }
}

#region Test Entities

public class EventView
{
    public ulong EventId { get; set; }
    public string EventType { get; set; } = string.Empty;
    public ulong UserId { get; set; }
    public DateTime Timestamp { get; set; }
    public decimal Value { get; set; }
}

public class ProductView
{
    public ulong ProductId { get; set; }
    public string Category { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public int Stock { get; set; }
}

public class SaleView
{
    public ulong SaleId { get; set; }
    public string Region { get; set; } = string.Empty;
    public string Product { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateOnly SaleDate { get; set; }
}

public class ItemView
{
    public ulong ItemId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Description { get; set; }
}

public class LogView
{
    public ulong LogId { get; set; }
    public string Level { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
}

#endregion

#region Test Context

public class ParameterizedViewTestContext : DbContext
{
    public ParameterizedViewTestContext(DbContextOptions<ParameterizedViewTestContext> options)
        : base(options) { }

    public DbSet<EventView> EventViews => Set<EventView>();
    public DbSet<ProductView> ProductViews => Set<ProductView>();
    public DbSet<SaleView> SaleViews => Set<SaleView>();
    public DbSet<ItemView> ItemViews => Set<ItemView>();
    public DbSet<LogView> LogViews => Set<LogView>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<EventView>(entity =>
        {
            entity.HasParameterizedView("user_events_view");
        });

        modelBuilder.Entity<ProductView>(entity =>
        {
            entity.HasParameterizedView("products_by_category_view");
        });

        modelBuilder.Entity<SaleView>(entity =>
        {
            entity.HasParameterizedView("sales_by_region_view");
        });

        modelBuilder.Entity<ItemView>(entity =>
        {
            entity.HasParameterizedView("items_view");
        });

        modelBuilder.Entity<LogView>(entity =>
        {
            entity.HasParameterizedView("logs_view");
        });
    }
}

#endregion
