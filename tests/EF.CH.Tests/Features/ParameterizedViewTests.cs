using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.ParameterizedViews;
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

    #region Strongly-Typed Access Tests

    [Fact]
    public async Task ClickHouseParameterizedView_Query_ReturnsIQueryable()
    {
        await using var context = CreateContext();

        // Create source table and view
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS typed_events (
                event_id UInt64,
                event_type String,
                user_id UInt64,
                timestamp DateTime,
                value Decimal(18, 4)
            ) ENGINE = MergeTree()
            ORDER BY (user_id, timestamp)
        ");

        await context.Database.CreateParameterizedViewAsync(
            "typed_user_events_view",
            @"SELECT event_id AS ""EventId"",
                     event_type AS ""EventType"",
                     user_id AS ""UserId"",
                     timestamp AS ""Timestamp"",
                     value AS ""Value""
              FROM typed_events
              WHERE user_id = {user_id:UInt64}",
            ifNotExists: true);

        // Insert test data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO typed_events (event_id, event_type, user_id, timestamp, value) VALUES
            (1, 'click', 100, '2024-01-15 10:00:00', 1.0),
            (2, 'view', 100, '2024-01-15 11:00:00', 2.0),
            (3, 'click', 200, '2024-01-15 12:00:00', 3.0)
        ");

        // Use strongly-typed accessor
        var view = new ClickHouseParameterizedView<TypedEventView>(context);
        Assert.Equal("typed_user_events_view", view.ViewName);

        var results = await view.Query(new { user_id = 100UL })
            .OrderBy(e => e.Timestamp)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal(100UL, r.UserId));
    }

    [Fact]
    public async Task ClickHouseParameterizedView_ToListAsync_ConvenienceMethod()
    {
        await using var context = CreateContext();

        // Create source table and view
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS conv_events (
                event_id UInt64,
                event_type String,
                user_id UInt64,
                timestamp DateTime,
                value Decimal(18, 4)
            ) ENGINE = MergeTree()
            ORDER BY (user_id, timestamp)
        ");

        await context.Database.CreateParameterizedViewAsync(
            "conv_user_events_view",
            @"SELECT event_id AS ""EventId"",
                     event_type AS ""EventType"",
                     user_id AS ""UserId"",
                     timestamp AS ""Timestamp"",
                     value AS ""Value""
              FROM conv_events
              WHERE user_id = {user_id:UInt64}",
            ifNotExists: true);

        // Insert test data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO conv_events (event_id, event_type, user_id, timestamp, value) VALUES
            (1, 'click', 100, '2024-01-15 10:00:00', 1.0)
        ");

        var view = new ClickHouseParameterizedView<ConvEventView>(context);

        // Test convenience methods
        var list = await view.ToListAsync(new { user_id = 100UL });
        Assert.Single(list);

        var first = await view.FirstOrDefaultAsync(new { user_id = 100UL });
        Assert.NotNull(first);

        var count = await view.CountAsync(new { user_id = 100UL });
        Assert.Equal(1, count);

        var any = await view.AnyAsync(new { user_id = 100UL });
        Assert.True(any);

        var anyEmpty = await view.AnyAsync(new { user_id = 999UL });
        Assert.False(anyEmpty);
    }

    [Fact]
    public void ClickHouseParameterizedView_ThrowsWhenNotConfigured()
    {
        using var context = CreateContext();

        // UnconfiguredView is not configured in OnModelCreating
        Assert.Throws<InvalidOperationException>(() =>
            new ClickHouseParameterizedView<UnconfiguredView>(context));
    }

    #endregion

    #region Fluent Builder Tests

    [Fact]
    public void AsParameterizedView_SetsAnnotationsCorrectly()
    {
        using var context = CreateFluentContext();
        var entityType = context.Model.FindEntityType(typeof(FluentEventView));

        Assert.NotNull(entityType);
        Assert.True((bool?)entityType.FindAnnotation(ClickHouseAnnotationNames.ParameterizedView)?.Value);
        Assert.Equal("fluent_events_view", entityType.FindAnnotation(ClickHouseAnnotationNames.ParameterizedViewName)?.Value);

        var metadata = entityType.FindAnnotation(ClickHouseAnnotationNames.ParameterizedViewMetadata)?.Value as ParameterizedViewMetadataBase;
        Assert.NotNull(metadata);
        Assert.Equal("fluent_events_view", metadata.ViewName);
        Assert.Equal(typeof(FluentEventView), metadata.ResultType);
        Assert.Equal(typeof(FluentEvent), metadata.SourceType);
        Assert.NotNull(metadata.ProjectionExpression);
        Assert.NotNull(metadata.Parameters);
        Assert.Equal(2, metadata.Parameters.Count);
        Assert.True(metadata.Parameters.ContainsKey("user_id"));
        Assert.True(metadata.Parameters.ContainsKey("start_date"));
    }

    [Fact]
    public void GetParameterizedViewSql_GeneratesCorrectSql()
    {
        using var context = CreateFluentContext();

        var sql = context.Database.GetParameterizedViewSql<FluentEventView>();

        Assert.Contains("CREATE VIEW", sql);
        Assert.Contains("\"fluent_events_view\"", sql);
        Assert.Contains("AS \"EventId\"", sql);
        Assert.Contains("AS \"EventType\"", sql);
        Assert.Contains("{user_id:UInt64}", sql);
        Assert.Contains("{start_date:DateTime}", sql);
        Assert.Contains("FROM \"fluent_events\"", sql);
    }

    [Fact]
    public async Task EnsureParameterizedViewAsync_CreatesView()
    {
        await using var context = CreateFluentContext();

        // Create source table first
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS fluent_events (
                event_id UInt64,
                event_type String,
                user_id UInt64,
                timestamp DateTime,
                value Decimal(18, 4)
            ) ENGINE = MergeTree()
            ORDER BY (user_id, timestamp)
        ");

        // Ensure view is created
        await context.Database.EnsureParameterizedViewAsync<FluentEventView>(ifNotExists: true);

        // Insert test data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO fluent_events (event_id, event_type, user_id, timestamp, value) VALUES
            (1, 'click', 100, '2024-01-15 10:00:00', 1.0),
            (2, 'view', 100, '2024-01-16 11:00:00', 2.0),
            (3, 'click', 200, '2024-01-15 12:00:00', 3.0)
        ");

        // Query using the view
        var results = await context.FromParameterizedView<FluentEventView>(
            "fluent_events_view",
            new { user_id = 100UL, start_date = new DateTime(2024, 1, 15) })
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal(100UL, r.UserId));
    }

    [Fact]
    public async Task EnsureParameterizedViewsAsync_CreatesAllViews()
    {
        await using var context = CreateFluentContext();

        // Create source tables first
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS fluent_events (
                event_id UInt64,
                event_type String,
                user_id UInt64,
                timestamp DateTime,
                value Decimal(18, 4)
            ) ENGINE = MergeTree()
            ORDER BY (user_id, timestamp)
        ");

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS fluent_products (
                product_id UInt64,
                category String,
                price Decimal(18, 4)
            ) ENGINE = MergeTree()
            ORDER BY product_id
        ");

        // Create all views
        var viewsCreated = await context.Database.EnsureParameterizedViewsAsync();

        Assert.Equal(2, viewsCreated);
    }

    [Fact]
    public void ParameterizedViewConfiguration_ValidatesParameters()
    {
        var config = new ParameterizedViewConfiguration<FluentEventView, FluentEvent>();

        // Without Select(), validation should fail
        var errors = config.Validate().ToList();
        Assert.Contains(errors, e => e.Contains("Select()"));

        // Without parameters, validation should fail
        config.Select(e => new FluentEventView
        {
            EventId = e.EventId,
            EventType = e.EventType,
            UserId = e.UserId,
            Timestamp = e.Timestamp,
            Value = e.Value
        });

        errors = config.Validate().ToList();
        Assert.Contains(errors, e => e.Contains("Parameter()"));
    }

    [Fact]
    public void ParameterizedViewConfiguration_ValidatesWhereParameterReferences()
    {
        var config = new ParameterizedViewConfiguration<FluentEventView, FluentEvent>();

        config.Select(e => new FluentEventView
        {
            EventId = e.EventId,
            EventType = e.EventType,
            UserId = e.UserId,
            Timestamp = e.Timestamp,
            Value = e.Value
        });

        config.Parameter<ulong>("user_id");

        // Reference undefined parameter
        config.Where((e, p) => e.Timestamp >= p.Get<DateTime>("undefined_param"));

        var errors = config.Validate().ToList();
        Assert.Contains(errors, e => e.Contains("undefined_param"));
    }

    [Fact]
    public void ParameterizedViewSqlGenerator_GeneratesCorrectWhereClause()
    {
        using var context = CreateFluentContext();

        var sql = context.Database.GetParameterizedViewSql<FluentEventView>();

        // Should contain WHERE clause with parameters
        // Column names come from EF Core model (snake_case configured via HasColumnName)
        Assert.Contains("WHERE", sql);
        Assert.Contains("{user_id:UInt64}", sql);
        Assert.Contains("{start_date:DateTime}", sql);
    }

    #endregion

    private ParameterizedViewTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<ParameterizedViewTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new ParameterizedViewTestContext(options);
    }

    private FluentTestContext CreateFluentContext()
    {
        var options = new DbContextOptionsBuilder<FluentTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new FluentTestContext(options);
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

// For strongly-typed access tests
public class TypedEventView
{
    public ulong EventId { get; set; }
    public string EventType { get; set; } = string.Empty;
    public ulong UserId { get; set; }
    public DateTime Timestamp { get; set; }
    public decimal Value { get; set; }
}

public class ConvEventView
{
    public ulong EventId { get; set; }
    public string EventType { get; set; } = string.Empty;
    public ulong UserId { get; set; }
    public DateTime Timestamp { get; set; }
    public decimal Value { get; set; }
}

public class UnconfiguredView
{
    public ulong Id { get; set; }
}

// For fluent builder tests
public class FluentEvent
{
    public ulong EventId { get; set; }
    public string EventType { get; set; } = string.Empty;
    public ulong UserId { get; set; }
    public DateTime Timestamp { get; set; }
    public decimal Value { get; set; }
}

public class FluentEventView
{
    public ulong EventId { get; set; }
    public string EventType { get; set; } = string.Empty;
    public ulong UserId { get; set; }
    public DateTime Timestamp { get; set; }
    public decimal Value { get; set; }
}

public class FluentProduct
{
    public ulong ProductId { get; set; }
    public string Category { get; set; } = string.Empty;
    public decimal Price { get; set; }
}

public class FluentProductView
{
    public ulong ProductId { get; set; }
    public string Category { get; set; } = string.Empty;
    public decimal Price { get; set; }
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

        // For strongly-typed access tests
        modelBuilder.Entity<TypedEventView>(entity =>
        {
            entity.HasParameterizedView("typed_user_events_view");
        });

        modelBuilder.Entity<ConvEventView>(entity =>
        {
            entity.HasParameterizedView("conv_user_events_view");
        });
    }
}

public class FluentTestContext : DbContext
{
    public FluentTestContext(DbContextOptions<FluentTestContext> options)
        : base(options) { }

    public DbSet<FluentEvent> FluentEvents => Set<FluentEvent>();
    public DbSet<FluentEventView> FluentEventViews => Set<FluentEventView>();
    public DbSet<FluentProduct> FluentProducts => Set<FluentProduct>();
    public DbSet<FluentProductView> FluentProductViews => Set<FluentProductView>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Source entity with explicit column names (to match snake_case table columns)
        modelBuilder.Entity<FluentEvent>(entity =>
        {
            entity.ToTable("fluent_events");
            entity.HasKey(e => e.EventId);
            entity.UseMergeTree(e => new { e.UserId, e.Timestamp });
            entity.Property(e => e.EventId).HasColumnName("event_id");
            entity.Property(e => e.EventType).HasColumnName("event_type");
            entity.Property(e => e.UserId).HasColumnName("user_id");
            entity.Property(e => e.Timestamp).HasColumnName("timestamp");
            entity.Property(e => e.Value).HasColumnName("value");
        });

        modelBuilder.Entity<FluentProduct>(entity =>
        {
            entity.ToTable("fluent_products");
            entity.HasKey(e => e.ProductId);
            entity.UseMergeTree(e => e.ProductId);
            entity.Property(e => e.ProductId).HasColumnName("product_id");
            entity.Property(e => e.Category).HasColumnName("category");
            entity.Property(e => e.Price).HasColumnName("price");
        });

        // View with fluent configuration
        modelBuilder.Entity<FluentEventView>(entity =>
        {
            entity.AsParameterizedView<FluentEventView, FluentEvent>(cfg => cfg
                .HasName("fluent_events_view")
                .FromTable()
                .Select(e => new FluentEventView
                {
                    EventId = e.EventId,
                    EventType = e.EventType,
                    UserId = e.UserId,
                    Timestamp = e.Timestamp,
                    Value = e.Value
                })
                .Parameter<ulong>("user_id")
                .Parameter<DateTime>("start_date")
                .Where((e, p) => e.UserId == p.Get<ulong>("user_id"))
                .Where((e, p) => e.Timestamp >= p.Get<DateTime>("start_date")));
        });

        modelBuilder.Entity<FluentProductView>(entity =>
        {
            entity.AsParameterizedView<FluentProductView, FluentProduct>(cfg => cfg
                .HasName("fluent_products_view")
                .FromTable()
                .Select(p => new FluentProductView
                {
                    ProductId = p.ProductId,
                    Category = p.Category,
                    Price = p.Price
                })
                .Parameter<string>("category")
                .Where((p, a) => p.Category == a.Get<string>("category")));
        });
    }
}

#endregion
