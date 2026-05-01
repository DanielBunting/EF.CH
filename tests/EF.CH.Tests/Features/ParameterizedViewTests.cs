using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.ParameterizedViews;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Features;

// Live-ClickHouse integration coverage for parameterized views lives in
// tests/EF.CH.SystemTests/Schema/ParameterizedViewLifecycleTests.cs. This file covers
// parameter formatting, snake-case conversion, SQL generation, annotation, and
// configuration-validation — all metadata-only and free of a running container.
public class ParameterizedViewTests
{
    private const string StubConnectionString =
        "Host=localhost;Port=9000;Database=default;Username=default;Password=";

    private static string GetConnectionString() => StubConnectionString;

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

    // WithParameterAsync used to use a divergent SQL-standard `''` escape that silently
    // returned wrong rows when a value contained a quote or backslash. Pin that the
    // call-site formatter now matches FormatParameterValue (`\\` + `\'` convention).
    [Theory]
    [InlineData("normal", "level='normal'")]
    [InlineData("it's", @"level='it\'s'")]
    [InlineData(@"a\b", @"level='a\\b'")]
    [InlineData("has,comma", "level='has,comma'")]
    public void BuildParameterArgs_String_UsesBackslashEscape(string input, string expected)
    {
        var args = ClickHouseQueryableParameterExtensions.BuildParameterArgs(
            new Dictionary<string, object?> { ["level"] = input });
        Assert.Equal(expected, args);
    }

    [Fact]
    public void BuildParameterArgs_MultipleParameters_JoinedWithCommaSpace()
    {
        var args = ClickHouseQueryableParameterExtensions.BuildParameterArgs(
            new Dictionary<string, object?>
            {
                ["user_id"] = 123UL,
                ["start_date"] = new DateTime(2024, 1, 1),
            });
        Assert.Equal("user_id=123, start_date='2024-01-01 00:00:00'", args);
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

    #region Model Configuration Tests

    [Fact]
    public void ToParameterizedView_SetsCorrectAnnotations()
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
            entity.ToParameterizedView("user_events_view");
        });

        modelBuilder.Entity<ProductView>(entity =>
        {
            entity.ToParameterizedView("products_by_category_view");
        });

        modelBuilder.Entity<SaleView>(entity =>
        {
            entity.ToParameterizedView("sales_by_region_view");
        });

        modelBuilder.Entity<ItemView>(entity =>
        {
            entity.ToParameterizedView("items_view");
        });

        modelBuilder.Entity<LogView>(entity =>
        {
            entity.ToParameterizedView("logs_view");
        });

        // For strongly-typed access tests
        modelBuilder.Entity<TypedEventView>(entity =>
        {
            entity.ToParameterizedView("typed_user_events_view");
        });

        modelBuilder.Entity<ConvEventView>(entity =>
        {
            entity.ToParameterizedView("conv_user_events_view");
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
