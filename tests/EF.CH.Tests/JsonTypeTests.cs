using System.Text.Json;
using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Metadata.Attributes;
using EF.CH.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests;

#region Test Entities

/// <summary>
/// Entity with JSON column configured via fluent API.
/// </summary>
public class FluentJsonEntity
{
    public Guid Id { get; set; }
    public JsonElement Metadata { get; set; }
    public JsonDocument? OptionalData { get; set; }
}

/// <summary>
/// Entity with JSON column configured via fluent API with parameters.
/// </summary>
public class ParameterizedJsonEntity
{
    public Guid Id { get; set; }
    public JsonElement Data { get; set; }
}

/// <summary>
/// Entity with JSON column configured via attribute.
/// </summary>
public class AttributeJsonEntity
{
    public Guid Id { get; set; }

    [ClickHouseJson(MaxDynamicPaths = 512, MaxDynamicTypes = 16)]
    public JsonElement Config { get; set; }
}

/// <summary>
/// Typed POCO for JSON column.
/// </summary>
public class OrderMetadata
{
    public string CustomerName { get; set; } = string.Empty;
    public string CustomerEmail { get; set; } = string.Empty;
    public ShippingAddress? ShippingAddress { get; set; }
    public List<string> Tags { get; set; } = [];
}

public class ShippingAddress
{
    public string Street { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string Country { get; set; } = string.Empty;
    public string PostalCode { get; set; } = string.Empty;
}

/// <summary>
/// Entity with typed JSON column.
/// </summary>
public class TypedJsonEntity
{
    public Guid Id { get; set; }

    [ClickHouseJson(IsTyped = true)]
    public OrderMetadata Metadata { get; set; } = new();
}

#endregion

#region Test DbContexts

public class FluentJsonContext : DbContext
{
    public FluentJsonContext(DbContextOptions<FluentJsonContext> options)
        : base(options) { }

    public DbSet<FluentJsonEntity> Entities => Set<FluentJsonEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FluentJsonEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("fluent_json_entities");
            entity.UseMergeTree(x => x.Id);

            entity.Property(e => e.Metadata)
                .HasColumnType("JSON");

            entity.Property(e => e.OptionalData)
                .HasColumnType("JSON");
        });
    }
}

public class ParameterizedJsonContext : DbContext
{
    public ParameterizedJsonContext(DbContextOptions<ParameterizedJsonContext> options)
        : base(options) { }

    public DbSet<ParameterizedJsonEntity> Entities => Set<ParameterizedJsonEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ParameterizedJsonEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("parameterized_json_entities");
            entity.UseMergeTree(x => x.Id);

            entity.Property(e => e.Data)
                .HasColumnType("JSON")
                .HasMaxDynamicPaths(2048)
                .HasMaxDynamicTypes(64);
        });
    }
}

public class AttributeJsonContext : DbContext
{
    public AttributeJsonContext(DbContextOptions<AttributeJsonContext> options)
        : base(options) { }

    public DbSet<AttributeJsonEntity> Entities => Set<AttributeJsonEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AttributeJsonEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("attribute_json_entities");
            entity.UseMergeTree(x => x.Id);
        });
    }
}

public class TypedJsonContext : DbContext
{
    public TypedJsonContext(DbContextOptions<TypedJsonContext> options)
        : base(options) { }

    public DbSet<TypedJsonEntity> Entities => Set<TypedJsonEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TypedJsonEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("typed_json_entities");
            entity.UseMergeTree(x => x.Id);

            entity.Property(e => e.Metadata)
                .HasColumnType("JSON");
        });
    }
}

#endregion

public class JsonTypeTests
{
    #region Fluent API Annotation Tests

    [Fact]
    public void HasColumnType_JSON_SetsJsonType()
    {
        using var context = CreateContext<FluentJsonContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentJsonEntity))!;
        var property = entityType.FindProperty(nameof(FluentJsonEntity.Metadata))!;

        Assert.Equal("JSON", property.GetColumnType());
    }

    [Fact]
    public void HasMaxDynamicPaths_SetsAnnotation()
    {
        using var context = CreateContext<ParameterizedJsonContext>();

        var entityType = context.Model.FindEntityType(typeof(ParameterizedJsonEntity))!;
        var property = entityType.FindProperty(nameof(ParameterizedJsonEntity.Data))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.JsonMaxDynamicPaths);

        Assert.NotNull(annotation);
        Assert.Equal(2048, annotation.Value);
    }

    [Fact]
    public void HasMaxDynamicTypes_SetsAnnotation()
    {
        using var context = CreateContext<ParameterizedJsonContext>();

        var entityType = context.Model.FindEntityType(typeof(ParameterizedJsonEntity))!;
        var property = entityType.FindProperty(nameof(ParameterizedJsonEntity.Data))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.JsonMaxDynamicTypes);

        Assert.NotNull(annotation);
        Assert.Equal(64, annotation.Value);
    }

    #endregion

    #region Attribute Annotation Tests

    [Fact]
    public void ClickHouseJsonAttribute_MaxDynamicPaths_SetsAnnotation()
    {
        using var context = CreateContext<AttributeJsonContext>();

        var entityType = context.Model.FindEntityType(typeof(AttributeJsonEntity))!;
        var property = entityType.FindProperty(nameof(AttributeJsonEntity.Config))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.JsonMaxDynamicPaths);

        Assert.NotNull(annotation);
        Assert.Equal(512, annotation.Value);
    }

    [Fact]
    public void ClickHouseJsonAttribute_MaxDynamicTypes_SetsAnnotation()
    {
        using var context = CreateContext<AttributeJsonContext>();

        var entityType = context.Model.FindEntityType(typeof(AttributeJsonEntity))!;
        var property = entityType.FindProperty(nameof(AttributeJsonEntity.Config))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.JsonMaxDynamicTypes);

        Assert.NotNull(annotation);
        Assert.Equal(16, annotation.Value);
    }

    #endregion

    #region DDL Generation Tests

    [Fact]
    public void CreateTable_GeneratesJsonType_ForFluentApi()
    {
        using var context = CreateContext<FluentJsonContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("\"Metadata\" JSON", script);
        Assert.Contains("\"OptionalData\" Nullable(JSON)", script);
    }

    [Fact]
    public void CreateTable_GeneratesJsonWithParameters_ForFluentApi()
    {
        using var context = CreateContext<ParameterizedJsonContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("\"Data\" JSON(max_dynamic_paths=2048, max_dynamic_types=64)", script);
    }

    [Fact]
    public void CreateTable_GeneratesJsonWithParameters_ForAttribute()
    {
        using var context = CreateContext<AttributeJsonContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("\"Config\" JSON(max_dynamic_paths=512, max_dynamic_types=16)", script);
    }

    #endregion

    #region Typed JSON Value Converter Tests

    [Fact]
    public void TypedJson_AppliesValueConverter()
    {
        using var context = CreateContext<TypedJsonContext>();

        var entityType = context.Model.FindEntityType(typeof(TypedJsonEntity))!;
        var property = entityType.FindProperty(nameof(TypedJsonEntity.Metadata))!;

        var converter = property.GetValueConverter();
        Assert.NotNull(converter);
        Assert.IsType<ClickHouseJsonValueConverter<OrderMetadata>>(converter);
    }

    [Fact]
    public void JsonValueConverter_SerializesPocoToJson()
    {
        var converter = new ClickHouseJsonValueConverter<OrderMetadata>();
        var poco = new OrderMetadata
        {
            CustomerName = "John Doe",
            CustomerEmail = "john@example.com",
            Tags = ["vip", "frequent"]
        };

        var json = (string)converter.ConvertToProvider(poco)!;

        Assert.Contains("\"customer_name\"", json); // Snake case
        Assert.Contains("\"customer_email\"", json);
        Assert.Contains("\"John Doe\"", json);
    }

    [Fact]
    public void JsonValueConverter_DeserializesJsonToPoco()
    {
        var converter = new ClickHouseJsonValueConverter<OrderMetadata>();
        var json = """{"customer_name":"Jane Doe","customer_email":"jane@example.com","tags":["premium"]}""";

        var poco = (OrderMetadata)converter.ConvertFromProvider(json)!;

        Assert.Equal("Jane Doe", poco.CustomerName);
        Assert.Equal("jane@example.com", poco.CustomerEmail);
        Assert.Contains("premium", poco.Tags);
    }

    #endregion

    #region JSON Path Expression Tests

    [Fact]
    public void JsonPathExpression_ParsesSimplePath()
    {
        var column = new Microsoft.EntityFrameworkCore.Query.SqlExpressions.SqlConstantExpression(
            "test", null);
        var expr = EF.CH.Query.Internal.Expressions.ClickHouseJsonPathExpression.Create(
            column, "user.email", typeof(string), null);

        Assert.Equal(2, expr.PathSegments.Count);
        Assert.Equal("user", expr.PathSegments[0]);
        Assert.Equal("email", expr.PathSegments[1]);
        Assert.All(expr.ArrayIndices, i => Assert.Null(i));
    }

    [Fact]
    public void JsonPathExpression_ParsesArrayIndex()
    {
        var column = new Microsoft.EntityFrameworkCore.Query.SqlExpressions.SqlConstantExpression(
            "test", null);
        var expr = EF.CH.Query.Internal.Expressions.ClickHouseJsonPathExpression.Create(
            column, "tags[0]", typeof(string), null);

        Assert.Single(expr.PathSegments);
        Assert.Equal("tags", expr.PathSegments[0]);
        Assert.Equal(0, expr.ArrayIndices[0]);
    }

    [Fact]
    public void JsonPathExpression_ParsesNestedPathWithArray()
    {
        var column = new Microsoft.EntityFrameworkCore.Query.SqlExpressions.SqlConstantExpression(
            "test", null);
        var expr = EF.CH.Query.Internal.Expressions.ClickHouseJsonPathExpression.Create(
            column, "order.items[2].name", typeof(string), null);

        Assert.Equal(3, expr.PathSegments.Count);
        Assert.Equal("order", expr.PathSegments[0]);
        Assert.Equal("items", expr.PathSegments[1]);
        Assert.Equal("name", expr.PathSegments[2]);
        Assert.Null(expr.ArrayIndices[0]);
        Assert.Equal(2, expr.ArrayIndices[1]);
        Assert.Null(expr.ArrayIndices[2]);
    }

    #endregion

    private static TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }
}

/// <summary>
/// Integration tests that require a real ClickHouse 24.8+ instance.
/// </summary>
public class JsonTypeIntegrationTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:24.8")
        .Build();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    [Fact]
    public async Task CreateTable_WithJsonColumn_ExecutesSuccessfully()
    {
        var options = new DbContextOptionsBuilder<FluentJsonContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new FluentJsonContext(options);
        await context.Database.EnsureCreatedAsync();

        // Verify table was created
        var tables = await context.Database
            .SqlQuery<string>($"SHOW TABLES")
            .ToListAsync();

        Assert.Contains("fluent_json_entities", tables);
    }

    [Fact]
    public async Task InsertAndQuery_JsonData_WorksCorrectly()
    {
        var options = new DbContextOptionsBuilder<FluentJsonContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new FluentJsonContext(options);
        await context.Database.EnsureCreatedAsync();

        // Insert JSON data
        var id = Guid.NewGuid();
        var json = JsonSerializer.Deserialize<JsonElement>(
            """{"user": {"name": "Test User", "email": "test@example.com"}, "score": 42}""");

        context.Entities.Add(new FluentJsonEntity
        {
            Id = id,
            Metadata = json
        });
        await context.SaveChangesAsync();

        // Query the data
        var entity = await context.Entities.FindAsync(id);

        Assert.NotNull(entity);
        Assert.Equal("Test User", entity.Metadata.GetProperty("user").GetProperty("name").GetString());
    }

    [Fact]
    public async Task Query_JsonPath_TranslatesToClickHouseSyntax()
    {
        var options = new DbContextOptionsBuilder<FluentJsonContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new FluentJsonContext(options);
        await context.Database.EnsureCreatedAsync();

        // Insert test data
        var json = JsonSerializer.Deserialize<JsonElement>(
            """{"user": {"name": "Alice", "email": "alice@example.com"}, "active": true}""");

        context.Entities.Add(new FluentJsonEntity
        {
            Id = Guid.NewGuid(),
            Metadata = json
        });
        await context.SaveChangesAsync();

        // Query using GetPath
        var results = await context.Entities
            .Where(e => e.Metadata.GetPath<string>("user.email") == "alice@example.com")
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("alice@example.com",
            results[0].Metadata.GetProperty("user").GetProperty("email").GetString());
    }
}
