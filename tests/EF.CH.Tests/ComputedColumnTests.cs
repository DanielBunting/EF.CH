using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Metadata.Attributes;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests;

#region Test Entities

/// <summary>
/// Entity with computed columns configured via fluent API.
/// </summary>
public class FluentComputedEntity
{
    public Guid Id { get; set; }
    public decimal Amount { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public DateTime OrderDate { get; set; }

    // MATERIALIZED - computed on INSERT, stored
    public decimal TotalWithTax { get; set; }

    // ALIAS - computed at query time, not stored
    public string FullName { get; set; } = string.Empty;

    // DEFAULT expression
    public DateTime CreatedAt { get; set; }
}

/// <summary>
/// Entity with computed columns configured via attributes.
/// </summary>
public class AttributeComputedEntity
{
    public Guid Id { get; set; }
    public decimal Amount { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;

    [MaterializedColumn("\"Amount\" * 1.1")]
    public decimal TotalWithTax { get; set; }

    [AliasColumn("concat(\"FirstName\", ' ', \"LastName\")")]
    public string FullName { get; set; } = string.Empty;

    [DefaultExpression("now()")]
    public DateTime CreatedAt { get; set; }
}

/// <summary>
/// Entity to test fluent API overriding attribute.
/// </summary>
public class OverrideComputedEntity
{
    public Guid Id { get; set; }
    public decimal Amount { get; set; }

    [MaterializedColumn("\"Amount\" * 1.1")] // Attribute
    public decimal TotalWithTax { get; set; }
}

/// <summary>
/// Entity with computed columns and codec combined.
/// </summary>
public class ComputedWithCodecEntity
{
    public Guid Id { get; set; }
    public DateTime EventDate { get; set; }

    // MATERIALIZED with CODEC
    public int EventYear { get; set; }
}

#endregion

#region Test DbContexts

public class FluentComputedContext : DbContext
{
    public FluentComputedContext(DbContextOptions<FluentComputedContext> options)
        : base(options) { }

    public DbSet<FluentComputedEntity> Entities => Set<FluentComputedEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FluentComputedEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("fluent_computed_entities");
            entity.UseMergeTree(x => x.OrderDate);

            // MATERIALIZED column
            entity.Property(e => e.TotalWithTax)
                .IsMaterialized("\"Amount\" * 1.1");

            // ALIAS column
            entity.Property(e => e.FullName)
                .IsAlias("concat(\"FirstName\", ' ', \"LastName\")");

            // DEFAULT expression
            entity.Property(e => e.CreatedAt)
                .HasDefaultExpression("now()");
        });
    }
}

public class AttributeComputedContext : DbContext
{
    public AttributeComputedContext(DbContextOptions<AttributeComputedContext> options)
        : base(options) { }

    public DbSet<AttributeComputedEntity> Entities => Set<AttributeComputedEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AttributeComputedEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("attribute_computed_entities");
            entity.UseMergeTree(x => x.Id);
        });
    }
}

public class OverrideComputedContext : DbContext
{
    public OverrideComputedContext(DbContextOptions<OverrideComputedContext> options)
        : base(options) { }

    public DbSet<OverrideComputedEntity> Entities => Set<OverrideComputedEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OverrideComputedEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("override_computed_entities");
            entity.UseMergeTree(x => x.Id);

            // Fluent API should override the attribute
            entity.Property(e => e.TotalWithTax)
                .IsMaterialized("\"Amount\" * 1.2"); // Different expression
        });
    }
}

public class ComputedWithCodecContext : DbContext
{
    public ComputedWithCodecContext(DbContextOptions<ComputedWithCodecContext> options)
        : base(options) { }

    public DbSet<ComputedWithCodecEntity> Entities => Set<ComputedWithCodecEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ComputedWithCodecEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("computed_with_codec_entities");
            entity.UseMergeTree(x => x.EventDate);

            // MATERIALIZED with CODEC
            entity.Property(e => e.EventYear)
                .IsMaterialized("toYear(\"EventDate\")")
                .HasCodec("Delta, ZSTD");
        });
    }
}

#endregion

public class ComputedColumnTests
{
    #region Fluent API Annotation Tests

    [Fact]
    public void IsMaterialized_SetsAnnotation()
    {
        using var context = CreateContext<FluentComputedContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentComputedEntity))!;
        var property = entityType.FindProperty(nameof(FluentComputedEntity.TotalWithTax))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.MaterializedExpression);

        Assert.NotNull(annotation);
        Assert.Equal("\"Amount\" * 1.1", annotation.Value);
    }

    [Fact]
    public void IsMaterialized_SetsValueGeneratedOnAdd()
    {
        using var context = CreateContext<FluentComputedContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentComputedEntity))!;
        var property = entityType.FindProperty(nameof(FluentComputedEntity.TotalWithTax))!;

        Assert.Equal(ValueGenerated.OnAdd, property.ValueGenerated);
    }

    [Fact]
    public void IsAlias_SetsAnnotation()
    {
        using var context = CreateContext<FluentComputedContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentComputedEntity))!;
        var property = entityType.FindProperty(nameof(FluentComputedEntity.FullName))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.AliasExpression);

        Assert.NotNull(annotation);
        Assert.Equal("concat(\"FirstName\", ' ', \"LastName\")", annotation.Value);
    }

    [Fact]
    public void IsAlias_SetsValueGeneratedOnAddOrUpdate()
    {
        using var context = CreateContext<FluentComputedContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentComputedEntity))!;
        var property = entityType.FindProperty(nameof(FluentComputedEntity.FullName))!;

        Assert.Equal(ValueGenerated.OnAddOrUpdate, property.ValueGenerated);
    }

    [Fact]
    public void HasDefaultExpression_SetsAnnotation()
    {
        using var context = CreateContext<FluentComputedContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentComputedEntity))!;
        var property = entityType.FindProperty(nameof(FluentComputedEntity.CreatedAt))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.DefaultExpression);

        Assert.NotNull(annotation);
        Assert.Equal("now()", annotation.Value);
    }

    [Fact]
    public void HasDefaultExpression_DoesNotSetValueGenerated()
    {
        using var context = CreateContext<FluentComputedContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentComputedEntity))!;
        var property = entityType.FindProperty(nameof(FluentComputedEntity.CreatedAt))!;

        // DEFAULT columns can be explicitly set, so shouldn't have ValueGenerated
        Assert.Equal(ValueGenerated.Never, property.ValueGenerated);
    }

    #endregion

    #region Attribute Annotation Tests

    [Fact]
    public void MaterializedColumnAttribute_SetsAnnotation()
    {
        using var context = CreateContext<AttributeComputedContext>();

        var entityType = context.Model.FindEntityType(typeof(AttributeComputedEntity))!;
        var property = entityType.FindProperty(nameof(AttributeComputedEntity.TotalWithTax))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.MaterializedExpression);

        Assert.NotNull(annotation);
        Assert.Equal("\"Amount\" * 1.1", annotation.Value);
    }

    [Fact]
    public void MaterializedColumnAttribute_SetsValueGeneratedOnAdd()
    {
        using var context = CreateContext<AttributeComputedContext>();

        var entityType = context.Model.FindEntityType(typeof(AttributeComputedEntity))!;
        var property = entityType.FindProperty(nameof(AttributeComputedEntity.TotalWithTax))!;

        Assert.Equal(ValueGenerated.OnAdd, property.ValueGenerated);
    }

    [Fact]
    public void AliasColumnAttribute_SetsAnnotation()
    {
        using var context = CreateContext<AttributeComputedContext>();

        var entityType = context.Model.FindEntityType(typeof(AttributeComputedEntity))!;
        var property = entityType.FindProperty(nameof(AttributeComputedEntity.FullName))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.AliasExpression);

        Assert.NotNull(annotation);
        Assert.Equal("concat(\"FirstName\", ' ', \"LastName\")", annotation.Value);
    }

    [Fact]
    public void AliasColumnAttribute_SetsValueGeneratedOnAddOrUpdate()
    {
        using var context = CreateContext<AttributeComputedContext>();

        var entityType = context.Model.FindEntityType(typeof(AttributeComputedEntity))!;
        var property = entityType.FindProperty(nameof(AttributeComputedEntity.FullName))!;

        Assert.Equal(ValueGenerated.OnAddOrUpdate, property.ValueGenerated);
    }

    [Fact]
    public void DefaultExpressionAttribute_SetsAnnotation()
    {
        using var context = CreateContext<AttributeComputedContext>();

        var entityType = context.Model.FindEntityType(typeof(AttributeComputedEntity))!;
        var property = entityType.FindProperty(nameof(AttributeComputedEntity.CreatedAt))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.DefaultExpression);

        Assert.NotNull(annotation);
        Assert.Equal("now()", annotation.Value);
    }

    #endregion

    #region Fluent API Overrides Attribute Tests

    [Fact]
    public void FluentApi_OverridesAttribute()
    {
        using var context = CreateContext<OverrideComputedContext>();

        var entityType = context.Model.FindEntityType(typeof(OverrideComputedEntity))!;
        var property = entityType.FindProperty(nameof(OverrideComputedEntity.TotalWithTax))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.MaterializedExpression);

        Assert.NotNull(annotation);
        // Fluent API (Amount * 1.2) should override the attribute (Amount * 1.1)
        Assert.Equal("\"Amount\" * 1.2", annotation.Value);
    }

    #endregion

    #region DDL Generation Tests

    [Fact]
    public void CreateTable_GeneratesMaterializedClause()
    {
        using var context = CreateContext<FluentComputedContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("MATERIALIZED \"Amount\" * 1.1", script);
    }

    [Fact]
    public void CreateTable_GeneratesAliasClause()
    {
        using var context = CreateContext<FluentComputedContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("ALIAS concat(\"FirstName\", ' ', \"LastName\")", script);
    }

    [Fact]
    public void CreateTable_GeneratesDefaultExpressionClause()
    {
        using var context = CreateContext<FluentComputedContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("DEFAULT now()", script);
    }

    [Fact]
    public void CreateTable_GeneratesComputedClause_ForAttributes()
    {
        using var context = CreateContext<AttributeComputedContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("MATERIALIZED \"Amount\" * 1.1", script);
        Assert.Contains("ALIAS concat(\"FirstName\", ' ', \"LastName\")", script);
        Assert.Contains("DEFAULT now()", script);
    }

    [Fact]
    public void CreateTable_GeneratesCodecAfterMaterialized()
    {
        using var context = CreateContext<ComputedWithCodecContext>();

        var script = context.Database.GenerateCreateScript();

        // CODEC should come after MATERIALIZED expression
        Assert.Contains("MATERIALIZED toYear(\"EventDate\") CODEC(Delta, ZSTD)", script);
    }

    #endregion

    #region Validation Tests

    [Fact]
    public void IsMaterialized_NullExpression_ThrowsException()
    {
        // ArgumentException.ThrowIfNullOrWhiteSpace throws ArgumentNullException for null
        Assert.ThrowsAny<ArgumentException>(() =>
        {
            var options = new DbContextOptionsBuilder<DbContext>()
                .UseClickHouse("Host=localhost;Database=test")
                .Options;

            using var context = new DbContext(options);
            var builder = new ModelBuilder();
            builder.Entity<FluentComputedEntity>()
                .Property(e => e.TotalWithTax)
                .IsMaterialized(null!);
        });
    }

    [Fact]
    public void IsMaterialized_EmptyExpression_ThrowsException()
    {
        Assert.ThrowsAny<ArgumentException>(() =>
        {
            var options = new DbContextOptionsBuilder<DbContext>()
                .UseClickHouse("Host=localhost;Database=test")
                .Options;

            using var context = new DbContext(options);
            var builder = new ModelBuilder();
            builder.Entity<FluentComputedEntity>()
                .Property(e => e.TotalWithTax)
                .IsMaterialized("");
        });
    }

    [Fact]
    public void IsAlias_NullExpression_ThrowsException()
    {
        // ArgumentException.ThrowIfNullOrWhiteSpace throws ArgumentNullException for null
        Assert.ThrowsAny<ArgumentException>(() =>
        {
            var options = new DbContextOptionsBuilder<DbContext>()
                .UseClickHouse("Host=localhost;Database=test")
                .Options;

            using var context = new DbContext(options);
            var builder = new ModelBuilder();
            builder.Entity<FluentComputedEntity>()
                .Property(e => e.FullName)
                .IsAlias(null!);
        });
    }

    [Fact]
    public void HasDefaultExpression_NullExpression_ThrowsException()
    {
        // ArgumentException.ThrowIfNullOrWhiteSpace throws ArgumentNullException for null
        Assert.ThrowsAny<ArgumentException>(() =>
        {
            var options = new DbContextOptionsBuilder<DbContext>()
                .UseClickHouse("Host=localhost;Database=test")
                .Options;

            using var context = new DbContext(options);
            var builder = new ModelBuilder();
            builder.Entity<FluentComputedEntity>()
                .Property(e => e.CreatedAt)
                .HasDefaultExpression(null!);
        });
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
/// Integration tests that require a real ClickHouse instance.
/// </summary>
public class ComputedColumnIntegrationTests : IAsyncLifetime
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

    [Fact]
    public async Task CreateTable_WithMaterializedColumn_ExecutesSuccessfully()
    {
        var options = new DbContextOptionsBuilder<FluentComputedContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new FluentComputedContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Table should be created successfully - verify by inserting a row
        context.Entities.Add(new FluentComputedEntity
        {
            Id = Guid.NewGuid(),
            Amount = 100m,
            FirstName = "John",
            LastName = "Doe",
            OrderDate = DateTime.UtcNow
            // TotalWithTax will be computed
            // FullName will be computed
            // CreatedAt will use default
        });

        await context.SaveChangesAsync();

        var count = await context.Entities.CountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task CreateTable_WithAttributeComputedColumns_ExecutesSuccessfully()
    {
        var options = new DbContextOptionsBuilder<AttributeComputedContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new AttributeComputedContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Table should be created successfully - verify by inserting a row
        context.Entities.Add(new AttributeComputedEntity
        {
            Id = Guid.NewGuid(),
            Amount = 200m,
            FirstName = "Jane",
            LastName = "Smith"
        });

        await context.SaveChangesAsync();

        var count = await context.Entities.CountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task MaterializedColumn_ComputesValueOnInsert()
    {
        var options = new DbContextOptionsBuilder<FluentComputedContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new FluentComputedContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var id = Guid.NewGuid();
        context.Entities.Add(new FluentComputedEntity
        {
            Id = id,
            Amount = 100m,
            FirstName = "Test",
            LastName = "User",
            OrderDate = DateTime.UtcNow
        });

        await context.SaveChangesAsync();

        // Query and explicitly select the MATERIALIZED column
        var entity = await context.Entities
            .Where(e => e.Id == id)
            .Select(e => new { e.Id, e.Amount, e.TotalWithTax })
            .FirstOrDefaultAsync();

        Assert.NotNull(entity);
        Assert.Equal(100m, entity.Amount);
        // TotalWithTax should be Amount * 1.1 = 110
        Assert.Equal(110m, entity.TotalWithTax);
    }

    [Fact]
    public async Task AliasColumn_ComputesValueAtQueryTime()
    {
        var options = new DbContextOptionsBuilder<FluentComputedContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new FluentComputedContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var id = Guid.NewGuid();
        context.Entities.Add(new FluentComputedEntity
        {
            Id = id,
            Amount = 50m,
            FirstName = "Alice",
            LastName = "Wonder",
            OrderDate = DateTime.UtcNow
        });

        await context.SaveChangesAsync();

        // Query and explicitly select the ALIAS column
        var entity = await context.Entities
            .Where(e => e.Id == id)
            .Select(e => new { e.Id, e.FirstName, e.LastName, e.FullName })
            .FirstOrDefaultAsync();

        Assert.NotNull(entity);
        Assert.Equal("Alice", entity.FirstName);
        Assert.Equal("Wonder", entity.LastName);
        // FullName should be "Alice Wonder"
        Assert.Equal("Alice Wonder", entity.FullName);
    }
}
