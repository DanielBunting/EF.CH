using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Metadata.Attributes;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests;

#region Test Entities

/// <summary>
/// Entity with skip indices configured via fluent API.
/// </summary>
public class FluentSkipIndexEntity
{
    public Guid Id { get; set; }
    public DateTime CreatedAt { get; set; }
    public string[] Tags { get; set; } = [];
    public string ErrorMessage { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
}

/// <summary>
/// Entity with skip indices configured via attributes.
/// </summary>
public class AttributeSkipIndexEntity
{
    public Guid Id { get; set; }

    [MinMaxIndex(Granularity = 2)]
    public DateTime CreatedAt { get; set; }

    [BloomFilterIndex(FalsePositive = 0.025, Granularity = 3)]
    public string[] Tags { get; set; } = [];

    [TokenBFIndex(Size = 10240, Hashes = 3, Seed = 0, Granularity = 4)]
    public string ErrorMessage { get; set; } = string.Empty;

    [NgramBFIndex(NgramSize = 4, Size = 10240, Hashes = 3, Seed = 0, Granularity = 5)]
    public string Description { get; set; } = string.Empty;

    [SetIndex(MaxRows = 100, Granularity = 2)]
    public string Status { get; set; } = string.Empty;
}

/// <summary>
/// Entity to test fluent API overriding attribute.
/// </summary>
public class OverrideSkipIndexEntity
{
    public Guid Id { get; set; }

    [BloomFilterIndex(FalsePositive = 0.1, Granularity = 5)]
    public string[] Tags { get; set; } = [];
}

#endregion

#region Test DbContexts

public class FluentSkipIndexContext : DbContext
{
    public FluentSkipIndexContext(DbContextOptions<FluentSkipIndexContext> options)
        : base(options) { }

    public DbSet<FluentSkipIndexEntity> Entities => Set<FluentSkipIndexEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FluentSkipIndexEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("fluent_skip_index_entities");
            entity.UseMergeTree(x => new { x.CreatedAt, x.Id });

            // Minmax index with custom granularity
            entity.HasIndex(e => e.CreatedAt)
                .UseMinmax()
                .HasGranularity(2);

            // Bloom filter for array
            entity.HasIndex(e => e.Tags)
                .UseBloomFilter(falsePositive: 0.025)
                .HasGranularity(3);

            // TokenBF for text search
            entity.HasIndex(e => e.ErrorMessage)
                .UseTokenBF(size: 10240, hashes: 3, seed: 0)
                .HasGranularity(4);

            // NgramBF for fuzzy matching
            entity.HasIndex(e => e.Description)
                .UseNgramBF(ngramSize: 4, size: 10240, hashes: 3, seed: 0)
                .HasGranularity(5);

            // Set for low-cardinality
            entity.HasIndex(e => e.Status)
                .UseSet(maxRows: 100)
                .HasGranularity(2);
        });
    }
}

public class AttributeSkipIndexContext : DbContext
{
    public AttributeSkipIndexContext(DbContextOptions<AttributeSkipIndexContext> options)
        : base(options) { }

    public DbSet<AttributeSkipIndexEntity> Entities => Set<AttributeSkipIndexEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AttributeSkipIndexEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("attribute_skip_index_entities");
            entity.UseMergeTree(x => new { x.CreatedAt, x.Id });
        });
    }
}

public class OverrideSkipIndexContext : DbContext
{
    public OverrideSkipIndexContext(DbContextOptions<OverrideSkipIndexContext> options)
        : base(options) { }

    public DbSet<OverrideSkipIndexEntity> Entities => Set<OverrideSkipIndexEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OverrideSkipIndexEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("override_skip_index_entities");
            entity.UseMergeTree(x => x.Id);

            // Fluent API should override the attribute (0.1, granularity 5)
            entity.HasIndex(e => e.Tags)
                .UseBloomFilter(falsePositive: 0.05)
                .HasGranularity(3);
        });
    }
}

#endregion

public class SkipIndexTests
{
    #region Fluent API Annotation Tests

    [Fact]
    public void UseMinmax_SetsAnnotations()
    {
        using var context = CreateContext<FluentSkipIndexContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentSkipIndexEntity))!;
        var index = entityType.GetIndexes().First(i => i.Properties.Any(p => p.Name == "CreatedAt"));

        Assert.NotNull(index);
        Assert.Equal(SkipIndexType.Minmax, index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexType)?.Value);
        Assert.Equal(2, index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexGranularity)?.Value);
    }

    [Fact]
    public void UseBloomFilter_SetsAnnotations()
    {
        using var context = CreateContext<FluentSkipIndexContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentSkipIndexEntity))!;
        var index = entityType.GetIndexes().First(i => i.Properties.Any(p => p.Name == "Tags"));

        Assert.NotNull(index);
        Assert.Equal(SkipIndexType.BloomFilter, index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexType)?.Value);
        Assert.Equal(3, index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexGranularity)?.Value);

        var indexParams = index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexParams)?.Value as SkipIndexParams;
        Assert.NotNull(indexParams);
        Assert.Equal(0.025, indexParams.BloomFilterFalsePositive);
    }

    [Fact]
    public void UseTokenBF_SetsAnnotations()
    {
        using var context = CreateContext<FluentSkipIndexContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentSkipIndexEntity))!;
        var index = entityType.GetIndexes().First(i => i.Properties.Any(p => p.Name == "ErrorMessage"));

        Assert.NotNull(index);
        Assert.Equal(SkipIndexType.TokenBF, index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexType)?.Value);
        Assert.Equal(4, index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexGranularity)?.Value);

        var indexParams = index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexParams)?.Value as SkipIndexParams;
        Assert.NotNull(indexParams);
        Assert.Equal(10240, indexParams.TokenBFSize);
        Assert.Equal(3, indexParams.TokenBFHashes);
        Assert.Equal(0, indexParams.TokenBFSeed);
    }

    [Fact]
    public void UseNgramBF_SetsAnnotations()
    {
        using var context = CreateContext<FluentSkipIndexContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentSkipIndexEntity))!;
        var index = entityType.GetIndexes().First(i => i.Properties.Any(p => p.Name == "Description"));

        Assert.NotNull(index);
        Assert.Equal(SkipIndexType.NgramBF, index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexType)?.Value);
        Assert.Equal(5, index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexGranularity)?.Value);

        var indexParams = index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexParams)?.Value as SkipIndexParams;
        Assert.NotNull(indexParams);
        Assert.Equal(4, indexParams.NgramSize);
        Assert.Equal(10240, indexParams.NgramBFSize);
        Assert.Equal(3, indexParams.NgramBFHashes);
        Assert.Equal(0, indexParams.NgramBFSeed);
    }

    [Fact]
    public void UseSet_SetsAnnotations()
    {
        using var context = CreateContext<FluentSkipIndexContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentSkipIndexEntity))!;
        var index = entityType.GetIndexes().First(i => i.Properties.Any(p => p.Name == "Status"));

        Assert.NotNull(index);
        Assert.Equal(SkipIndexType.Set, index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexType)?.Value);
        Assert.Equal(2, index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexGranularity)?.Value);

        var indexParams = index.FindAnnotation(ClickHouseAnnotationNames.SkipIndexParams)?.Value as SkipIndexParams;
        Assert.NotNull(indexParams);
        Assert.Equal(100, indexParams.SetMaxRows);
    }

    #endregion

    #region Validation Tests

    [Fact]
    public void HasGranularity_InvalidValue_ThrowsException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            var options = new DbContextOptionsBuilder<InvalidGranularityContext>()
                .UseClickHouse("Host=localhost;Database=test")
                .Options;

            using var context = new InvalidGranularityContext(options);
            _ = context.Model; // Force model building
        });
    }

    [Fact]
    public void UseBloomFilter_InvalidFalsePositive_ThrowsException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            var options = new DbContextOptionsBuilder<InvalidBloomFilterContext>()
                .UseClickHouse("Host=localhost;Database=test")
                .Options;

            using var context = new InvalidBloomFilterContext(options);
            _ = context.Model; // Force model building
        });
    }

    [Fact]
    public void UseTokenBF_InvalidSize_ThrowsException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            var options = new DbContextOptionsBuilder<InvalidTokenBFContext>()
                .UseClickHouse("Host=localhost;Database=test")
                .Options;

            using var context = new InvalidTokenBFContext(options);
            _ = context.Model; // Force model building
        });
    }

    [Fact]
    public void UseSet_InvalidMaxRows_ThrowsException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            var options = new DbContextOptionsBuilder<InvalidSetContext>()
                .UseClickHouse("Host=localhost;Database=test")
                .Options;

            using var context = new InvalidSetContext(options);
            _ = context.Model; // Force model building
        });
    }

    #endregion

    #region SkipIndexParams Tests

    [Fact]
    public void SkipIndexParams_BuildTypeSpecification_Minmax()
    {
        var indexParams = new SkipIndexParams();
        var typeSpec = indexParams.BuildTypeSpecification(SkipIndexType.Minmax);

        Assert.Equal("TYPE minmax", typeSpec);
    }

    [Fact]
    public void SkipIndexParams_BuildTypeSpecification_BloomFilter()
    {
        var indexParams = SkipIndexParams.ForBloomFilter(0.025);
        var typeSpec = indexParams.BuildTypeSpecification(SkipIndexType.BloomFilter);

        Assert.Equal("TYPE bloom_filter(0.025)", typeSpec);
    }

    [Fact]
    public void SkipIndexParams_BuildTypeSpecification_TokenBF()
    {
        var indexParams = SkipIndexParams.ForTokenBF(10240, 3, 0);
        var typeSpec = indexParams.BuildTypeSpecification(SkipIndexType.TokenBF);

        Assert.Equal("TYPE tokenbf_v1(10240, 3, 0)", typeSpec);
    }

    [Fact]
    public void SkipIndexParams_BuildTypeSpecification_NgramBF()
    {
        var indexParams = SkipIndexParams.ForNgramBF(4, 10240, 3, 0);
        var typeSpec = indexParams.BuildTypeSpecification(SkipIndexType.NgramBF);

        Assert.Equal("TYPE ngrambf_v1(4, 10240, 3, 0)", typeSpec);
    }

    [Fact]
    public void SkipIndexParams_BuildTypeSpecification_Set()
    {
        var indexParams = SkipIndexParams.ForSet(100);
        var typeSpec = indexParams.BuildTypeSpecification(SkipIndexType.Set);

        Assert.Equal("TYPE set(100)", typeSpec);
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

#region Invalid Configuration Contexts for Validation Tests

public class InvalidGranularityContext : DbContext
{
    public InvalidGranularityContext(DbContextOptions<InvalidGranularityContext> options) : base(options) { }

    public DbSet<FluentSkipIndexEntity> Entities => Set<FluentSkipIndexEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FluentSkipIndexEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);

            entity.HasIndex(e => e.CreatedAt)
                .HasGranularity(0); // Invalid: must be 1-1000
        });
    }
}

public class InvalidBloomFilterContext : DbContext
{
    public InvalidBloomFilterContext(DbContextOptions<InvalidBloomFilterContext> options) : base(options) { }

    public DbSet<FluentSkipIndexEntity> Entities => Set<FluentSkipIndexEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FluentSkipIndexEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);

            entity.HasIndex(e => e.Tags)
                .UseBloomFilter(falsePositive: 0.6); // Invalid: must be 0.001-0.5
        });
    }
}

public class InvalidTokenBFContext : DbContext
{
    public InvalidTokenBFContext(DbContextOptions<InvalidTokenBFContext> options) : base(options) { }

    public DbSet<FluentSkipIndexEntity> Entities => Set<FluentSkipIndexEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FluentSkipIndexEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);

            entity.HasIndex(e => e.ErrorMessage)
                .UseTokenBF(size: 100); // Invalid: must be 256-1048576
        });
    }
}

public class InvalidSetContext : DbContext
{
    public InvalidSetContext(DbContextOptions<InvalidSetContext> options) : base(options) { }

    public DbSet<FluentSkipIndexEntity> Entities => Set<FluentSkipIndexEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FluentSkipIndexEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);

            entity.HasIndex(e => e.Status)
                .UseSet(maxRows: 0); // Invalid: must be 1-100000
        });
    }
}

#endregion

/// <summary>
/// Integration tests that require a real ClickHouse instance.
/// </summary>
public class SkipIndexIntegrationTests : IAsyncLifetime
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
    public async Task CreateTable_WithSkipIndices_ExecutesSuccessfully()
    {
        var options = new DbContextOptionsBuilder<FluentSkipIndexContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new FluentSkipIndexContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Table should be created successfully - verify by inserting a row
        context.Entities.Add(new FluentSkipIndexEntity
        {
            Id = Guid.NewGuid(),
            CreatedAt = DateTime.UtcNow,
            Tags = ["test", "tag"],
            ErrorMessage = "Test error message",
            Description = "Test description",
            Status = "active"
        });

        await context.SaveChangesAsync();

        var count = await context.Entities.CountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task CreateTable_WithAttributeSkipIndices_ExecutesSuccessfully()
    {
        var options = new DbContextOptionsBuilder<AttributeSkipIndexContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new AttributeSkipIndexContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Table should be created successfully - verify by inserting a row
        context.Entities.Add(new AttributeSkipIndexEntity
        {
            Id = Guid.NewGuid(),
            CreatedAt = DateTime.UtcNow,
            Tags = ["test", "tag"],
            ErrorMessage = "Test error message",
            Description = "Test description",
            Status = "pending"
        });

        await context.SaveChangesAsync();

        var count = await context.Entities.CountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task Query_WithSkipIndexColumn_Works()
    {
        var options = new DbContextOptionsBuilder<FluentSkipIndexContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new FluentSkipIndexContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert test data
        context.Entities.Add(new FluentSkipIndexEntity
        {
            Id = Guid.NewGuid(),
            CreatedAt = DateTime.UtcNow.AddDays(-1),
            Tags = ["important", "urgent"],
            ErrorMessage = "Connection timeout",
            Description = "Network issue",
            Status = "error"
        });

        context.Entities.Add(new FluentSkipIndexEntity
        {
            Id = Guid.NewGuid(),
            CreatedAt = DateTime.UtcNow,
            Tags = ["info"],
            ErrorMessage = "",
            Description = "Normal operation",
            Status = "success"
        });

        await context.SaveChangesAsync();

        // Query using indexed columns
        var errorResults = await context.Entities
            .Where(e => e.Status == "error")
            .ToListAsync();

        Assert.Single(errorResults);
        Assert.Equal("error", errorResults[0].Status);
    }
}
