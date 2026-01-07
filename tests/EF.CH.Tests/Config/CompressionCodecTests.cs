using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Metadata.Attributes;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Config;

#region Test Entities

/// <summary>
/// Entity with codec configured via fluent API.
/// </summary>
public class FluentCodecEntity
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public long SensorId { get; set; }
    public double Value { get; set; }
    public string RawPayload { get; set; } = string.Empty;
    public byte[] UncompressedData { get; set; } = [];
}

/// <summary>
/// Entity with codec configured via attributes.
/// </summary>
public class AttributeCodecEntity
{
    public Guid Id { get; set; }

    [TimestampCodec]
    public DateTime Timestamp { get; set; }

    [SequentialCodec]
    public long SensorId { get; set; }

    [FloatCodec]
    public double Value { get; set; }

    [HighCompressionCodec]
    public string RawPayload { get; set; } = string.Empty;

    [NoCompression]
    public byte[] UncompressedData { get; set; } = [];

    [ClickHouseCodec("Delta, ZSTD(5)")]
    public int Counter { get; set; }
}

/// <summary>
/// Entity to test fluent API overriding attribute.
/// </summary>
public class OverrideCodecEntity
{
    public Guid Id { get; set; }

    [HighCompressionCodec] // Attribute says ZSTD(9)
    public string Data { get; set; } = string.Empty;
}

#endregion

#region Test DbContexts

public class FluentCodecContext : DbContext
{
    public FluentCodecContext(DbContextOptions<FluentCodecContext> options)
        : base(options) { }

    public DbSet<FluentCodecEntity> Entities => Set<FluentCodecEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FluentCodecEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("fluent_codec_entities");
            entity.UseMergeTree(x => new { x.Timestamp, x.SensorId });

            // Fluent builder API
            entity.Property(e => e.Timestamp)
                .HasCodec(c => c.DoubleDelta().LZ4());

            entity.Property(e => e.SensorId)
                .HasCodec(c => c.Delta().ZSTD(3));

            // Raw string API
            entity.Property(e => e.Value)
                .HasCodec("Gorilla, ZSTD(1)");

            // Convenience methods
            entity.Property(e => e.RawPayload)
                .HasHighCompressionCodec();

            entity.Property(e => e.UncompressedData)
                .HasNoCompression();
        });
    }
}

public class AttributeCodecContext : DbContext
{
    public AttributeCodecContext(DbContextOptions<AttributeCodecContext> options)
        : base(options) { }

    public DbSet<AttributeCodecEntity> Entities => Set<AttributeCodecEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AttributeCodecEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("attribute_codec_entities");
            entity.UseMergeTree(x => new { x.Timestamp, x.SensorId });
        });
    }
}

public class OverrideCodecContext : DbContext
{
    public OverrideCodecContext(DbContextOptions<OverrideCodecContext> options)
        : base(options) { }

    public DbSet<OverrideCodecEntity> Entities => Set<OverrideCodecEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OverrideCodecEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("override_codec_entities");
            entity.UseMergeTree(x => x.Id);

            // Fluent API should override the [HighCompressionCodec] attribute
            entity.Property(e => e.Data)
                .HasCodec("LZ4");
        });
    }
}

#endregion

public class CompressionCodecTests
{
    #region Fluent API Annotation Tests

    [Fact]
    public void HasCodec_RawString_SetsAnnotation()
    {
        using var context = CreateContext<FluentCodecContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentCodecEntity))!;
        var property = entityType.FindProperty(nameof(FluentCodecEntity.Value))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.CompressionCodec);

        Assert.NotNull(annotation);
        Assert.Equal("Gorilla, ZSTD(1)", annotation.Value);
    }

    [Fact]
    public void HasCodec_FluentBuilder_CreatesChain()
    {
        using var context = CreateContext<FluentCodecContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentCodecEntity))!;
        var property = entityType.FindProperty(nameof(FluentCodecEntity.Timestamp))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.CompressionCodec);

        Assert.NotNull(annotation);
        Assert.Equal("DoubleDelta, LZ4", annotation.Value);
    }

    [Fact]
    public void HasCodec_WithLevel_IncludesLevel()
    {
        using var context = CreateContext<FluentCodecContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentCodecEntity))!;
        var property = entityType.FindProperty(nameof(FluentCodecEntity.SensorId))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.CompressionCodec);

        Assert.NotNull(annotation);
        Assert.Equal("Delta, ZSTD(3)", annotation.Value);
    }

    [Fact]
    public void HasHighCompressionCodec_AppliesZstd9()
    {
        using var context = CreateContext<FluentCodecContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentCodecEntity))!;
        var property = entityType.FindProperty(nameof(FluentCodecEntity.RawPayload))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.CompressionCodec);

        Assert.NotNull(annotation);
        Assert.Equal("ZSTD(9)", annotation.Value);
    }

    [Fact]
    public void HasNoCompression_AppliesNone()
    {
        using var context = CreateContext<FluentCodecContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentCodecEntity))!;
        var property = entityType.FindProperty(nameof(FluentCodecEntity.UncompressedData))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.CompressionCodec);

        Assert.NotNull(annotation);
        Assert.Equal("NONE", annotation.Value);
    }

    #endregion

    #region Attribute Annotation Tests

    [Fact]
    public void TimestampCodecAttribute_SetsAnnotation()
    {
        using var context = CreateContext<AttributeCodecContext>();

        var entityType = context.Model.FindEntityType(typeof(AttributeCodecEntity))!;
        var property = entityType.FindProperty(nameof(AttributeCodecEntity.Timestamp))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.CompressionCodec);

        Assert.NotNull(annotation);
        Assert.Equal("DoubleDelta, LZ4", annotation.Value);
    }

    [Fact]
    public void SequentialCodecAttribute_SetsAnnotation()
    {
        using var context = CreateContext<AttributeCodecContext>();

        var entityType = context.Model.FindEntityType(typeof(AttributeCodecEntity))!;
        var property = entityType.FindProperty(nameof(AttributeCodecEntity.SensorId))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.CompressionCodec);

        Assert.NotNull(annotation);
        Assert.Equal("Delta, ZSTD", annotation.Value);
    }

    [Fact]
    public void FloatCodecAttribute_SetsAnnotation()
    {
        using var context = CreateContext<AttributeCodecContext>();

        var entityType = context.Model.FindEntityType(typeof(AttributeCodecEntity))!;
        var property = entityType.FindProperty(nameof(AttributeCodecEntity.Value))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.CompressionCodec);

        Assert.NotNull(annotation);
        Assert.Equal("Gorilla, ZSTD(1)", annotation.Value);
    }

    [Fact]
    public void HighCompressionCodecAttribute_SetsAnnotation()
    {
        using var context = CreateContext<AttributeCodecContext>();

        var entityType = context.Model.FindEntityType(typeof(AttributeCodecEntity))!;
        var property = entityType.FindProperty(nameof(AttributeCodecEntity.RawPayload))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.CompressionCodec);

        Assert.NotNull(annotation);
        Assert.Equal("ZSTD(9)", annotation.Value);
    }

    [Fact]
    public void NoCompressionAttribute_SetsAnnotation()
    {
        using var context = CreateContext<AttributeCodecContext>();

        var entityType = context.Model.FindEntityType(typeof(AttributeCodecEntity))!;
        var property = entityType.FindProperty(nameof(AttributeCodecEntity.UncompressedData))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.CompressionCodec);

        Assert.NotNull(annotation);
        Assert.Equal("NONE", annotation.Value);
    }

    [Fact]
    public void ClickHouseCodecAttribute_WithCustomSpec_SetsAnnotation()
    {
        using var context = CreateContext<AttributeCodecContext>();

        var entityType = context.Model.FindEntityType(typeof(AttributeCodecEntity))!;
        var property = entityType.FindProperty(nameof(AttributeCodecEntity.Counter))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.CompressionCodec);

        Assert.NotNull(annotation);
        Assert.Equal("Delta, ZSTD(5)", annotation.Value);
    }

    #endregion

    #region Fluent API Overrides Attribute Tests

    [Fact]
    public void FluentApi_OverridesAttribute()
    {
        using var context = CreateContext<OverrideCodecContext>();

        var entityType = context.Model.FindEntityType(typeof(OverrideCodecEntity))!;
        var property = entityType.FindProperty(nameof(OverrideCodecEntity.Data))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.CompressionCodec);

        Assert.NotNull(annotation);
        // Fluent API (LZ4) should override the attribute (ZSTD(9))
        Assert.Equal("LZ4", annotation.Value);
    }

    #endregion

    #region DDL Generation Tests

    [Fact]
    public void CreateTable_GeneratesCodecClause_ForFluentApi()
    {
        using var context = CreateContext<FluentCodecContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("\"Timestamp\" DateTime64(3) CODEC(DoubleDelta, LZ4)", script);
        Assert.Contains("\"SensorId\" Int64 CODEC(Delta, ZSTD(3))", script);
        Assert.Contains("\"Value\" Float64 CODEC(Gorilla, ZSTD(1))", script);
        Assert.Contains("\"RawPayload\" String CODEC(ZSTD(9))", script);
        // byte[] maps to Array(UInt8) in ClickHouse
        Assert.Contains("\"UncompressedData\" Array(UInt8) CODEC(NONE)", script);
    }

    [Fact]
    public void CreateTable_GeneratesCodecClause_ForAttributes()
    {
        using var context = CreateContext<AttributeCodecContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("CODEC(DoubleDelta, LZ4)", script);
        Assert.Contains("CODEC(Delta, ZSTD)", script);
        Assert.Contains("CODEC(Gorilla, ZSTD(1))", script);
        Assert.Contains("CODEC(ZSTD(9))", script);
        Assert.Contains("CODEC(NONE)", script);
        Assert.Contains("CODEC(Delta, ZSTD(5))", script);
    }

    #endregion

    #region CodecChainBuilder Validation Tests

    [Fact]
    public void ZSTD_InvalidLevel_ThrowsException()
    {
        var builder = new CodecChainBuilder();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.ZSTD(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.ZSTD(23));
    }

    [Fact]
    public void FPC_InvalidLevel_ThrowsException()
    {
        var builder = new CodecChainBuilder();

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.FPC(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.FPC(29));
    }

    [Fact]
    public void CodecChainBuilder_EmptyChain_ThrowsOnBuild()
    {
        var builder = new CodecChainBuilder();

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public void CodecChainBuilder_None_ClearsPreviousCodecs()
    {
        var builder = new CodecChainBuilder();
        builder.Delta().ZSTD(); // Add some codecs
        builder.None(); // Should clear them

        Assert.Equal("NONE", builder.Build());
    }

    #endregion

    #region Codec Static Helper Tests

    [Fact]
    public void Codec_ZstdLevel_ReturnsCorrectString()
    {
        Assert.Equal("ZSTD(1)", Codec.ZstdLevel(1));
        Assert.Equal("ZSTD(9)", Codec.ZstdLevel(9));
        Assert.Equal("ZSTD(22)", Codec.ZstdLevel(22));
    }

    [Fact]
    public void Codec_ZstdLevel_InvalidLevel_ThrowsException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => Codec.ZstdLevel(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => Codec.ZstdLevel(23));
    }

    [Fact]
    public void Codec_FpcLevel_ReturnsCorrectString()
    {
        Assert.Equal("FPC(1)", Codec.FpcLevel(1));
        Assert.Equal("FPC(12)", Codec.FpcLevel(12));
        Assert.Equal("FPC(28)", Codec.FpcLevel(28));
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
public class CompressionCodecIntegrationTests : IAsyncLifetime
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
    public async Task CreateTable_WithCodecs_ExecutesSuccessfully()
    {
        var options = new DbContextOptionsBuilder<FluentCodecContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new FluentCodecContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Table should be created successfully - verify by inserting a row
        context.Entities.Add(new FluentCodecEntity
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow,
            SensorId = 123,
            Value = 45.67,
            RawPayload = "test payload",
            UncompressedData = [1, 2, 3]
        });

        await context.SaveChangesAsync();

        var count = await context.Entities.CountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task CreateTable_WithAttributeCodecs_ExecutesSuccessfully()
    {
        var options = new DbContextOptionsBuilder<AttributeCodecContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new AttributeCodecContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Table should be created successfully - verify by inserting a row
        context.Entities.Add(new AttributeCodecEntity
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow,
            SensorId = 456,
            Value = 78.90,
            RawPayload = "test payload",
            UncompressedData = [4, 5, 6],
            Counter = 100
        });

        await context.SaveChangesAsync();

        var count = await context.Entities.CountAsync();
        Assert.Equal(1, count);
    }
}
