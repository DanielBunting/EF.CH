using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Metadata.Attributes;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests;

#region Test Entities

/// <summary>
/// Entity with timezone configured via fluent API.
/// </summary>
public class FluentTimeZoneEntity
{
    public Guid Id { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? UpdatedAt { get; set; }
}

/// <summary>
/// Entity with timezone configured via attribute.
/// </summary>
public class AttributeTimeZoneEntity
{
    public Guid Id { get; set; }

    [ClickHouseTimeZone("America/New_York")]
    public DateTimeOffset CreatedAt { get; set; }

    [ClickHouseTimeZone("Europe/London")]
    public DateTimeOffset? UpdatedAt { get; set; }
}

/// <summary>
/// Entity to test fluent API overriding attribute.
/// </summary>
public class OverrideTimeZoneEntity
{
    public Guid Id { get; set; }

    [ClickHouseTimeZone("America/New_York")] // Attribute says New York
    public DateTimeOffset CreatedAt { get; set; }
}

/// <summary>
/// Entity with timezone configured via column type.
/// </summary>
public class ColumnTypeTimeZoneEntity
{
    public Guid Id { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}

#endregion

#region Test DbContexts

public class FluentTimeZoneContext(DbContextOptions<FluentTimeZoneContext> options) : DbContext(options)
{
    public DbSet<FluentTimeZoneEntity> Entities => Set<FluentTimeZoneEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FluentTimeZoneEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("fluent_timezone_entities");
            entity.UseMergeTree(x => x.Id);

            entity.Property(e => e.CreatedAt)
                .HasTimeZone("America/New_York");

            entity.Property(e => e.UpdatedAt)
                .HasTimeZone("Europe/London");
        });
    }
}

public class AttributeTimeZoneContext(DbContextOptions<AttributeTimeZoneContext> options) : DbContext(options)
{
    public DbSet<AttributeTimeZoneEntity> Entities => Set<AttributeTimeZoneEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AttributeTimeZoneEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("attribute_timezone_entities");
            entity.UseMergeTree(x => x.Id);
        });
    }
}

public class OverrideTimeZoneContext(DbContextOptions<OverrideTimeZoneContext> options) : DbContext(options)
{
    public DbSet<OverrideTimeZoneEntity> Entities => Set<OverrideTimeZoneEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OverrideTimeZoneEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("override_timezone_entities");
            entity.UseMergeTree(x => x.Id);

            // Fluent API should override the [ClickHouseTimeZone("America/New_York")] attribute
            entity.Property(e => e.CreatedAt)
                .HasTimeZone("Asia/Tokyo");
        });
    }
}

public class ColumnTypeTimeZoneContext(DbContextOptions<ColumnTypeTimeZoneContext> options) : DbContext(options)
{
    public DbSet<ColumnTypeTimeZoneEntity> Entities => Set<ColumnTypeTimeZoneEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ColumnTypeTimeZoneEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("column_type_timezone_entities");
            entity.UseMergeTree(x => x.Id);

            // Using HasColumnType directly
            entity.Property(e => e.CreatedAt)
                .HasColumnType("DateTime64(3, 'America/Los_Angeles')");
        });
    }
}

#endregion

public class DateTimeOffsetTimeZoneTests
{
    #region Type Mapping Unit Tests

    [Fact]
    public void DateTimeOffsetMapping_WithoutTimezone_DefaultsToUTC()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping();

        Assert.Equal("DateTime64(3, 'UTC')", mapping.StoreType);
        Assert.Equal(typeof(DateTimeOffset), mapping.ClrType);
        Assert.Null(mapping.TimeZone);
    }

    [Fact]
    public void DateTimeOffsetMapping_WithTimezone_IncludesTimezone()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "America/New_York");

        Assert.Equal("DateTime64(3, 'America/New_York')", mapping.StoreType);
        Assert.Equal(typeof(DateTimeOffset), mapping.ClrType);
        Assert.Equal("America/New_York", mapping.TimeZone);
    }

    [Fact]
    public void DateTimeOffsetMapping_WithPrecision_IncludesPrecision()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(6, "Europe/London");

        Assert.Equal("DateTime64(6, 'Europe/London')", mapping.StoreType);
        Assert.Equal(6, mapping.Precision);
    }

    [Fact]
    public void DateTimeOffsetMapping_GeneratesUtcLiteral()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "America/New_York");

        // Create a DateTimeOffset in Eastern time
        var eastern = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
        var localTime = new DateTime(2024, 7, 15, 14, 30, 0); // 2:30 PM
        var offset = eastern.GetUtcOffset(localTime);
        var dto = new DateTimeOffset(localTime, offset);

        var literal = mapping.GenerateSqlLiteral(dto);

        // Should generate UTC time (18:30 in summer when Eastern is -4)
        Assert.Contains("18:30:00", literal);
    }

    #endregion

    #region Value Conversion Unit Tests

    [Fact]
    public void ValueConverter_WritesAsUtc()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "America/New_York");
        var converter = mapping.Converter!;

        // Create a DateTimeOffset with +05:30 offset (IST)
        var dto = new DateTimeOffset(2024, 7, 15, 10, 0, 0, TimeSpan.FromHours(5.5));

        var result = converter.ConvertToProvider(dto);

        Assert.IsType<DateTime>(result);
        var utcDateTime = (DateTime)result!;

        // 10:00 IST (+5:30) = 04:30 UTC
        Assert.Equal(4, utcDateTime.Hour);
        Assert.Equal(30, utcDateTime.Minute);
    }

    [Fact]
    public void ValueConverter_ReadsWithTimezoneOffset_Summer()
    {
        // America/New_York in summer is UTC-4 (EDT)
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "America/New_York");
        var converter = mapping.Converter!;

        // UTC time in July (summer)
        var utcDateTime = new DateTime(2024, 7, 15, 18, 0, 0, DateTimeKind.Utc);

        var result = converter.ConvertFromProvider(utcDateTime);

        Assert.IsType<DateTimeOffset>(result);
        var dto = (DateTimeOffset)result!;

        // 18:00 UTC = 14:00 EDT (UTC-4)
        Assert.Equal(14, dto.Hour);
        Assert.Equal(TimeSpan.FromHours(-4), dto.Offset);
    }

    [Fact]
    public void ValueConverter_ReadsWithTimezoneOffset_Winter()
    {
        // America/New_York in winter is UTC-5 (EST)
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "America/New_York");
        var converter = mapping.Converter!;

        // UTC time in January (winter)
        var utcDateTime = new DateTime(2024, 1, 15, 18, 0, 0, DateTimeKind.Utc);

        var result = converter.ConvertFromProvider(utcDateTime);

        Assert.IsType<DateTimeOffset>(result);
        var dto = (DateTimeOffset)result!;

        // 18:00 UTC = 13:00 EST (UTC-5)
        Assert.Equal(13, dto.Hour);
        Assert.Equal(TimeSpan.FromHours(-5), dto.Offset);
    }

    [Fact]
    public void ValueConverter_UTC_ReturnsZeroOffset()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "UTC");
        var converter = mapping.Converter!;

        var utcDateTime = new DateTime(2024, 7, 15, 12, 0, 0, DateTimeKind.Utc);

        var result = converter.ConvertFromProvider(utcDateTime);

        Assert.IsType<DateTimeOffset>(result);
        var dto = (DateTimeOffset)result!;

        Assert.Equal(TimeSpan.Zero, dto.Offset);
    }

    [Fact]
    public void ValueConverter_EuropeLondon_HandlesBST()
    {
        // Europe/London in summer is UTC+1 (BST)
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "Europe/London");
        var converter = mapping.Converter!;

        // UTC time in July (summer)
        var utcDateTime = new DateTime(2024, 7, 15, 12, 0, 0, DateTimeKind.Utc);

        var result = converter.ConvertFromProvider(utcDateTime);

        Assert.IsType<DateTimeOffset>(result);
        var dto = (DateTimeOffset)result!;

        // 12:00 UTC = 13:00 BST (UTC+1)
        Assert.Equal(13, dto.Hour);
        Assert.Equal(TimeSpan.FromHours(1), dto.Offset);
    }

    #endregion

    #region Fluent API Annotation Tests

    [Fact]
    public void HasTimeZone_SetsAnnotation()
    {
        using var context = CreateContext<FluentTimeZoneContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentTimeZoneEntity))!;
        var property = entityType.FindProperty(nameof(FluentTimeZoneEntity.CreatedAt))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.TimeZone);

        Assert.NotNull(annotation);
        Assert.Equal("America/New_York", annotation.Value);
    }

    [Fact]
    public void HasTimeZone_Nullable_SetsAnnotation()
    {
        using var context = CreateContext<FluentTimeZoneContext>();

        var entityType = context.Model.FindEntityType(typeof(FluentTimeZoneEntity))!;
        var property = entityType.FindProperty(nameof(FluentTimeZoneEntity.UpdatedAt))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.TimeZone);

        Assert.NotNull(annotation);
        Assert.Equal("Europe/London", annotation.Value);
    }

    #endregion

    #region Attribute Annotation Tests

    [Fact]
    public void ClickHouseTimeZoneAttribute_SetsAnnotation()
    {
        using var context = CreateContext<AttributeTimeZoneContext>();

        var entityType = context.Model.FindEntityType(typeof(AttributeTimeZoneEntity))!;
        var property = entityType.FindProperty(nameof(AttributeTimeZoneEntity.CreatedAt))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.TimeZone);

        Assert.NotNull(annotation);
        Assert.Equal("America/New_York", annotation.Value);
    }

    [Fact]
    public void ClickHouseTimeZoneAttribute_Nullable_SetsAnnotation()
    {
        using var context = CreateContext<AttributeTimeZoneContext>();

        var entityType = context.Model.FindEntityType(typeof(AttributeTimeZoneEntity))!;
        var property = entityType.FindProperty(nameof(AttributeTimeZoneEntity.UpdatedAt))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.TimeZone);

        Assert.NotNull(annotation);
        Assert.Equal("Europe/London", annotation.Value);
    }

    #endregion

    #region Fluent API Overrides Attribute Tests

    [Fact]
    public void FluentApi_OverridesAttribute()
    {
        using var context = CreateContext<OverrideTimeZoneContext>();

        var entityType = context.Model.FindEntityType(typeof(OverrideTimeZoneEntity))!;
        var property = entityType.FindProperty(nameof(OverrideTimeZoneEntity.CreatedAt))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.TimeZone);

        Assert.NotNull(annotation);
        // Fluent API (Asia/Tokyo) should override the attribute (America/New_York)
        Assert.Equal("Asia/Tokyo", annotation.Value);
    }

    #endregion

    #region DDL Generation Tests

    [Fact]
    public void CreateTable_GeneratesTimezoneColumnType_ForFluentApi()
    {
        using var context = CreateContext<FluentTimeZoneContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("\"CreatedAt\" DateTime64(3, 'America/New_York')", script);
        Assert.Contains("\"UpdatedAt\" Nullable(DateTime64(3, 'Europe/London'))", script);
    }

    [Fact]
    public void CreateTable_GeneratesTimezoneColumnType_ForAttributes()
    {
        using var context = CreateContext<AttributeTimeZoneContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("DateTime64(3, 'America/New_York')", script);
        Assert.Contains("Nullable(DateTime64(3, 'Europe/London'))", script);
    }

    [Fact]
    public void CreateTable_GeneratesTimezoneColumnType_ForColumnType()
    {
        using var context = CreateContext<ColumnTypeTimeZoneContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("DateTime64(3, 'America/Los_Angeles')", script);
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
public class DateTimeOffsetTimeZoneIntegrationTests : IAsyncLifetime
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
    public async Task RoundTrip_WithTimezone_PreservesInstant()
    {
        var options = new DbContextOptionsBuilder<FluentTimeZoneContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new FluentTimeZoneContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Create a specific instant in time
        var utcNow = DateTime.UtcNow;
        var originalDto = new DateTimeOffset(utcNow, TimeSpan.Zero);

        var entity = new FluentTimeZoneEntity
        {
            Id = Guid.NewGuid(),
            CreatedAt = originalDto,
            UpdatedAt = originalDto
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();

        // Clear the context to force a fresh read
        context.ChangeTracker.Clear();

        var retrieved = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        // The instant should be preserved (same UtcDateTime)
        Assert.Equal(originalDto.UtcDateTime, retrieved.CreatedAt.UtcDateTime, TimeSpan.FromMilliseconds(1));
        Assert.Equal(originalDto.UtcDateTime, retrieved.UpdatedAt!.Value.UtcDateTime, TimeSpan.FromMilliseconds(1));
    }

    [Fact]
    public async Task Read_WithNewYorkTimezone_ReturnsCorrectOffset_Summer()
    {
        var options = new DbContextOptionsBuilder<FluentTimeZoneContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new FluentTimeZoneContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert a known UTC time in summer (when New York is UTC-4)
        var summerUtc = new DateTime(2024, 7, 15, 18, 0, 0, DateTimeKind.Utc);
        var entity = new FluentTimeZoneEntity
        {
            Id = Guid.NewGuid(),
            CreatedAt = new DateTimeOffset(summerUtc, TimeSpan.Zero)
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var retrieved = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        // Should return with New York summer offset (EDT = UTC-4)
        Assert.Equal(TimeSpan.FromHours(-4), retrieved.CreatedAt.Offset);
        Assert.Equal(14, retrieved.CreatedAt.Hour); // 18:00 UTC = 14:00 EDT
    }

    [Fact]
    public async Task Read_WithNewYorkTimezone_ReturnsCorrectOffset_Winter()
    {
        var options = new DbContextOptionsBuilder<FluentTimeZoneContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new FluentTimeZoneContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert a known UTC time in winter (when New York is UTC-5)
        var winterUtc = new DateTime(2024, 1, 15, 18, 0, 0, DateTimeKind.Utc);
        var entity = new FluentTimeZoneEntity
        {
            Id = Guid.NewGuid(),
            CreatedAt = new DateTimeOffset(winterUtc, TimeSpan.Zero)
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var retrieved = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        // Should return with New York winter offset (EST = UTC-5)
        Assert.Equal(TimeSpan.FromHours(-5), retrieved.CreatedAt.Offset);
        Assert.Equal(13, retrieved.CreatedAt.Hour); // 18:00 UTC = 13:00 EST
    }

    [Fact]
    public async Task Read_WithLondonTimezone_HandlesBST()
    {
        var options = new DbContextOptionsBuilder<FluentTimeZoneContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new FluentTimeZoneContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert a known UTC time in summer (when London is UTC+1 BST)
        var summerUtc = new DateTime(2024, 7, 15, 12, 0, 0, DateTimeKind.Utc);
        var entity = new FluentTimeZoneEntity
        {
            Id = Guid.NewGuid(),
            CreatedAt = DateTimeOffset.UtcNow, // Placeholder
            UpdatedAt = new DateTimeOffset(summerUtc, TimeSpan.Zero)
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var retrieved = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        // UpdatedAt is configured with Europe/London timezone
        // Should return with London summer offset (BST = UTC+1)
        Assert.Equal(TimeSpan.FromHours(1), retrieved.UpdatedAt!.Value.Offset);
        Assert.Equal(13, retrieved.UpdatedAt.Value.Hour); // 12:00 UTC = 13:00 BST
    }

    [Fact]
    public async Task CreateTable_WithAttributes_ExecutesSuccessfully()
    {
        var options = new DbContextOptionsBuilder<AttributeTimeZoneContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new AttributeTimeZoneContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Table should be created successfully - verify by inserting a row
        context.Entities.Add(new AttributeTimeZoneEntity
        {
            Id = Guid.NewGuid(),
            CreatedAt = DateTimeOffset.UtcNow,
            UpdatedAt = DateTimeOffset.UtcNow
        });

        await context.SaveChangesAsync();

        var count = await context.Entities.CountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task NullableDateTimeOffset_WithTimezone_HandlesNull()
    {
        var options = new DbContextOptionsBuilder<FluentTimeZoneContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new FluentTimeZoneContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var entity = new FluentTimeZoneEntity
        {
            Id = Guid.NewGuid(),
            CreatedAt = DateTimeOffset.UtcNow,
            UpdatedAt = null
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var retrieved = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        Assert.Null(retrieved.UpdatedAt);
    }
}
