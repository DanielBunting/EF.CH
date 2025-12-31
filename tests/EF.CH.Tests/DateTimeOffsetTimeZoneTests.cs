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
/// Entity with DateTimeOffset properties configured via fluent API.
/// </summary>
public class FluentTimeZoneEntity
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;

    // Non-nullable DateTimeOffset with timezone
    public DateTimeOffset CreatedAt { get; set; }

    // Nullable DateTimeOffset with timezone
    public DateTimeOffset? ScheduledAt { get; set; }
}

/// <summary>
/// Entity with DateTimeOffset properties configured via attributes.
/// </summary>
public class AttributeTimeZoneEntity
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;

    [ClickHouseTimeZone("America/New_York")]
    public DateTimeOffset CreatedAt { get; set; }

    [ClickHouseTimeZone("Europe/London")]
    public DateTimeOffset? ScheduledAt { get; set; }
}

/// <summary>
/// Entity to test fluent API overriding attribute.
/// </summary>
public class OverrideTimeZoneEntity
{
    public Guid Id { get; set; }

    [ClickHouseTimeZone("America/New_York")] // Attribute
    public DateTimeOffset CreatedAt { get; set; }
}

/// <summary>
/// Entity with explicit column type instead of HasTimeZone.
/// </summary>
public class ExplicitColumnTypeEntity
{
    public Guid Id { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}

/// <summary>
/// Entity without timezone (uses UTC by default).
/// </summary>
public class DefaultTimeZoneEntity
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

            entity.Property(e => e.ScheduledAt)
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

            // Fluent API should override the attribute
            entity.Property(e => e.CreatedAt)
                .HasTimeZone("Europe/Paris"); // Different timezone
        });
    }
}

public class ExplicitColumnTypeContext(DbContextOptions<ExplicitColumnTypeContext> options) : DbContext(options)
{
    public DbSet<ExplicitColumnTypeEntity> Entities => Set<ExplicitColumnTypeEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ExplicitColumnTypeEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("explicit_column_type_entities");
            entity.UseMergeTree(x => x.Id);

            // Explicit column type takes precedence over HasTimeZone
            entity.Property(e => e.CreatedAt)
                .HasColumnType("DateTime64(3, 'Asia/Tokyo')");
        });
    }
}

public class DefaultTimeZoneContext(DbContextOptions<DefaultTimeZoneContext> options) : DbContext(options)
{
    public DbSet<DefaultTimeZoneEntity> Entities => Set<DefaultTimeZoneEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DefaultTimeZoneEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("default_timezone_entities");
            entity.UseMergeTree(x => x.Id);
        });
    }
}

#endregion

/// <summary>
/// Unit tests for DateTimeOffset timezone type mappings.
/// These tests don't require Docker/ClickHouse.
/// </summary>
public class DateTimeOffsetTimeZoneUnitTests
{
    #region Type Mapping Unit Tests

    [Fact]
    public void DateTimeOffsetMapping_WithoutTimezone_DefaultsToUTC()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping();

        Assert.Equal("DateTime64(3, 'UTC')", mapping.StoreType);
        Assert.Equal(typeof(DateTimeOffset), mapping.ClrType);
        Assert.Null(mapping.TimeZone);
        Assert.Equal(3, mapping.Precision);
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

    #endregion

    #region Value Conversion Unit Tests

    [Fact]
    public void ValueConverter_WritesAsUtc()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "America/New_York");
        var converter = mapping.Converter!;

        // DateTimeOffset in Eastern Time (UTC-5)
        var input = new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(-5));

        var result = (DateTime)converter.ConvertToProvider(input)!;

        // Should be converted to UTC
        Assert.Equal(DateTimeKind.Utc, result.Kind);
        Assert.Equal(new DateTime(2024, 1, 15, 15, 30, 0, DateTimeKind.Utc), result);
    }

    [Fact]
    public void ValueConverter_ReadsWithTimezoneOffset_Winter()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "America/New_York");
        var converter = mapping.Converter!;

        // UTC DateTime (15:30 UTC = 10:30 EST in winter)
        var utcDateTime = new DateTime(2024, 1, 15, 15, 30, 0, DateTimeKind.Utc);

        var result = (DateTimeOffset)converter.ConvertFromProvider(utcDateTime)!;

        // Should have Eastern Time offset (-5:00 in winter)
        Assert.Equal(TimeSpan.FromHours(-5), result.Offset);
        Assert.Equal(10, result.Hour);
        Assert.Equal(30, result.Minute);
    }

    [Fact]
    public void ValueConverter_ReadsWithTimezoneOffset_Summer()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "America/New_York");
        var converter = mapping.Converter!;

        // UTC DateTime (15:30 UTC = 11:30 EDT in summer due to DST)
        var utcDateTime = new DateTime(2024, 7, 15, 15, 30, 0, DateTimeKind.Utc);

        var result = (DateTimeOffset)converter.ConvertFromProvider(utcDateTime)!;

        // Should have Eastern Daylight Time offset (-4:00 in summer)
        Assert.Equal(TimeSpan.FromHours(-4), result.Offset);
        Assert.Equal(11, result.Hour);
        Assert.Equal(30, result.Minute);
    }

    [Fact]
    public void ValueConverter_EuropeLondon_HandlesBST()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "Europe/London");
        var converter = mapping.Converter!;

        // Test winter (GMT, no offset)
        var winterUtc = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);
        var winterResult = (DateTimeOffset)converter.ConvertFromProvider(winterUtc)!;
        Assert.Equal(TimeSpan.Zero, winterResult.Offset);

        // Test summer (BST, +1 offset)
        var summerUtc = new DateTime(2024, 7, 15, 12, 0, 0, DateTimeKind.Utc);
        var summerResult = (DateTimeOffset)converter.ConvertFromProvider(summerUtc)!;
        Assert.Equal(TimeSpan.FromHours(1), summerResult.Offset);
    }

    [Fact]
    public void ValueConverter_Utc_ReturnsZeroOffset()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "UTC");
        var converter = mapping.Converter!;

        var utcDateTime = new DateTime(2024, 7, 15, 15, 30, 0, DateTimeKind.Utc);
        var result = (DateTimeOffset)converter.ConvertFromProvider(utcDateTime)!;

        Assert.Equal(TimeSpan.Zero, result.Offset);
    }

    [Fact]
    public void ValueConverter_NullTimezone_TreatedAsUtc()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, null);
        var converter = mapping.Converter!;

        var utcDateTime = new DateTime(2024, 7, 15, 15, 30, 0, DateTimeKind.Utc);
        var result = (DateTimeOffset)converter.ConvertFromProvider(utcDateTime)!;

        Assert.Equal(TimeSpan.Zero, result.Offset);
    }

    #endregion

    #region Fluent API Tests

    [Fact]
    public void HasTimeZone_SetsAnnotation()
    {
        var options = new DbContextOptionsBuilder<FluentTimeZoneContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new FluentTimeZoneContext(options);
        var entityType = context.Model.FindEntityType(typeof(FluentTimeZoneEntity))!;

        var createdAtProperty = entityType.FindProperty(nameof(FluentTimeZoneEntity.CreatedAt))!;
        var timeZone = createdAtProperty.FindAnnotation(ClickHouseAnnotationNames.TimeZone)?.Value;

        Assert.Equal("America/New_York", timeZone);
    }

    [Fact]
    public void HasTimeZone_Nullable_SetsAnnotation()
    {
        var options = new DbContextOptionsBuilder<FluentTimeZoneContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new FluentTimeZoneContext(options);
        var entityType = context.Model.FindEntityType(typeof(FluentTimeZoneEntity))!;

        var scheduledAtProperty = entityType.FindProperty(nameof(FluentTimeZoneEntity.ScheduledAt))!;
        var timeZone = scheduledAtProperty.FindAnnotation(ClickHouseAnnotationNames.TimeZone)?.Value;

        Assert.Equal("Europe/London", timeZone);
    }

    [Fact]
    public void HasTimeZone_SetsColumnType()
    {
        var options = new DbContextOptionsBuilder<FluentTimeZoneContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new FluentTimeZoneContext(options);
        var entityType = context.Model.FindEntityType(typeof(FluentTimeZoneEntity))!;

        var createdAtProperty = entityType.FindProperty(nameof(FluentTimeZoneEntity.CreatedAt))!;
        var columnType = createdAtProperty.GetColumnType();

        Assert.Equal("DateTime64(3, 'America/New_York')", columnType);
    }

    [Fact]
    public void HasTimeZone_Nullable_SetsNullableColumnType()
    {
        var options = new DbContextOptionsBuilder<FluentTimeZoneContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new FluentTimeZoneContext(options);
        var entityType = context.Model.FindEntityType(typeof(FluentTimeZoneEntity))!;

        var scheduledAtProperty = entityType.FindProperty(nameof(FluentTimeZoneEntity.ScheduledAt))!;
        var columnType = scheduledAtProperty.GetColumnType();

        Assert.Equal("Nullable(DateTime64(3, 'Europe/London'))", columnType);
    }

    #endregion

    #region Attribute Tests

    [Fact]
    public void ClickHouseTimeZoneAttribute_SetsAnnotation()
    {
        var options = new DbContextOptionsBuilder<AttributeTimeZoneContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new AttributeTimeZoneContext(options);
        var entityType = context.Model.FindEntityType(typeof(AttributeTimeZoneEntity))!;

        var createdAtProperty = entityType.FindProperty(nameof(AttributeTimeZoneEntity.CreatedAt))!;
        var timeZone = createdAtProperty.FindAnnotation(ClickHouseAnnotationNames.TimeZone)?.Value;

        Assert.Equal("America/New_York", timeZone);
    }

    [Fact]
    public void ClickHouseTimeZoneAttribute_Nullable_SetsAnnotation()
    {
        var options = new DbContextOptionsBuilder<AttributeTimeZoneContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new AttributeTimeZoneContext(options);
        var entityType = context.Model.FindEntityType(typeof(AttributeTimeZoneEntity))!;

        var scheduledAtProperty = entityType.FindProperty(nameof(AttributeTimeZoneEntity.ScheduledAt))!;
        var timeZone = scheduledAtProperty.FindAnnotation(ClickHouseAnnotationNames.TimeZone)?.Value;

        Assert.Equal("Europe/London", timeZone);
    }

    [Fact]
    public void FluentApi_OverridesAttribute()
    {
        var options = new DbContextOptionsBuilder<OverrideTimeZoneContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new OverrideTimeZoneContext(options);
        var entityType = context.Model.FindEntityType(typeof(OverrideTimeZoneEntity))!;

        var createdAtProperty = entityType.FindProperty(nameof(OverrideTimeZoneEntity.CreatedAt))!;
        var timeZone = createdAtProperty.FindAnnotation(ClickHouseAnnotationNames.TimeZone)?.Value;

        // Fluent API should win over attribute
        Assert.Equal("Europe/Paris", timeZone);
    }

    [Fact]
    public void ExplicitColumnType_TakesPrecedence()
    {
        var options = new DbContextOptionsBuilder<ExplicitColumnTypeContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new ExplicitColumnTypeContext(options);
        var entityType = context.Model.FindEntityType(typeof(ExplicitColumnTypeEntity))!;

        var createdAtProperty = entityType.FindProperty(nameof(ExplicitColumnTypeEntity.CreatedAt))!;
        var columnType = createdAtProperty.GetColumnType();

        // Explicit column type should be preserved
        Assert.Equal("DateTime64(3, 'Asia/Tokyo')", columnType);
    }

    #endregion

    #region Default Behavior Tests

    [Fact]
    public void DefaultTimeZone_UsesUtc()
    {
        var options = new DbContextOptionsBuilder<DefaultTimeZoneContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new DefaultTimeZoneContext(options);
        var entityType = context.Model.FindEntityType(typeof(DefaultTimeZoneEntity))!;

        var createdAtProperty = entityType.FindProperty(nameof(DefaultTimeZoneEntity.CreatedAt))!;
        var timeZone = createdAtProperty.FindAnnotation(ClickHouseAnnotationNames.TimeZone)?.Value;

        // Should have no timezone annotation (uses default)
        Assert.Null(timeZone);
    }

    #endregion
}

/// <summary>
/// Integration tests for DateTimeOffset timezone support.
/// These tests require Docker/ClickHouse.
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

    private string GetConnectionString() => _container.GetConnectionString();

    [Fact]
    public async Task CanCreateTable_WithTimezoneColumn()
    {
        var options = new DbContextOptionsBuilder<FluentTimeZoneContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        await using var context = new FluentTimeZoneContext(options);

        // Create table
        await context.Database.EnsureCreatedAsync();

        // Verify table was created by querying its structure
        var columns = await context.Database.SqlQueryRaw<string>(
            @"SELECT name, type FROM system.columns WHERE table = 'fluent_timezone_entities' AND database = currentDatabase() ORDER BY position"
        ).ToListAsync();

        Assert.Contains(columns, c => c.Contains("CreatedAt") || c.Contains("DateTime64(3, 'America/New_York')"));
    }

    [Fact]
    public async Task RoundTrip_WithTimezone_PreservesInstant()
    {
        var options = new DbContextOptionsBuilder<FluentTimeZoneContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        await using var context = new FluentTimeZoneContext(options);
        await context.Database.EnsureCreatedAsync();

        // Insert data with specific DateTimeOffset
        var id = Guid.NewGuid();
        var originalDto = new DateTimeOffset(2024, 7, 15, 10, 30, 0, TimeSpan.FromHours(-4)); // EDT

        var entity = new FluentTimeZoneEntity
        {
            Id = id,
            Name = "Test Entity",
            CreatedAt = originalDto,
            ScheduledAt = null
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();

        // Read back
        context.ChangeTracker.Clear();
        var retrieved = await context.Entities.FindAsync(id);

        Assert.NotNull(retrieved);

        // The instant (UTC time) should be preserved
        // Original: 2024-07-15 10:30:00 EDT (UTC-4) = 2024-07-15 14:30:00 UTC
        // When read with America/New_York timezone in summer, offset should be -4
        Assert.Equal(originalDto.UtcDateTime, retrieved.CreatedAt.UtcDateTime);
        Assert.Equal(TimeSpan.FromHours(-4), retrieved.CreatedAt.Offset); // EDT in summer
    }

    [Fact]
    public async Task Read_WithNewYorkTimezone_ReturnsCorrectOffset_Winter()
    {
        var options = new DbContextOptionsBuilder<FluentTimeZoneContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        await using var context = new FluentTimeZoneContext(options);
        await context.Database.EnsureCreatedAsync();

        // Insert a winter date (January - EST, UTC-5)
        var id = Guid.NewGuid();
        var winterDto = new DateTimeOffset(2024, 1, 15, 10, 0, 0, TimeSpan.FromHours(-5)); // EST

        var entity = new FluentTimeZoneEntity
        {
            Id = id,
            Name = "Winter Entity",
            CreatedAt = winterDto,
            ScheduledAt = null
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();

        // Read back
        context.ChangeTracker.Clear();
        var retrieved = await context.Entities.FindAsync(id);

        Assert.NotNull(retrieved);
        Assert.Equal(TimeSpan.FromHours(-5), retrieved.CreatedAt.Offset); // EST in winter
    }

    [Fact]
    public async Task Read_WithNewYorkTimezone_ReturnsCorrectOffset_Summer()
    {
        var options = new DbContextOptionsBuilder<FluentTimeZoneContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        await using var context = new FluentTimeZoneContext(options);
        await context.Database.EnsureCreatedAsync();

        // Insert a summer date (July - EDT, UTC-4)
        var id = Guid.NewGuid();
        var summerDto = new DateTimeOffset(2024, 7, 15, 10, 0, 0, TimeSpan.FromHours(-4)); // EDT

        var entity = new FluentTimeZoneEntity
        {
            Id = id,
            Name = "Summer Entity",
            CreatedAt = summerDto,
            ScheduledAt = null
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();

        // Read back
        context.ChangeTracker.Clear();
        var retrieved = await context.Entities.FindAsync(id);

        Assert.NotNull(retrieved);
        Assert.Equal(TimeSpan.FromHours(-4), retrieved.CreatedAt.Offset); // EDT in summer
    }

    [Fact]
    public async Task AttributeConfiguration_WorksInIntegration()
    {
        var options = new DbContextOptionsBuilder<AttributeTimeZoneContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        await using var context = new AttributeTimeZoneContext(options);
        await context.Database.EnsureCreatedAsync();

        // Insert data
        var id = Guid.NewGuid();
        var entity = new AttributeTimeZoneEntity
        {
            Id = id,
            Name = "Attribute Entity",
            CreatedAt = new DateTimeOffset(2024, 7, 15, 10, 0, 0, TimeSpan.FromHours(-4)),
            ScheduledAt = new DateTimeOffset(2024, 7, 15, 15, 0, 0, TimeSpan.FromHours(1)) // BST
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();

        // Read back
        context.ChangeTracker.Clear();
        var retrieved = await context.Entities.FindAsync(id);

        Assert.NotNull(retrieved);

        // CreatedAt should have New York offset
        Assert.Equal(TimeSpan.FromHours(-4), retrieved.CreatedAt.Offset);

        // ScheduledAt should have London BST offset
        Assert.NotNull(retrieved.ScheduledAt);
        Assert.Equal(TimeSpan.FromHours(1), retrieved.ScheduledAt.Value.Offset);
    }
}
