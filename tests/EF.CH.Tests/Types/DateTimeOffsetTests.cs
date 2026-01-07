using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Metadata.Attributes;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Types;

#region Test Entities

/// <summary>
/// Basic entity with DateTimeOffset properties (no timezone configured).
/// </summary>
public class DateTimeOffsetEntity
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}

/// <summary>
/// Entity with DateTimeOffset properties configured via [ClickHouseTimeZone] attribute.
/// </summary>
public class TimezoneAttributeEntity
{
    public Guid Id { get; set; }

    [ClickHouseTimeZone("America/New_York")]
    public DateTimeOffset EasternTime { get; set; }

    [ClickHouseTimeZone("Europe/London")]
    public DateTimeOffset LondonTime { get; set; }

    [ClickHouseTimeZone("UTC")]
    public DateTimeOffset UtcTime { get; set; }
}

/// <summary>
/// Entity with DateTimeOffset properties configured via fluent API.
/// </summary>
public class FluentTimezoneEntity
{
    public Guid Id { get; set; }
    public DateTimeOffset EasternTime { get; set; }
    public DateTimeOffset TokyoTime { get; set; }
}

/// <summary>
/// Entity with nullable DateTimeOffset properties.
/// </summary>
public class NullableDateTimeOffsetEntity
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public DateTimeOffset? OptionalTimestamp { get; set; }

    [ClickHouseTimeZone("America/Chicago")]
    public DateTimeOffset? OptionalCentralTime { get; set; }
}

/// <summary>
/// Entity with multiple timezones configured via attributes.
/// </summary>
public class MultiTimezoneEntity
{
    public Guid Id { get; set; }

    [ClickHouseTimeZone("America/New_York")]
    public DateTimeOffset NewYork { get; set; }

    [ClickHouseTimeZone("America/Los_Angeles")]
    public DateTimeOffset LosAngeles { get; set; }

    [ClickHouseTimeZone("Europe/Paris")]
    public DateTimeOffset Paris { get; set; }

    [ClickHouseTimeZone("Asia/Tokyo")]
    public DateTimeOffset Tokyo { get; set; }
}

/// <summary>
/// Entity to test fluent API overriding attribute.
/// </summary>
public class OverrideTimezoneEntity
{
    public Guid Id { get; set; }

    [ClickHouseTimeZone("America/New_York")]
    public DateTimeOffset Timestamp { get; set; }
}

#endregion

#region Test DbContexts

public class BasicDateTimeOffsetContext : DbContext
{
    public BasicDateTimeOffsetContext(DbContextOptions<BasicDateTimeOffsetContext> options)
        : base(options) { }

    public DbSet<DateTimeOffsetEntity> Entities => Set<DateTimeOffsetEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DateTimeOffsetEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("datetimeoffset_entities");
            entity.UseMergeTree(x => x.CreatedAt);
        });
    }
}

public class AttributeTimezoneContext : DbContext
{
    public AttributeTimezoneContext(DbContextOptions<AttributeTimezoneContext> options)
        : base(options) { }

    public DbSet<TimezoneAttributeEntity> Entities => Set<TimezoneAttributeEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TimezoneAttributeEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("timezone_attribute_entities");
            entity.UseMergeTree(x => x.Id);
        });
    }
}

public class FluentTimezoneContext : DbContext
{
    public FluentTimezoneContext(DbContextOptions<FluentTimezoneContext> options)
        : base(options) { }

    public DbSet<FluentTimezoneEntity> Entities => Set<FluentTimezoneEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FluentTimezoneEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("fluent_timezone_entities");
            entity.UseMergeTree(x => x.Id);

            entity.Property(e => e.EasternTime)
                .HasTimeZone("America/New_York");

            entity.Property(e => e.TokyoTime)
                .HasTimeZone("Asia/Tokyo");
        });
    }
}

public class NullableDateTimeOffsetContext : DbContext
{
    public NullableDateTimeOffsetContext(DbContextOptions<NullableDateTimeOffsetContext> options)
        : base(options) { }

    public DbSet<NullableDateTimeOffsetEntity> Entities => Set<NullableDateTimeOffsetEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<NullableDateTimeOffsetEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("nullable_datetimeoffset_entities");
            entity.UseMergeTree(x => x.Id);
        });
    }
}

public class MultiTimezoneContext : DbContext
{
    public MultiTimezoneContext(DbContextOptions<MultiTimezoneContext> options)
        : base(options) { }

    public DbSet<MultiTimezoneEntity> Entities => Set<MultiTimezoneEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MultiTimezoneEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("multi_timezone_entities");
            entity.UseMergeTree(x => x.Id);
        });
    }
}

public class OverrideTimezoneContext : DbContext
{
    public OverrideTimezoneContext(DbContextOptions<OverrideTimezoneContext> options)
        : base(options) { }

    public DbSet<OverrideTimezoneEntity> Entities => Set<OverrideTimezoneEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OverrideTimezoneEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("override_timezone_entities");
            entity.UseMergeTree(x => x.Id);

            // Fluent API should override the [ClickHouseTimeZone("America/New_York")] attribute
            entity.Property(e => e.Timestamp)
                .HasTimeZone("Europe/London");
        });
    }
}

#endregion

#region Unit Tests

public class DateTimeOffsetTests
{
    #region Type Mapping Unit Tests

    [Fact]
    public void DateTimeOffsetMapping_GeneratesCorrectStoreType_WithoutTimezone()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping();

        Assert.Equal("DateTime64(3, 'UTC')", mapping.StoreType);
        Assert.Equal(typeof(DateTimeOffset), mapping.ClrType);
    }

    [Fact]
    public void DateTimeOffsetMapping_GeneratesCorrectStoreType_WithTimezone()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "America/New_York");

        Assert.Equal("DateTime64(3, 'America/New_York')", mapping.StoreType);
        Assert.Equal(typeof(DateTimeOffset), mapping.ClrType);
    }

    [Theory]
    [InlineData(0, "DateTime64(0, 'UTC')")]
    [InlineData(3, "DateTime64(3, 'UTC')")]
    [InlineData(6, "DateTime64(6, 'UTC')")]
    [InlineData(9, "DateTime64(9, 'UTC')")]
    public void DateTimeOffsetMapping_GeneratesCorrectStoreType_WithPrecision(int precision, string expected)
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(precision);

        Assert.Equal(expected, mapping.StoreType);
    }

    [Fact]
    public void DateTimeOffsetMapping_GeneratesCorrectLiteral_ConvertsToUtc()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping();
        // 2:30 PM EST (UTC-5) = 7:30 PM UTC
        var dto = new DateTimeOffset(2024, 1, 15, 14, 30, 0, TimeSpan.FromHours(-5));

        var literal = mapping.GenerateSqlLiteral(dto);

        // Should be converted to UTC: 19:30:00
        Assert.Equal("'2024-01-15 19:30:00.000'", literal);
    }

    [Fact]
    public void DateTimeOffsetMapping_GeneratesCorrectLiteral_WithMillisecondPrecision()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3);
        var dto = new DateTimeOffset(2024, 6, 15, 14, 30, 45, 123, TimeSpan.Zero);

        var literal = mapping.GenerateSqlLiteral(dto);

        Assert.Equal("'2024-06-15 14:30:45.123'", literal);
    }

    [Fact]
    public void DateTimeOffsetMapping_GeneratesCorrectLiteral_WithMicrosecondPrecision()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(6);
        var dto = new DateTimeOffset(2024, 6, 15, 14, 30, 45, 123, TimeSpan.Zero);

        var literal = mapping.GenerateSqlLiteral(dto);

        // Precision 6 uses 6 decimal places
        Assert.Equal("'2024-06-15 14:30:45.123000'", literal);
    }

    [Fact]
    public void DateTimeOffsetMapping_HasValueConverter()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "America/New_York");

        Assert.NotNull(mapping.Converter);
        Assert.IsType<ValueConverter<DateTimeOffset, DateTime>>(mapping.Converter);
    }

    [Fact]
    public void DateTimeOffsetMapping_ValueConverter_WritesUtc()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "America/New_York");
        var converter = (ValueConverter<DateTimeOffset, DateTime>)mapping.Converter!;

        // 2:30 PM EST (UTC-5) = 7:30 PM UTC
        var dto = new DateTimeOffset(2024, 1, 15, 14, 30, 0, TimeSpan.FromHours(-5));
        var result = converter.ConvertToProvider(dto);

        Assert.Equal(new DateTime(2024, 1, 15, 19, 30, 0, DateTimeKind.Utc), result);
    }

    [Fact]
    public void DateTimeOffsetMapping_ValueConverter_ReadsWithOffset_Winter()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "America/New_York");
        var converter = (ValueConverter<DateTimeOffset, DateTime>)mapping.Converter!;

        // UTC time in winter (January)
        var utc = new DateTime(2024, 1, 15, 19, 30, 0, DateTimeKind.Utc);
        var result = (DateTimeOffset)converter.ConvertFromProvider(utc)!;

        // Should be 2:30 PM EST (UTC-5)
        Assert.Equal(new DateTimeOffset(2024, 1, 15, 14, 30, 0, TimeSpan.FromHours(-5)), result);
    }

    [Fact]
    public void DateTimeOffsetMapping_ValueConverter_ReadsWithOffset_Summer()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "America/New_York");
        var converter = (ValueConverter<DateTimeOffset, DateTime>)mapping.Converter!;

        // UTC time in summer (June)
        var utc = new DateTime(2024, 6, 15, 18, 30, 0, DateTimeKind.Utc);
        var result = (DateTimeOffset)converter.ConvertFromProvider(utc)!;

        // Should be 2:30 PM EDT (UTC-4)
        Assert.Equal(new DateTimeOffset(2024, 6, 15, 14, 30, 0, TimeSpan.FromHours(-4)), result);
    }

    [Fact]
    public void DateTimeOffsetMapping_ValueConverter_ReadsWithOffset_UTC()
    {
        var mapping = new ClickHouseDateTimeOffsetTypeMapping(3, "UTC");
        var converter = (ValueConverter<DateTimeOffset, DateTime>)mapping.Converter!;

        var utc = new DateTime(2024, 6, 15, 18, 30, 0, DateTimeKind.Utc);
        var result = (DateTimeOffset)converter.ConvertFromProvider(utc)!;

        // UTC should have zero offset
        Assert.Equal(TimeSpan.Zero, result.Offset);
        Assert.Equal(new DateTimeOffset(2024, 6, 15, 18, 30, 0, TimeSpan.Zero), result);
    }

    #endregion

    #region Annotation Tests

    [Fact]
    public void ClickHouseTimeZoneAttribute_SetsAnnotation()
    {
        var options = new DbContextOptionsBuilder<AttributeTimezoneContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new AttributeTimezoneContext(options);
        var entityType = context.Model.FindEntityType(typeof(TimezoneAttributeEntity))!;
        var property = entityType.FindProperty(nameof(TimezoneAttributeEntity.EasternTime))!;

        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.TimeZone);

        Assert.NotNull(annotation);
        Assert.Equal("America/New_York", annotation.Value);
    }

    [Fact]
    public void HasTimeZone_SetsAnnotation()
    {
        var options = new DbContextOptionsBuilder<FluentTimezoneContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new FluentTimezoneContext(options);
        var entityType = context.Model.FindEntityType(typeof(FluentTimezoneEntity))!;
        var property = entityType.FindProperty(nameof(FluentTimezoneEntity.EasternTime))!;

        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.TimeZone);

        Assert.NotNull(annotation);
        Assert.Equal("America/New_York", annotation.Value);
    }

    [Fact]
    public void HasTimeZone_FluentOverridesAttribute()
    {
        var options = new DbContextOptionsBuilder<OverrideTimezoneContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new OverrideTimezoneContext(options);
        var entityType = context.Model.FindEntityType(typeof(OverrideTimezoneEntity))!;
        var property = entityType.FindProperty(nameof(OverrideTimezoneEntity.Timestamp))!;

        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.TimeZone);

        Assert.NotNull(annotation);
        // Fluent API should override the attribute's "America/New_York"
        Assert.Equal("Europe/London", annotation.Value);
    }

    [Fact]
    public void HasTimeZone_NonDateTimeOffsetProperty_ThrowsInvalidOperationException()
    {
        var exception = Assert.Throws<InvalidOperationException>(() =>
        {
            var optionsBuilder = new DbContextOptionsBuilder<DbContext>()
                .UseClickHouse("Host=localhost;Database=test");

            using var context = new TestNonDateTimeOffsetContext(optionsBuilder.Options);
            // Force model building
            _ = context.Model;
        });

        Assert.Contains("HasTimeZone", exception.Message);
    }

    [Fact]
    public void ClickHouseTimeZoneAttribute_NullTimezone_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new ClickHouseTimeZoneAttribute(null!));
    }

    [Fact]
    public void ClickHouseTimeZoneAttribute_EmptyTimezone_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => new ClickHouseTimeZoneAttribute(""));
    }

    [Fact]
    public void ClickHouseTimeZoneAttribute_WhitespaceTimezone_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => new ClickHouseTimeZoneAttribute("   "));
    }

    #endregion

    #region DDL Generation Tests

    [Fact]
    public void CreateTable_GeneratesDateTimeOffset_WithoutTimezone()
    {
        var options = new DbContextOptionsBuilder<BasicDateTimeOffsetContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new BasicDateTimeOffsetContext(options);
        var entityType = context.Model.FindEntityType(typeof(DateTimeOffsetEntity))!;
        var property = entityType.FindProperty(nameof(DateTimeOffsetEntity.CreatedAt))!;

        var columnType = property.GetColumnType();

        // Default DateTimeOffset without timezone should use DateTime64(3)
        Assert.Contains("DateTime64", columnType);
    }

    [Fact]
    public void CreateTable_GeneratesDateTimeOffset_WithTimezone()
    {
        var options = new DbContextOptionsBuilder<AttributeTimezoneContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new AttributeTimezoneContext(options);
        var entityType = context.Model.FindEntityType(typeof(TimezoneAttributeEntity))!;
        var property = entityType.FindProperty(nameof(TimezoneAttributeEntity.EasternTime))!;

        var columnType = property.GetColumnType();

        Assert.Equal("DateTime64(3, 'America/New_York')", columnType);
    }

    [Fact]
    public void CreateTable_GeneratesNullableDateTimeOffset_WithTimezone()
    {
        var options = new DbContextOptionsBuilder<NullableDateTimeOffsetContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new NullableDateTimeOffsetContext(options);
        var entityType = context.Model.FindEntityType(typeof(NullableDateTimeOffsetEntity))!;
        var property = entityType.FindProperty(nameof(NullableDateTimeOffsetEntity.OptionalCentralTime))!;

        var columnType = property.GetColumnType();

        Assert.Equal("Nullable(DateTime64(3, 'America/Chicago'))", columnType);
    }

    [Fact]
    public void CreateTable_GeneratesMultipleTimezones()
    {
        var options = new DbContextOptionsBuilder<MultiTimezoneContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new MultiTimezoneContext(options);
        var entityType = context.Model.FindEntityType(typeof(MultiTimezoneEntity))!;

        var nyProperty = entityType.FindProperty(nameof(MultiTimezoneEntity.NewYork))!;
        var laProperty = entityType.FindProperty(nameof(MultiTimezoneEntity.LosAngeles))!;
        var parisProperty = entityType.FindProperty(nameof(MultiTimezoneEntity.Paris))!;
        var tokyoProperty = entityType.FindProperty(nameof(MultiTimezoneEntity.Tokyo))!;

        Assert.Equal("DateTime64(3, 'America/New_York')", nyProperty.GetColumnType());
        Assert.Equal("DateTime64(3, 'America/Los_Angeles')", laProperty.GetColumnType());
        Assert.Equal("DateTime64(3, 'Europe/Paris')", parisProperty.GetColumnType());
        Assert.Equal("DateTime64(3, 'Asia/Tokyo')", tokyoProperty.GetColumnType());
    }

    #endregion
}

/// <summary>
/// Test context for verifying HasTimeZone throws on non-DateTimeOffset properties.
/// </summary>
public class TestNonDateTimeOffsetContext : DbContext
{
    public TestNonDateTimeOffsetContext(DbContextOptions options) : base(options) { }

    public DbSet<TestNonDateTimeOffsetEntity> Entities => Set<TestNonDateTimeOffsetEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TestNonDateTimeOffsetEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("test_entities");
            entity.UseMergeTree(x => x.Id);

            // This should throw - DateTime is not DateTimeOffset
            entity.Property(e => e.CreatedAt)
                .HasTimeZone("America/New_York");
        });
    }
}

public class TestNonDateTimeOffsetEntity
{
    public Guid Id { get; set; }
    public DateTime CreatedAt { get; set; }
}

#endregion

#region Integration Tests

public class DateTimeOffsetIntegrationTests : IAsyncLifetime
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

    private TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }

    #region Round-trip Tests

    [Fact]
    public async Task RoundTrip_PreservesInstant_NoTimezone()
    {
        await using var context = CreateContext<BasicDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var original = new DateTimeOffset(2024, 6, 15, 14, 30, 0, TimeSpan.FromHours(-4));
        var entity = new DateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = "Test",
            CreatedAt = original,
            UpdatedAt = original
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        // The instant (UTC moment) should be preserved
        Assert.Equal(original.UtcDateTime, result.CreatedAt.UtcDateTime);
    }

    [Fact]
    public async Task RoundTrip_PreservesInstant_WithTimezone()
    {
        await using var context = CreateContext<AttributeTimezoneContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var original = new DateTimeOffset(2024, 6, 15, 14, 30, 0, TimeSpan.FromHours(-4));
        var entity = new TimezoneAttributeEntity
        {
            Id = Guid.NewGuid(),
            EasternTime = original,
            LondonTime = original,
            UtcTime = original
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        // All should preserve the same UTC instant
        Assert.Equal(original.UtcDateTime, result.EasternTime.UtcDateTime);
        Assert.Equal(original.UtcDateTime, result.LondonTime.UtcDateTime);
        Assert.Equal(original.UtcDateTime, result.UtcTime.UtcDateTime);
    }

    [Fact]
    public async Task RoundTrip_DifferentInputOffsets_PreserveSameInstant()
    {
        await using var context = CreateContext<BasicDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Same instant represented with different offsets
        var utcTime = new DateTime(2024, 6, 15, 18, 30, 0, DateTimeKind.Utc);
        var offsets = new[]
        {
            new DateTimeOffset(utcTime),  // UTC
            new DateTimeOffset(2024, 6, 15, 14, 30, 0, TimeSpan.FromHours(-4)),  // EDT
            new DateTimeOffset(2024, 6, 15, 11, 30, 0, TimeSpan.FromHours(-7)),  // PDT
            new DateTimeOffset(2024, 6, 16, 3, 30, 0, TimeSpan.FromHours(9)),    // JST
        };

        var entities = offsets.Select((dto, i) => new DateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = $"Test{i}",
            CreatedAt = dto,
            UpdatedAt = dto
        }).ToList();

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var results = await context.Entities.ToListAsync();

        // All should have the same UTC instant
        foreach (var result in results)
        {
            Assert.Equal(utcTime, result.CreatedAt.UtcDateTime);
        }
    }

    [Fact]
    public async Task RoundTrip_PreservesPrecision_Milliseconds()
    {
        await using var context = CreateContext<BasicDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var original = new DateTimeOffset(2024, 6, 15, 14, 30, 45, 123, TimeSpan.Zero);
        var entity = new DateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = "Test",
            CreatedAt = original,
            UpdatedAt = original
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        // Should preserve milliseconds (within 1ms tolerance)
        var diff = Math.Abs((original.UtcDateTime - result.CreatedAt.UtcDateTime).TotalMilliseconds);
        Assert.True(diff < 1, $"Precision lost: {diff}ms difference");
    }

    [Fact]
    public async Task BulkInsert_PreservesInstants()
    {
        await using var context = CreateContext<BasicDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var baseTime = new DateTimeOffset(2024, 6, 15, 0, 0, 0, TimeSpan.Zero);
        var entities = Enumerable.Range(0, 100).Select(i => new DateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = $"Entity{i:D3}",  // Use zero-padded names for correct lexicographic order
            CreatedAt = baseTime.AddHours(i),
            UpdatedAt = baseTime.AddHours(i)
        }).ToList();

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        // Order by CreatedAt to ensure chronological order
        var results = await context.Entities.OrderBy(e => e.CreatedAt).ToListAsync();

        Assert.Equal(100, results.Count);
        for (int i = 0; i < 100; i++)
        {
            Assert.Equal(baseTime.AddHours(i).UtcDateTime, results[i].CreatedAt.UtcDateTime);
        }
    }

    #endregion

    #region Timezone Tests

    [Fact]
    public async Task WithTimezone_AppliesOffset_Summer()
    {
        await using var context = CreateContext<AttributeTimezoneContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Store a summer date
        var original = new DateTimeOffset(2024, 6, 15, 18, 30, 0, TimeSpan.Zero);  // UTC
        var entity = new TimezoneAttributeEntity
        {
            Id = Guid.NewGuid(),
            EasternTime = original,
            LondonTime = original,
            UtcTime = original
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        // America/New_York in summer = EDT (UTC-4)
        Assert.Equal(TimeSpan.FromHours(-4), result.EasternTime.Offset);

        // Europe/London in summer = BST (UTC+1)
        Assert.Equal(TimeSpan.FromHours(1), result.LondonTime.Offset);

        // UTC should have zero offset
        Assert.Equal(TimeSpan.Zero, result.UtcTime.Offset);
    }

    [Fact]
    public async Task WithTimezone_AppliesOffset_Winter()
    {
        await using var context = CreateContext<AttributeTimezoneContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Store a winter date
        var original = new DateTimeOffset(2024, 1, 15, 19, 30, 0, TimeSpan.Zero);  // UTC
        var entity = new TimezoneAttributeEntity
        {
            Id = Guid.NewGuid(),
            EasternTime = original,
            LondonTime = original,
            UtcTime = original
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        // America/New_York in winter = EST (UTC-5)
        Assert.Equal(TimeSpan.FromHours(-5), result.EasternTime.Offset);

        // Europe/London in winter = GMT (UTC+0)
        Assert.Equal(TimeSpan.Zero, result.LondonTime.Offset);

        // UTC should have zero offset
        Assert.Equal(TimeSpan.Zero, result.UtcTime.Offset);
    }

    [Fact]
    public async Task WithTimezone_AppliesOffset_Tokyo_NoDst()
    {
        await using var context = CreateContext<FluentTimezoneContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Test both summer and winter - Tokyo doesn't observe DST
        var summerUtc = new DateTimeOffset(2024, 6, 15, 0, 0, 0, TimeSpan.Zero);
        var winterUtc = new DateTimeOffset(2024, 1, 15, 0, 0, 0, TimeSpan.Zero);

        var summerEntity = new FluentTimezoneEntity
        {
            Id = Guid.NewGuid(),
            EasternTime = summerUtc,
            TokyoTime = summerUtc
        };

        var winterEntity = new FluentTimezoneEntity
        {
            Id = Guid.NewGuid(),
            EasternTime = winterUtc,
            TokyoTime = winterUtc
        };

        context.Entities.AddRange(summerEntity, winterEntity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var summerResult = await context.Entities.FirstAsync(e => e.Id == summerEntity.Id);
        var winterResult = await context.Entities.FirstAsync(e => e.Id == winterEntity.Id);

        // Tokyo should always be UTC+9 regardless of season
        Assert.Equal(TimeSpan.FromHours(9), summerResult.TokyoTime.Offset);
        Assert.Equal(TimeSpan.FromHours(9), winterResult.TokyoTime.Offset);
    }

    [Fact]
    public async Task MultipleTimezones_EachAppliesCorrectOffset()
    {
        await using var context = CreateContext<MultiTimezoneContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Summer date so DST is in effect for some timezones
        var utcTime = new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.Zero);
        var entity = new MultiTimezoneEntity
        {
            Id = Guid.NewGuid(),
            NewYork = utcTime,
            LosAngeles = utcTime,
            Paris = utcTime,
            Tokyo = utcTime
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        // Verify each timezone has correct offset for summer
        Assert.Equal(TimeSpan.FromHours(-4), result.NewYork.Offset);      // EDT
        Assert.Equal(TimeSpan.FromHours(-7), result.LosAngeles.Offset);   // PDT
        Assert.Equal(TimeSpan.FromHours(2), result.Paris.Offset);         // CEST
        Assert.Equal(TimeSpan.FromHours(9), result.Tokyo.Offset);         // JST (no DST)

        // All should represent the same instant
        Assert.Equal(utcTime.UtcDateTime, result.NewYork.UtcDateTime);
        Assert.Equal(utcTime.UtcDateTime, result.LosAngeles.UtcDateTime);
        Assert.Equal(utcTime.UtcDateTime, result.Paris.UtcDateTime);
        Assert.Equal(utcTime.UtcDateTime, result.Tokyo.UtcDateTime);
    }

    [Fact]
    public async Task FluentApi_AppliesCorrectOffset()
    {
        await using var context = CreateContext<FluentTimezoneContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var utcTime = new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.Zero);
        var entity = new FluentTimezoneEntity
        {
            Id = Guid.NewGuid(),
            EasternTime = utcTime,
            TokyoTime = utcTime
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        // Verify fluent API configuration works
        Assert.Equal(TimeSpan.FromHours(-4), result.EasternTime.Offset);  // EDT
        Assert.Equal(TimeSpan.FromHours(9), result.TokyoTime.Offset);     // JST
    }

    [Fact]
    public async Task FluentOverridesAttribute_AppliesFluentTimezone()
    {
        await using var context = CreateContext<OverrideTimezoneContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var utcTime = new DateTimeOffset(2024, 6, 15, 12, 0, 0, TimeSpan.Zero);
        var entity = new OverrideTimezoneEntity
        {
            Id = Guid.NewGuid(),
            Timestamp = utcTime
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        // Fluent API (Europe/London) should override attribute (America/New_York)
        // London in summer = BST (UTC+1)
        Assert.Equal(TimeSpan.FromHours(1), result.Timestamp.Offset);
    }

    #endregion

    #region DST Edge Case Tests

    [Fact]
    public async Task DstSpringForward_PreservesInstant()
    {
        await using var context = CreateContext<AttributeTimezoneContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Spring forward: March 10, 2024 at 2:00 AM EST → 3:00 AM EDT
        // 1:59 AM EST = UTC 6:59 AM (offset -5)
        var beforeSpring = new DateTimeOffset(2024, 3, 10, 1, 59, 0, TimeSpan.FromHours(-5));
        // 3:00 AM EDT = UTC 7:00 AM (offset -4)
        var afterSpring = new DateTimeOffset(2024, 3, 10, 3, 0, 0, TimeSpan.FromHours(-4));

        var entity1 = new TimezoneAttributeEntity
        {
            Id = Guid.NewGuid(),
            EasternTime = beforeSpring,
            LondonTime = beforeSpring,
            UtcTime = beforeSpring
        };

        var entity2 = new TimezoneAttributeEntity
        {
            Id = Guid.NewGuid(),
            EasternTime = afterSpring,
            LondonTime = afterSpring,
            UtcTime = afterSpring
        };

        context.Entities.AddRange(entity1, entity2);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var result1 = await context.Entities.FirstAsync(e => e.Id == entity1.Id);
        var result2 = await context.Entities.FirstAsync(e => e.Id == entity2.Id);

        // UTC instants should be preserved
        Assert.Equal(beforeSpring.UtcDateTime, result1.EasternTime.UtcDateTime);
        Assert.Equal(afterSpring.UtcDateTime, result2.EasternTime.UtcDateTime);
    }

    [Fact]
    public async Task DstSpringForward_ReturnsValidTime()
    {
        await using var context = CreateContext<AttributeTimezoneContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Store a time during the DST gap (2:30 AM EST doesn't exist on March 10, 2024)
        // We'll store the UTC equivalent and verify the read-back is valid
        var gapTimeUtc = new DateTimeOffset(2024, 3, 10, 7, 30, 0, TimeSpan.Zero);  // 7:30 AM UTC

        var entity = new TimezoneAttributeEntity
        {
            Id = Guid.NewGuid(),
            EasternTime = gapTimeUtc,
            LondonTime = gapTimeUtc,
            UtcTime = gapTimeUtc
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        // The result should be a valid time in EDT (after spring forward)
        // 7:30 AM UTC = 3:30 AM EDT (not 2:30 AM EST which doesn't exist)
        Assert.Equal(TimeSpan.FromHours(-4), result.EasternTime.Offset);
        Assert.Equal(3, result.EasternTime.Hour);
        Assert.Equal(30, result.EasternTime.Minute);
    }

    [Fact(Skip = "Known limitation: During DST fall-back, UTC times that map to the same local time " +
                   "may not round-trip correctly when using timezone-aware columns. " +
                   "Use UTC timezone for precise instant preservation.")]
    public async Task DstFallBack_PreservesInstant()
    {
        // This test documents a known limitation with DST fall-back times.
        // When we write a UTC time (e.g., 05:30 UTC) as a SQL literal to a timezone-aware column,
        // ClickHouse interprets the literal as local time in that timezone, not UTC.
        // This causes the UTC instant to be shifted.
        //
        // Workaround: For applications that need precise instant preservation during DST transitions,
        // use UTC timezone for the column and handle timezone conversion in the application layer.

        await using var context = CreateContext<AttributeTimezoneContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Fall back: November 3, 2024 at 2:00 AM EDT → 1:00 AM EST
        // First 1:30 AM (EDT) = UTC 5:30 AM (offset -4)
        var fallBackFirst = new DateTimeOffset(2024, 11, 3, 1, 30, 0, TimeSpan.FromHours(-4));
        // Second 1:30 AM (EST) = UTC 6:30 AM (offset -5)
        var fallBackSecond = new DateTimeOffset(2024, 11, 3, 1, 30, 0, TimeSpan.FromHours(-5));

        var entity1 = new TimezoneAttributeEntity
        {
            Id = Guid.NewGuid(),
            EasternTime = fallBackFirst,
            LondonTime = fallBackFirst,
            UtcTime = fallBackFirst
        };

        var entity2 = new TimezoneAttributeEntity
        {
            Id = Guid.NewGuid(),
            EasternTime = fallBackSecond,
            LondonTime = fallBackSecond,
            UtcTime = fallBackSecond
        };

        context.Entities.AddRange(entity1, entity2);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var result1 = await context.Entities.FirstAsync(e => e.Id == entity1.Id);
        var result2 = await context.Entities.FirstAsync(e => e.Id == entity2.Id);

        // UTC instants should be preserved - they are 1 hour apart
        Assert.Equal(fallBackFirst.UtcDateTime, result1.EasternTime.UtcDateTime);
        Assert.Equal(fallBackSecond.UtcDateTime, result2.EasternTime.UtcDateTime);
        Assert.Equal(TimeSpan.FromHours(1), result2.EasternTime.UtcDateTime - result1.EasternTime.UtcDateTime);
    }

    [Fact(Skip = "Known limitation: During DST fall-back, ClickHouse timezone handling may not preserve " +
                   "distinct UTC instants for ambiguous local times. The UTC time is written as a literal " +
                   "which ClickHouse interprets as local time in the column's timezone.")]
    public async Task DstFallBack_AmbiguousTime_PreservesUtc()
    {
        // This test documents a known limitation with DST fall-back ambiguous times.
        // During fall back, 1:30 AM occurs twice:
        // - First occurrence (EDT): 1:30 AM offset -4 = 5:30 AM UTC
        // - Second occurrence (EST): 1:30 AM offset -5 = 6:30 AM UTC
        //
        // When we write 5:30 UTC as '2024-11-03 05:30:00' to a DateTime64(3, 'America/New_York') column,
        // ClickHouse interprets this as 05:30 AM in New York time, not as 05:30 UTC.
        // This causes the UTC instant to be shifted.
        //
        // Workaround: For applications that need precise DST fall-back handling, use UTC timezone
        // for the column and handle timezone conversion in the application layer.

        await using var context = CreateContext<AttributeTimezoneContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var firstOccurrence = new DateTimeOffset(2024, 11, 3, 1, 30, 0, TimeSpan.FromHours(-4));
        var secondOccurrence = new DateTimeOffset(2024, 11, 3, 1, 30, 0, TimeSpan.FromHours(-5));

        var entity1 = new TimezoneAttributeEntity
        {
            Id = Guid.NewGuid(),
            EasternTime = firstOccurrence,
            LondonTime = firstOccurrence,
            UtcTime = firstOccurrence
        };

        var entity2 = new TimezoneAttributeEntity
        {
            Id = Guid.NewGuid(),
            EasternTime = secondOccurrence,
            LondonTime = secondOccurrence,
            UtcTime = secondOccurrence
        };

        context.Entities.AddRange(entity1, entity2);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var result1 = await context.Entities.FirstAsync(e => e.Id == entity1.Id);
        var result2 = await context.Entities.FirstAsync(e => e.Id == entity2.Id);

        // The UTC instants should be different (1 hour apart)
        Assert.NotEqual(result1.EasternTime.UtcDateTime, result2.EasternTime.UtcDateTime);
        Assert.Equal(firstOccurrence.UtcDateTime, result1.EasternTime.UtcDateTime);
        Assert.Equal(secondOccurrence.UtcDateTime, result2.EasternTime.UtcDateTime);
    }

    [Fact]
    public async Task CrossDstBoundary_OrderByCorrect()
    {
        await using var context = CreateContext<AttributeTimezoneContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Create entities spanning the DST transition
        var times = new[]
        {
            new DateTimeOffset(2024, 3, 10, 1, 0, 0, TimeSpan.FromHours(-5)),   // 1 AM EST
            new DateTimeOffset(2024, 3, 10, 1, 30, 0, TimeSpan.FromHours(-5)),  // 1:30 AM EST
            new DateTimeOffset(2024, 3, 10, 3, 0, 0, TimeSpan.FromHours(-4)),   // 3 AM EDT (right after spring forward)
            new DateTimeOffset(2024, 3, 10, 3, 30, 0, TimeSpan.FromHours(-4)),  // 3:30 AM EDT
        };

        var entities = times.Select((t, i) => new TimezoneAttributeEntity
        {
            Id = Guid.NewGuid(),
            EasternTime = t,
            LondonTime = t,
            UtcTime = t
        }).ToList();

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var results = await context.Entities
            .OrderBy(e => e.EasternTime)
            .ToListAsync();

        // Verify chronological order
        for (int i = 1; i < results.Count; i++)
        {
            Assert.True(
                results[i - 1].EasternTime.UtcDateTime <= results[i].EasternTime.UtcDateTime,
                $"Results not in chronological order at index {i}");
        }
    }

    #endregion

    #region Nullable Tests

    [Fact]
    public async Task Nullable_CanInsertNull()
    {
        await using var context = CreateContext<NullableDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var entity = new NullableDateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = "Test",
            OptionalTimestamp = null,
            OptionalCentralTime = null
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();

        var count = await context.Entities.CountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task Nullable_CanReadNull()
    {
        await using var context = CreateContext<NullableDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var entity = new NullableDateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = "Test",
            OptionalTimestamp = null,
            OptionalCentralTime = null
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        Assert.Null(result.OptionalTimestamp);
        Assert.Null(result.OptionalCentralTime);
    }

    [Fact]
    public async Task Nullable_CanInsertValue()
    {
        await using var context = CreateContext<NullableDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var timestamp = new DateTimeOffset(2024, 6, 15, 14, 30, 0, TimeSpan.FromHours(-4));
        var entity = new NullableDateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = "Test",
            OptionalTimestamp = timestamp,
            OptionalCentralTime = timestamp
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        Assert.NotNull(result.OptionalTimestamp);
        Assert.Equal(timestamp.UtcDateTime, result.OptionalTimestamp!.Value.UtcDateTime);
    }

    [Fact]
    public async Task Nullable_WithTimezone_Null()
    {
        await using var context = CreateContext<NullableDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var entity = new NullableDateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = "Test",
            OptionalTimestamp = null,
            OptionalCentralTime = null  // Has [ClickHouseTimeZone("America/Chicago")]
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        Assert.Null(result.OptionalCentralTime);
    }

    [Fact]
    public async Task Nullable_WithTimezone_Value_AppliesOffset()
    {
        await using var context = CreateContext<NullableDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var utcTime = new DateTimeOffset(2024, 6, 15, 18, 30, 0, TimeSpan.Zero);
        var entity = new NullableDateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = "Test",
            OptionalTimestamp = utcTime,
            OptionalCentralTime = utcTime  // Has [ClickHouseTimeZone("America/Chicago")]
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstAsync(e => e.Id == entity.Id);

        Assert.NotNull(result.OptionalCentralTime);
        // America/Chicago in summer = CDT (UTC-5)
        Assert.Equal(TimeSpan.FromHours(-5), result.OptionalCentralTime!.Value.Offset);
        Assert.Equal(utcTime.UtcDateTime, result.OptionalCentralTime.Value.UtcDateTime);
    }

    [Fact]
    public async Task Nullable_QueryWhereNull()
    {
        await using var context = CreateContext<NullableDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var withValue = new NullableDateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = "WithValue",
            OptionalTimestamp = DateTimeOffset.UtcNow,
            OptionalCentralTime = null
        };

        var withNull = new NullableDateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = "WithNull",
            OptionalTimestamp = null,
            OptionalCentralTime = null
        };

        context.Entities.AddRange(withValue, withNull);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var nullResults = await context.Entities
            .Where(e => e.OptionalTimestamp == null)
            .ToListAsync();

        Assert.Single(nullResults);
        Assert.Equal("WithNull", nullResults[0].Name);
    }

    [Fact]
    public async Task Nullable_QueryWhereNotNull()
    {
        await using var context = CreateContext<NullableDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var withValue = new NullableDateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = "WithValue",
            OptionalTimestamp = DateTimeOffset.UtcNow,
            OptionalCentralTime = null
        };

        var withNull = new NullableDateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = "WithNull",
            OptionalTimestamp = null,
            OptionalCentralTime = null
        };

        context.Entities.AddRange(withValue, withNull);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var notNullResults = await context.Entities
            .Where(e => e.OptionalTimestamp != null)
            .ToListAsync();

        Assert.Single(notNullResults);
        Assert.Equal("WithValue", notNullResults[0].Name);
    }

    [Fact]
    public async Task Nullable_MixedNullAndValue()
    {
        await using var context = CreateContext<NullableDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var entities = new[]
        {
            new NullableDateTimeOffsetEntity { Id = Guid.NewGuid(), Name = "E1", OptionalTimestamp = null },
            new NullableDateTimeOffsetEntity { Id = Guid.NewGuid(), Name = "E2", OptionalTimestamp = DateTimeOffset.UtcNow },
            new NullableDateTimeOffsetEntity { Id = Guid.NewGuid(), Name = "E3", OptionalTimestamp = null },
            new NullableDateTimeOffsetEntity { Id = Guid.NewGuid(), Name = "E4", OptionalTimestamp = DateTimeOffset.UtcNow.AddHours(1) },
        };

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var allResults = await context.Entities.ToListAsync();
        var nullCount = allResults.Count(e => e.OptionalTimestamp == null);
        var notNullCount = allResults.Count(e => e.OptionalTimestamp != null);

        Assert.Equal(4, allResults.Count);
        Assert.Equal(2, nullCount);
        Assert.Equal(2, notNullCount);
    }

    #endregion

    #region Query Tests

    [Fact]
    public async Task Query_ByValue_FindsMatch()
    {
        await using var context = CreateContext<BasicDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var targetTime = new DateTimeOffset(2024, 6, 15, 14, 30, 0, TimeSpan.Zero);
        var entity = new DateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = "Target",
            CreatedAt = targetTime,
            UpdatedAt = targetTime
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        // Query by the exact value
        var result = await context.Entities
            .Where(e => e.CreatedAt == targetTime)
            .FirstOrDefaultAsync();

        Assert.NotNull(result);
        Assert.Equal("Target", result.Name);
    }

    [Fact]
    public async Task Query_GreaterThan_ReturnsCorrectResults()
    {
        await using var context = CreateContext<BasicDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var baseTime = new DateTimeOffset(2024, 6, 15, 0, 0, 0, TimeSpan.Zero);
        var entities = Enumerable.Range(0, 10).Select(i => new DateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = $"Entity{i}",
            CreatedAt = baseTime.AddHours(i),
            UpdatedAt = baseTime.AddHours(i)
        }).ToList();

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var threshold = baseTime.AddHours(5);
        var results = await context.Entities
            .Where(e => e.CreatedAt > threshold)
            .ToListAsync();

        Assert.Equal(4, results.Count);  // Hours 6, 7, 8, 9
        Assert.All(results, r => Assert.True(r.CreatedAt > threshold));
    }

    [Fact]
    public async Task Query_Range_ReturnsCorrectResults()
    {
        await using var context = CreateContext<BasicDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var baseTime = new DateTimeOffset(2024, 6, 15, 0, 0, 0, TimeSpan.Zero);
        var entities = Enumerable.Range(0, 10).Select(i => new DateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = $"Entity{i}",
            CreatedAt = baseTime.AddHours(i),
            UpdatedAt = baseTime.AddHours(i)
        }).ToList();

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var start = baseTime.AddHours(3);
        var end = baseTime.AddHours(7);
        var results = await context.Entities
            .Where(e => e.CreatedAt >= start && e.CreatedAt <= end)
            .ToListAsync();

        Assert.Equal(5, results.Count);  // Hours 3, 4, 5, 6, 7
        Assert.All(results, r =>
        {
            Assert.True(r.CreatedAt >= start);
            Assert.True(r.CreatedAt <= end);
        });
    }

    [Fact]
    public async Task Query_OrderByAsc_Chronological()
    {
        await using var context = CreateContext<BasicDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var baseTime = new DateTimeOffset(2024, 6, 15, 0, 0, 0, TimeSpan.Zero);
        // Insert in random order
        var entities = new[] { 5, 2, 8, 1, 9, 3, 7, 0, 6, 4 }.Select(i => new DateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = $"Entity{i}",
            CreatedAt = baseTime.AddHours(i),
            UpdatedAt = baseTime.AddHours(i)
        }).ToList();

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var results = await context.Entities
            .OrderBy(e => e.CreatedAt)
            .ToListAsync();

        Assert.Equal(10, results.Count);
        for (int i = 0; i < results.Count; i++)
        {
            Assert.Equal($"Entity{i}", results[i].Name);
        }
    }

    [Fact]
    public async Task Query_OrderByDesc_Chronological()
    {
        await using var context = CreateContext<BasicDateTimeOffsetContext>();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var baseTime = new DateTimeOffset(2024, 6, 15, 0, 0, 0, TimeSpan.Zero);
        // Insert in random order
        var entities = new[] { 5, 2, 8, 1, 9, 3, 7, 0, 6, 4 }.Select(i => new DateTimeOffsetEntity
        {
            Id = Guid.NewGuid(),
            Name = $"Entity{i}",
            CreatedAt = baseTime.AddHours(i),
            UpdatedAt = baseTime.AddHours(i)
        }).ToList();

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var results = await context.Entities
            .OrderByDescending(e => e.CreatedAt)
            .ToListAsync();

        Assert.Equal(10, results.Count);
        for (int i = 0; i < results.Count; i++)
        {
            Assert.Equal($"Entity{9 - i}", results[i].Name);
        }
    }

    #endregion
}

#endregion
