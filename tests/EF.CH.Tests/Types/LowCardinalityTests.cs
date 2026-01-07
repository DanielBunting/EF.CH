using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Types;

#region Test Entities

/// <summary>
/// Entity with LowCardinality string columns.
/// </summary>
public class LowCardinalityEntity
{
    public Guid Id { get; set; }
    public string Status { get; set; } = string.Empty;
    public string CountryCode { get; set; } = string.Empty;
    public string? NullableStatus { get; set; }
    public string CurrencyCode { get; set; } = string.Empty;
    public string? NullableCurrencyCode { get; set; }
}

#endregion

#region Test DbContexts

public class LowCardinalityContext : DbContext
{
    public LowCardinalityContext(DbContextOptions<LowCardinalityContext> options)
        : base(options) { }

    public DbSet<LowCardinalityEntity> Entities => Set<LowCardinalityEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<LowCardinalityEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("LowCardinalityEntities");
            entity.UseMergeTree(x => x.Id);

            // LowCardinality(String)
            entity.Property(e => e.Status)
                .HasLowCardinality();

            // LowCardinality(String) - another example
            entity.Property(e => e.CountryCode)
                .HasLowCardinality();

            // LowCardinality(Nullable(String))
            entity.Property(e => e.NullableStatus)
                .HasLowCardinality();

            // LowCardinality(FixedString(3))
            entity.Property(e => e.CurrencyCode)
                .HasLowCardinalityFixedString(3);

            // LowCardinality(Nullable(FixedString(3)))
            entity.Property(e => e.NullableCurrencyCode)
                .HasLowCardinalityFixedString(3);
        });
    }
}

#endregion

public class LowCardinalityTests
{
    #region Fluent API Tests

    [Fact]
    public void HasLowCardinality_SetsCorrectColumnType()
    {
        using var context = CreateContext<LowCardinalityContext>();

        var entityType = context.Model.FindEntityType(typeof(LowCardinalityEntity))!;
        var property = entityType.FindProperty(nameof(LowCardinalityEntity.Status))!;
        var mapping = (RelationalTypeMapping)property.GetTypeMapping();

        Assert.Equal("LowCardinality(String)", mapping.StoreType);
    }

    [Fact]
    public void HasLowCardinality_Nullable_SetsCorrectColumnType()
    {
        using var context = CreateContext<LowCardinalityContext>();

        var entityType = context.Model.FindEntityType(typeof(LowCardinalityEntity))!;
        var property = entityType.FindProperty(nameof(LowCardinalityEntity.NullableStatus))!;
        var mapping = (RelationalTypeMapping)property.GetTypeMapping();

        Assert.Equal("LowCardinality(Nullable(String))", mapping.StoreType);
    }

    [Fact]
    public void HasLowCardinalityFixedString_SetsCorrectColumnType()
    {
        using var context = CreateContext<LowCardinalityContext>();

        var entityType = context.Model.FindEntityType(typeof(LowCardinalityEntity))!;
        var property = entityType.FindProperty(nameof(LowCardinalityEntity.CurrencyCode))!;
        var mapping = (RelationalTypeMapping)property.GetTypeMapping();

        Assert.Equal("LowCardinality(FixedString(3))", mapping.StoreType);
    }

    [Fact]
    public void HasLowCardinalityFixedString_Nullable_SetsCorrectColumnType()
    {
        using var context = CreateContext<LowCardinalityContext>();

        var entityType = context.Model.FindEntityType(typeof(LowCardinalityEntity))!;
        var property = entityType.FindProperty(nameof(LowCardinalityEntity.NullableCurrencyCode))!;
        var mapping = (RelationalTypeMapping)property.GetTypeMapping();

        Assert.Equal("LowCardinality(Nullable(FixedString(3)))", mapping.StoreType);
    }

    #endregion

    #region DDL Generation Tests

    [Fact]
    public void CreateTable_GeneratesLowCardinalityColumns()
    {
        using var context = CreateContext<LowCardinalityContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("CREATE TABLE", script);
        Assert.Contains("LowCardinality(String)", script);
        Assert.Contains("LowCardinality(Nullable(String))", script);
        Assert.Contains("LowCardinality(FixedString(3))", script);
        Assert.Contains("LowCardinality(Nullable(FixedString(3)))", script);
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
public class LowCardinalityIntegrationTests : IAsyncLifetime
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
    public async Task CanCreateTableWithLowCardinalityColumns()
    {
        await using var context = CreateContext<LowCardinalityContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Verify table exists
        var tableExists = await context.Database.SqlQueryRaw<string>(
            "SELECT name AS \"Value\" FROM system.tables WHERE database = currentDatabase() AND name = 'LowCardinalityEntities'"
        ).AnyAsync();

        Assert.True(tableExists);

        // Verify column types
        var statusType = await context.Database.SqlQueryRaw<string>(
            "SELECT type AS \"Value\" FROM system.columns WHERE database = currentDatabase() AND table = 'LowCardinalityEntities' AND name = 'Status'"
        ).FirstOrDefaultAsync();

        Assert.NotNull(statusType);
        Assert.Contains("LowCardinality", statusType);
    }

    [Fact]
    public async Task CanInsertAndQueryLowCardinalityData()
    {
        await using var context = CreateContext<LowCardinalityContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert an entity
        var entity = new LowCardinalityEntity
        {
            Id = Guid.NewGuid(),
            Status = "active",
            CountryCode = "US",
            NullableStatus = "pending",
            CurrencyCode = "USD",
            NullableCurrencyCode = "EUR"
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();

        // Clear tracker and query
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstOrDefaultAsync();

        Assert.NotNull(result);
        Assert.Equal("active", result.Status);
        Assert.Equal("US", result.CountryCode);
        Assert.Equal("pending", result.NullableStatus);
        Assert.Equal("USD", result.CurrencyCode);
        Assert.Equal("EUR", result.NullableCurrencyCode);
    }

    [Fact]
    public async Task CanQueryWithLowCardinalityFilter()
    {
        await using var context = CreateContext<LowCardinalityContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert multiple entities
        var entities = new[]
        {
            new LowCardinalityEntity { Id = Guid.NewGuid(), Status = "active", CountryCode = "US", CurrencyCode = "USD" },
            new LowCardinalityEntity { Id = Guid.NewGuid(), Status = "inactive", CountryCode = "UK", CurrencyCode = "GBP" },
            new LowCardinalityEntity { Id = Guid.NewGuid(), Status = "active", CountryCode = "DE", CurrencyCode = "EUR" }
        };

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();

        // Clear tracker and query
        context.ChangeTracker.Clear();

        // Query with filter on LowCardinality column
        var activeCount = await context.Entities.CountAsync(e => e.Status == "active");

        Assert.Equal(2, activeCount);
    }

    [Fact]
    public async Task CanInsertWithNullLowCardinalityValues()
    {
        await using var context = CreateContext<LowCardinalityContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert an entity with null values
        var entity = new LowCardinalityEntity
        {
            Id = Guid.NewGuid(),
            Status = "active",
            CountryCode = "US",
            NullableStatus = null, // null
            CurrencyCode = "USD",
            NullableCurrencyCode = null // null
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();

        // Clear tracker and query
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstOrDefaultAsync();

        Assert.NotNull(result);
        Assert.Null(result.NullableStatus);
        Assert.Null(result.NullableCurrencyCode);
    }

    private TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }
}
