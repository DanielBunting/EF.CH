using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.InsertSelect;

/// <summary>
/// Tests for INSERT ... SELECT functionality.
///
/// NOTE: These tests verify same-type INSERT...SELECT operations where
/// the source query type matches the target DbSet type. For cross-type
/// operations, use the mapping overload.
/// </summary>
public class InsertSelectTests : IAsyncLifetime
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
    public async Task InsertSelect_WithFilter_InsertsMatchingRows()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        // Insert source data
        var sourceEvents = Enumerable.Range(0, 100)
            .Select(i => new Event
            {
                Id = Guid.NewGuid(),
                Timestamp = DateTime.UtcNow.AddDays(-i),
                Category = i % 2 == 0 ? "CategoryA" : "CategoryB",
                Amount = i * 10.5m
            })
            .ToList();

        await context.BulkInsertAsync(sourceEvents);

        // Execute INSERT ... SELECT with filter and mapping
        var result = await context.ArchivedEvents.ExecuteInsertFromQueryAsync(
            context.Events.Where(e => e.Category == "CategoryA"),
            e => new ArchivedEvent
            {
                Id = e.Id,
                Timestamp = e.Timestamp,
                Category = e.Category,
                Amount = e.Amount
            });

        Assert.True(result.RowsAffected >= 0); // ClickHouse may return 0 for INSERT ... SELECT
        Assert.True(result.Elapsed > TimeSpan.Zero);
        Assert.Contains("INSERT INTO", result.Sql);
        Assert.Contains("SELECT", result.Sql);

        // Verify data was inserted
        var archivedCount = await context.ArchivedEvents.CountAsync();
        Assert.Equal(50, archivedCount); // Half should be CategoryA
    }

    [Fact]
    public async Task InsertSelect_ReverseFluent_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        // Insert source data
        var sourceEvents = Enumerable.Range(0, 50)
            .Select(i => new Event
            {
                Id = Guid.NewGuid(),
                Timestamp = DateTime.UtcNow,
                Category = "Important",
                Amount = i * 2.0m
            })
            .ToList();

        await context.BulkInsertAsync(sourceEvents);

        // Use reverse fluent API with mapping
        var result = await context.Events
            .Where(e => e.Category == "Important")
            .InsertIntoAsync(context.ArchivedEvents,
                e => new ArchivedEvent
                {
                    Id = e.Id,
                    Timestamp = e.Timestamp,
                    Category = e.Category,
                    Amount = e.Amount
                });

        Assert.True(result.Elapsed > TimeSpan.Zero);
        Assert.Contains("INSERT INTO", result.Sql);

        var archivedCount = await context.ArchivedEvents.CountAsync();
        Assert.Equal(50, archivedCount);
    }

    [Fact]
    public async Task InsertSelect_EmptySource_ReturnsSuccessfully()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        // Execute INSERT ... SELECT on empty table with mapping
        var result = await context.ArchivedEvents.ExecuteInsertFromQueryAsync(
            context.Events.Where(e => e.Category == "NonExistent"),
            e => new ArchivedEvent
            {
                Id = e.Id,
                Timestamp = e.Timestamp,
                Category = e.Category,
                Amount = e.Amount
            });

        Assert.True(result.Elapsed > TimeSpan.Zero);
        Assert.Contains("INSERT INTO", result.Sql);

        var archivedCount = await context.ArchivedEvents.CountAsync();
        Assert.Equal(0, archivedCount);
    }

    [Fact]
    public async Task InsertSelect_SqlContainsCorrectTableAndColumns()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var result = await context.ArchivedEvents.ExecuteInsertFromQueryAsync(
            context.Events.Where(e => e.Category == "Test"),
            e => new ArchivedEvent
            {
                Id = e.Id,
                Timestamp = e.Timestamp,
                Category = e.Category,
                Amount = e.Amount
            });

        // Verify SQL structure
        Assert.Contains("INSERT INTO \"ArchivedEvents\"", result.Sql);
        Assert.Contains("\"Id\"", result.Sql);
        Assert.Contains("\"Timestamp\"", result.Sql);
        Assert.Contains("\"Category\"", result.Sql);
        Assert.Contains("\"Amount\"", result.Sql);
        Assert.Contains("SELECT", result.Sql);
        Assert.Contains("WHERE", result.Sql);
    }

    [Fact]
    public async Task InsertSelect_ComputedColumnsExcluded_SameType()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        // Verify computed column metadata is set
        var entityType = context.Model.FindEntityType(typeof(EntityWithComputed));
        var computedProp = entityType?.FindProperty("ComputedValue");
        var computedSql = computedProp?.GetComputedColumnSql();
        Assert.NotNull(computedSql);

        // Insert source data
        var sourceData = Enumerable.Range(0, 10)
            .Select(i => new EntityWithComputed
            {
                Id = Guid.NewGuid(),
                Value = i * 10
            })
            .ToList();

        await context.BulkInsertAsync(sourceData);

        // For same-type insert, computed columns are automatically excluded
        // from both INSERT column list and SELECT
        var result = await context.EntitiesWithComputed.ExecuteInsertFromQueryAsync(
            context.EntitiesWithComputed.Where(e => e.Value > 0));

        // The computed column should be excluded from the INSERT column list
        Assert.Contains("INSERT INTO", result.Sql);
        Assert.Contains("\"Id\"", result.Sql);
        Assert.Contains("\"Value\"", result.Sql);
        // ComputedValue should NOT be in the INSERT column list (first line)
        var insertLine = result.Sql.Split('\n')[0];
        Assert.DoesNotContain("\"ComputedValue\"", insertLine);

        // Note: For same-table insert, the source rows get duplicated
        // (9 rows with Value > 0 get inserted again = 19 total)
        var count = await context.EntitiesWithComputed.CountAsync();
        Assert.Equal(19, count);
    }

    [Fact]
    public async Task InsertSelect_WithOrderBy_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        // Insert source data
        var sourceEvents = Enumerable.Range(0, 30)
            .Select(i => new Event
            {
                Id = Guid.NewGuid(),
                Timestamp = DateTime.UtcNow.AddHours(-i),
                Category = "OrderTest",
                Amount = i * 5.0m
            })
            .ToList();

        await context.BulkInsertAsync(sourceEvents);

        // INSERT ... SELECT with ORDER BY and mapping
        var result = await context.ArchivedEvents.ExecuteInsertFromQueryAsync(
            context.Events
                .Where(e => e.Category == "OrderTest")
                .OrderByDescending(e => e.Amount),
            e => new ArchivedEvent
            {
                Id = e.Id,
                Timestamp = e.Timestamp,
                Category = e.Category,
                Amount = e.Amount
            });

        Assert.Contains("ORDER BY", result.Sql);

        var archivedCount = await context.ArchivedEvents.CountAsync();
        Assert.Equal(30, archivedCount);
    }

    [Fact]
    public async Task InsertSelect_WithMultipleConditions_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        // Insert source data
        var sourceEvents = Enumerable.Range(0, 100)
            .Select(i => new Event
            {
                Id = Guid.NewGuid(),
                Timestamp = DateTime.UtcNow,
                Category = i % 3 == 0 ? "A" : i % 3 == 1 ? "B" : "C",
                Amount = i * 1.5m
            })
            .ToList();

        await context.BulkInsertAsync(sourceEvents);

        // INSERT ... SELECT with multiple conditions and mapping
        var result = await context.ArchivedEvents.ExecuteInsertFromQueryAsync(
            context.Events.Where(e => e.Category == "A" && e.Amount > 50),
            e => new ArchivedEvent
            {
                Id = e.Id,
                Timestamp = e.Timestamp,
                Category = e.Category,
                Amount = e.Amount
            });

        Assert.Contains("WHERE", result.Sql);
        Assert.Contains("AND", result.Sql);

        // Verify some data was inserted
        var archivedCount = await context.ArchivedEvents.CountAsync();
        Assert.True(archivedCount > 0);
    }

    [Fact]
    public async Task InsertSelect_AllRows_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        // Insert source data
        var sourceEvents = Enumerable.Range(0, 50)
            .Select(i => new Event
            {
                Id = Guid.NewGuid(),
                Timestamp = DateTime.UtcNow,
                Category = $"Category{i}",
                Amount = 100m
            })
            .ToList();

        await context.BulkInsertAsync(sourceEvents);

        // INSERT ... SELECT all with mapping - should include all rows
        var result = await context.ArchivedEvents.ExecuteInsertFromQueryAsync(
            context.Events,
            e => new ArchivedEvent
            {
                Id = e.Id,
                Timestamp = e.Timestamp,
                Category = e.Category,
                Amount = e.Amount
            });

        var archivedCount = await context.ArchivedEvents.CountAsync();
        Assert.Equal(50, archivedCount);
    }

    private InsertSelectTestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<InsertSelectTestDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new InsertSelectTestDbContext(options);
    }
}

public class InsertSelectTestDbContext : DbContext
{
    public InsertSelectTestDbContext(DbContextOptions<InsertSelectTestDbContext> options) : base(options) { }

    // Events table - source of data
    public DbSet<Event> Events => Set<Event>();

    // ArchivedEvents table - separate entity type
    public DbSet<ArchivedEvent> ArchivedEvents => Set<ArchivedEvent>();

    public DbSet<EntityWithComputed> EntitiesWithComputed => Set<EntityWithComputed>();
    public DbSet<TargetWithComputedEntity> TargetWithComputed => Set<TargetWithComputedEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Configure Event entity
        modelBuilder.Entity<Event>(entity =>
        {
            entity.ToTable("Events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });
        });

        // Configure ArchivedEvent entity
        modelBuilder.Entity<ArchivedEvent>(entity =>
        {
            entity.ToTable("ArchivedEvents");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });
        });

        // Configure EntityWithComputed
        modelBuilder.Entity<EntityWithComputed>(entity =>
        {
            entity.ToTable("EntitiesWithComputed");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
            entity.Property(e => e.ComputedValue)
                .HasComputedColumnSql("Value * 2", stored: true);
        });

        // Configure TargetWithComputedEntity
        modelBuilder.Entity<TargetWithComputedEntity>(entity =>
        {
            entity.ToTable("TargetWithComputed");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
            entity.Property(e => e.ComputedValue)
                .HasComputedColumnSql("Value * 2", stored: true);
        });
    }
}

// Event entity
public class Event
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

// Archived event entity - same schema, different table
public class ArchivedEvent
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

// Entity with computed column
public class EntityWithComputed
{
    public Guid Id { get; set; }
    public int Value { get; set; }
    public int ComputedValue { get; set; }
}

// Target entity with computed column - same schema, different table
public class TargetWithComputedEntity
{
    public Guid Id { get; set; }
    public int Value { get; set; }
    public int ComputedValue { get; set; }
}
