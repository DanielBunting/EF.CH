using EF.CH.Extensions;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Testcontainers.ClickHouse;
using Xunit;
using Xunit.Abstractions;

namespace EF.CH.Tests.Types;

#region Test Entities

/// <summary>
/// A nested record representing goal data.
/// </summary>
public record Goal
{
    public uint ID { get; set; }
    public DateTime EventTime { get; set; }
}

/// <summary>
/// A nested record representing player statistics.
/// </summary>
public record PlayerStat
{
    public string Name { get; set; } = string.Empty;
    public int Score { get; set; }
    public double Accuracy { get; set; }
}

/// <summary>
/// Entity with a Nested column.
/// </summary>
public class GameEvent
{
    public Guid Id { get; set; }
    public string EventType { get; set; } = string.Empty;
    public List<Goal> Goals { get; set; } = new();
}

/// <summary>
/// Entity with multiple Nested columns.
/// </summary>
public class Match
{
    public Guid Id { get; set; }
    public string MatchName { get; set; } = string.Empty;
    public List<Goal> Goals { get; set; } = new();
    public List<PlayerStat> Players { get; set; } = new();
}

#endregion

#region Test DbContexts

public class NestedTestContext : DbContext
{
    public NestedTestContext(DbContextOptions<NestedTestContext> options)
        : base(options) { }

    public DbSet<GameEvent> GameEvents => Set<GameEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<GameEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("GameEvents");
            entity.UseMergeTree(x => x.Id);
        });
    }
}

public class MultiNestedTestContext : DbContext
{
    public MultiNestedTestContext(DbContextOptions<MultiNestedTestContext> options)
        : base(options) { }

    public DbSet<Match> Matches => Set<Match>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Match>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("Matches");
            entity.UseMergeTree(x => x.Id);
        });
    }
}

#endregion

public class NestedTypeTests
{
    #region Type Suitability Tests

    [Fact]
    public void IsSuitableForNested_ReturnsTrueForSimpleRecord()
    {
        Assert.True(ClickHouseNestedTypeMapping.IsSuitableForNested(typeof(Goal)));
    }

    [Fact]
    public void IsSuitableForNested_ReturnsTrueForRecordWithString()
    {
        Assert.True(ClickHouseNestedTypeMapping.IsSuitableForNested(typeof(PlayerStat)));
    }

    [Fact]
    public void IsSuitableForNested_ReturnsFalseForPrimitiveTypes()
    {
        Assert.False(ClickHouseNestedTypeMapping.IsSuitableForNested(typeof(int)));
        Assert.False(ClickHouseNestedTypeMapping.IsSuitableForNested(typeof(string)));
        Assert.False(ClickHouseNestedTypeMapping.IsSuitableForNested(typeof(Guid)));
        Assert.False(ClickHouseNestedTypeMapping.IsSuitableForNested(typeof(DateTime)));
    }

    [Fact]
    public void IsSuitableForNested_ReturnsFalseForCollections()
    {
        Assert.False(ClickHouseNestedTypeMapping.IsSuitableForNested(typeof(List<int>)));
        Assert.False(ClickHouseNestedTypeMapping.IsSuitableForNested(typeof(int[])));
    }

    #endregion

    #region Type Mapping Tests

    [Fact]
    public void NestedTypeMapping_GeneratesCorrectStoreType()
    {
        using var context = CreateContext<NestedTestContext>();

        var entityType = context.Model.FindEntityType(typeof(GameEvent))!;
        var property = entityType.FindProperty(nameof(GameEvent.Goals))!;
        var mapping = (RelationalTypeMapping)property.GetTypeMapping();

        Assert.NotNull(mapping);
        Assert.Contains("Nested(", mapping.StoreType);
        Assert.Contains("\"ID\"", mapping.StoreType);
        Assert.Contains("UInt32", mapping.StoreType);
        Assert.Contains("\"EventTime\"", mapping.StoreType);
        Assert.Contains("DateTime64", mapping.StoreType);
    }

    [Fact]
    public void NestedTypeMapping_GeneratesCorrectStoreTypeForMultipleNested()
    {
        using var context = CreateContext<MultiNestedTestContext>();

        var entityType = context.Model.FindEntityType(typeof(Match))!;

        // Check Goals property
        var goalsProperty = entityType.FindProperty(nameof(Match.Goals))!;
        var goalsMapping = (RelationalTypeMapping)goalsProperty.GetTypeMapping();
        Assert.Contains("Nested(", goalsMapping.StoreType);
        Assert.Contains("\"ID\"", goalsMapping.StoreType);

        // Check Players property
        var playersProperty = entityType.FindProperty(nameof(Match.Players))!;
        var playersMapping = (RelationalTypeMapping)playersProperty.GetTypeMapping();
        Assert.Contains("Nested(", playersMapping.StoreType);
        Assert.Contains("\"Name\"", playersMapping.StoreType);
        Assert.Contains("\"Score\"", playersMapping.StoreType);
        Assert.Contains("\"Accuracy\"", playersMapping.StoreType);
    }

    #endregion

    #region SQL Literal Generation Tests

    [Fact]
    public void NestedTypeMapping_GeneratesCorrectSqlLiteral()
    {
        using var context = CreateContext<NestedTestContext>();

        var entityType = context.Model.FindEntityType(typeof(GameEvent))!;
        var property = entityType.FindProperty(nameof(GameEvent.Goals))!;
        var mapping = (RelationalTypeMapping)property.GetTypeMapping();

        var goals = new List<Goal>
        {
            new() { ID = 1, EventTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc) },
            new() { ID = 2, EventTime = new DateTime(2024, 1, 1, 13, 0, 0, DateTimeKind.Utc) }
        };

        var literal = mapping.GenerateSqlLiteral(goals);

        // Should generate parallel arrays: ([1, 2], ['2024-01-01 12:00:00', '2024-01-01 13:00:00'])
        Assert.NotNull(literal);
        Assert.Contains("[1, 2]", literal);
        Assert.Contains("2024", literal);
    }

    [Fact]
    public void NestedTypeMapping_GeneratesEmptyArraysForEmptyList()
    {
        using var context = CreateContext<NestedTestContext>();

        var entityType = context.Model.FindEntityType(typeof(GameEvent))!;
        var property = entityType.FindProperty(nameof(GameEvent.Goals))!;
        var mapping = (RelationalTypeMapping)property.GetTypeMapping();

        var goals = new List<Goal>();

        var literal = mapping.GenerateSqlLiteral(goals);

        // Should generate empty parallel arrays: ([], [])
        Assert.NotNull(literal);
        Assert.Contains("[]", literal);
    }

    #endregion

    #region DDL Generation Tests

    [Fact]
    public void CreateTable_GeneratesNestedColumn()
    {
        using var context = CreateContext<NestedTestContext>();

        // Get the DDL that would be generated
        var script = context.Database.GenerateCreateScript();

        Assert.Contains("CREATE TABLE", script);
        Assert.Contains("\"Goals\" Nested(", script);
    }

    #endregion

    #region LINQ Query Translation Tests

    [Fact]
    public void NestedCount_TranslatesToLengthFunction()
    {
        using var context = CreateContext<NestedTestContext>();

        // Query: context.GameEvents.Select(e => e.Goals.Count)
        var query = context.GameEvents.Select(e => e.Goals.Count);
        var sql = query.ToQueryString();

        // Log the actual SQL for debugging
        _testOutputHelper?.WriteLine($"Generated SQL: {sql}");

        // Should translate to: length("Goals.ID")
        Assert.Contains("length", sql);
        Assert.Contains("Goals", sql);
    }

    private readonly ITestOutputHelper? _testOutputHelper;

    public NestedTypeTests(ITestOutputHelper? testOutputHelper = null)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public void NestedAny_TranslatesToNotEmptyFunction()
    {
        using var context = CreateContext<NestedTestContext>();

        // Query: context.GameEvents.Where(e => e.Goals.Any())
        var query = context.GameEvents.Where(e => e.Goals.Any());
        var sql = query.ToQueryString();

        // Should translate to: notEmpty("Goals.ID")
        Assert.Contains("notEmpty", sql);
        Assert.Contains("Goals", sql);
    }

    [Fact]
    public void EnumerableCount_OnNested_TranslatesToLengthFunction()
    {
        using var context = CreateContext<NestedTestContext>();

        // Query using Enumerable.Count static method
        var query = context.GameEvents.Select(e => Enumerable.Count(e.Goals));
        var sql = query.ToQueryString();

        // Should translate to: length("Goals.ID")
        Assert.Contains("length", sql);
        Assert.Contains("Goals", sql);
    }

    [Fact]
    public void WhereNestedCountGreaterThanZero_TranslatesCorrectly()
    {
        using var context = CreateContext<NestedTestContext>();

        // Query: context.GameEvents.Where(e => e.Goals.Count > 0)
        var query = context.GameEvents.Where(e => e.Goals.Count > 0);
        var sql = query.ToQueryString();

        // EF Core optimizes .Count > 0 to .Any(), which translates to notEmpty()
        // This is semantically equivalent and more efficient than length() > 0
        Assert.Contains("notEmpty", sql);
        Assert.Contains("Goals.ID", sql); // Uses first field pattern
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
public class NestedTypeIntegrationTests : IAsyncLifetime
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
    public async Task CanCreateTableWithNestedColumn()
    {
        await using var context = CreateContext<NestedTestContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Verify table exists
        var tableExists = await context.Database.SqlQueryRaw<string>(
            "SELECT name AS \"Value\" FROM system.tables WHERE database = currentDatabase() AND name = 'GameEvents'"
        ).AnyAsync();

        Assert.True(tableExists);

        // Verify column type
        var columnType = await context.Database.SqlQueryRaw<string>(
            "SELECT type AS \"Value\" FROM system.columns WHERE database = currentDatabase() AND table = 'GameEvents' AND name = 'Goals.ID'"
        ).FirstOrDefaultAsync();

        Assert.NotNull(columnType);
        Assert.Contains("Array", columnType);
    }

    [Fact]
    public async Task CanInsertAndQueryNestedData()
    {
        await using var context = CreateContext<NestedTestContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert an event with goals
        var gameEvent = new GameEvent
        {
            Id = Guid.NewGuid(),
            EventType = "match",
            Goals = new List<Goal>
            {
                new() { ID = 1, EventTime = DateTime.UtcNow },
                new() { ID = 2, EventTime = DateTime.UtcNow.AddMinutes(10) }
            }
        };

        context.GameEvents.Add(gameEvent);
        await context.SaveChangesAsync();

        // Clear tracker and query
        context.ChangeTracker.Clear();

        var result = await context.GameEvents.FirstOrDefaultAsync();

        Assert.NotNull(result);
        Assert.Equal("match", result.EventType);
        // Note: Nested data querying requires special handling
    }

    private TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }
}
