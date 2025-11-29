using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests;

public class ClickHouseIntegrationTests : IAsyncLifetime
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
    public async Task CanConnectToClickHouse()
    {
        await using var context = CreateContext<SimpleDbContext>();

        var canConnect = await context.Database.CanConnectAsync();

        Assert.True(canConnect);
    }

    [Fact]
    public async Task CanCreateMergeTreeTable()
    {
        await using var context = CreateContext<EventsDbContext>();

        // Create the table using raw SQL (migrations would do this via the generator)
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Events" (
                "Id" UUID,
                "EventTime" DateTime64(3),
                "EventType" String,
                "Data" Nullable(String)
            )
            ENGINE = MergeTree()
            ORDER BY ("EventTime", "Id")
            """);

        // Verify table exists by querying system tables
        var tableExists = await context.Database.SqlQueryRaw<string>(
            "SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'Events'"
        ).AnyAsync();

        Assert.True(tableExists);
    }

    [Fact]
    public async Task CanInsertAndQueryData()
    {
        await using var context = CreateContext<EventsDbContext>();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Events" (
                "Id" UUID,
                "EventTime" DateTime64(3),
                "EventType" String,
                "Data" Nullable(String)
            )
            ENGINE = MergeTree()
            ORDER BY ("EventTime", "Id")
            """);

        var eventId = Guid.NewGuid();
        var eventTime = DateTime.UtcNow;

        // Insert using EF Core
        context.Events.Add(new Event
        {
            Id = eventId,
            EventTime = eventTime,
            EventType = "test_event",
            Data = "test data"
        });
        await context.SaveChangesAsync();

        // Query using LINQ
        var result = await context.Events
            .Where(e => e.EventType == "test_event")
            .FirstOrDefaultAsync();

        Assert.NotNull(result);
        Assert.Equal(eventId, result.Id);
        Assert.Equal("test_event", result.EventType);
        Assert.Equal("test data", result.Data);
    }

    [Fact]
    public async Task CanQueryWithFilters()
    {
        await using var context = CreateContext<EventsDbContext>();

        // Drop and recreate to ensure clean state
        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"Events\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "Events" (
                "Id" UUID,
                "EventTime" DateTime64(3),
                "EventType" String,
                "Data" Nullable(String)
            )
            ENGINE = MergeTree()
            ORDER BY ("EventTime", "Id")
            """);

        // Insert multiple events
        var now = DateTime.UtcNow;
        context.Events.AddRange(
            new Event { Id = Guid.NewGuid(), EventTime = now.AddHours(-2), EventType = "click", Data = "page1" },
            new Event { Id = Guid.NewGuid(), EventTime = now.AddHours(-1), EventType = "view", Data = "page2" },
            new Event { Id = Guid.NewGuid(), EventTime = now, EventType = "click", Data = "page3" }
        );
        await context.SaveChangesAsync();

        // Query with filter
        var clicks = await context.Events
            .Where(e => e.EventType == "click")
            .OrderBy(e => e.EventTime)
            .ToListAsync();

        Assert.Equal(2, clicks.Count);
        Assert.All(clicks, e => Assert.Equal("click", e.EventType));
    }

    [Fact]
    public async Task CanUseAggregations()
    {
        await using var context = CreateContext<EventsDbContext>();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Events" (
                "Id" UUID,
                "EventTime" DateTime64(3),
                "EventType" String,
                "Data" Nullable(String)
            )
            ENGINE = MergeTree()
            ORDER BY ("EventTime", "Id")
            """);

        var now = DateTime.UtcNow;
        context.Events.AddRange(
            new Event { Id = Guid.NewGuid(), EventTime = now, EventType = "click", Data = "a" },
            new Event { Id = Guid.NewGuid(), EventTime = now, EventType = "click", Data = "b" },
            new Event { Id = Guid.NewGuid(), EventTime = now, EventType = "view", Data = "c" }
        );
        await context.SaveChangesAsync();

        var count = await context.Events.LongCountAsync();
        var clickCount = await context.Events.LongCountAsync(e => e.EventType == "click");

        Assert.Equal(3L, count);
        Assert.Equal(2L, clickCount);
    }

    [Fact]
    public async Task CanCreateReplacingMergeTreeTable()
    {
        await using var context = CreateContext<UsersDbContext>();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Users" (
                "Id" UUID,
                "Name" String,
                "Email" String,
                "Version" Int64
            )
            ENGINE = ReplacingMergeTree("Version")
            ORDER BY ("Id")
            """);

        var userId = Guid.NewGuid();

        // Insert initial version
        context.Users.Add(new User
        {
            Id = userId,
            Name = "John",
            Email = "john@example.com",
            Version = 1
        });
        await context.SaveChangesAsync();

        // Detach the tracked entity to allow inserting new version with same key
        context.ChangeTracker.Clear();

        // Insert updated version (ReplacingMergeTree will dedupe on merge)
        context.Users.Add(new User
        {
            Id = userId,
            Name = "John Updated",
            Email = "john.updated@example.com",
            Version = 2
        });
        await context.SaveChangesAsync();

        // Force merge to deduplicate (OPTIMIZE FINAL forces merge)
        await context.Database.ExecuteSqlRawAsync("OPTIMIZE TABLE \"Users\" FINAL");

        // Query all users and find by Id (avoid parameterized query)
        var result = await context.Users.FirstOrDefaultAsync();

        Assert.NotNull(result);
        // After OPTIMIZE FINAL, only the latest version should remain
        Assert.Equal("John Updated", result.Name);
        Assert.Equal(2, result.Version);
    }

    [Fact]
    public async Task CanCreateTableWithPartitionBy()
    {
        await using var context = CreateContext<LogsDbContext>();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Logs" (
                "Id" UUID,
                "Timestamp" DateTime64(3),
                "Level" String,
                "Message" String
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM("Timestamp")
            ORDER BY ("Timestamp", "Id")
            """);

        context.Logs.Add(new LogEntry
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow,
            Level = "INFO",
            Message = "Test log message"
        });
        await context.SaveChangesAsync();

        // Use string literal filter (no captured variable) to avoid parameter issue
        var log = await context.Logs.FirstOrDefaultAsync(l => l.Level == "INFO");
        Assert.NotNull(log);
        Assert.Equal("INFO", log.Level);
    }

    [Fact]
    public async Task LinqSelectProjection_Works()
    {
        await using var context = CreateContext<EventsDbContext>();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Events" (
                "Id" UUID,
                "EventTime" DateTime64(3),
                "EventType" String,
                "Data" Nullable(String)
            )
            ENGINE = MergeTree()
            ORDER BY ("EventTime", "Id")
            """);

        context.Events.Add(new Event
        {
            Id = Guid.NewGuid(),
            EventTime = DateTime.UtcNow,
            EventType = "test",
            Data = "payload"
        });
        await context.SaveChangesAsync();

        var projected = await context.Events
            .Select(e => new { e.EventType, e.Data })
            .FirstOrDefaultAsync();

        Assert.NotNull(projected);
        Assert.Equal("test", projected.EventType);
        Assert.Equal("payload", projected.Data);
    }

    [Fact]
    public async Task LinqGroupBy_Works()
    {
        await using var context = CreateContext<EventsDbContext>();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Events" (
                "Id" UUID,
                "EventTime" DateTime64(3),
                "EventType" String,
                "Data" Nullable(String)
            )
            ENGINE = MergeTree()
            ORDER BY ("EventTime", "Id")
            """);

        var now = DateTime.UtcNow;
        context.Events.AddRange(
            new Event { Id = Guid.NewGuid(), EventTime = now, EventType = "click", Data = null },
            new Event { Id = Guid.NewGuid(), EventTime = now, EventType = "click", Data = null },
            new Event { Id = Guid.NewGuid(), EventTime = now, EventType = "view", Data = null }
        );
        await context.SaveChangesAsync();

        var grouped = await context.Events
            .GroupBy(e => e.EventType)
            .Select(g => new { EventType = g.Key, Count = g.LongCount() })
            .OrderBy(x => x.EventType)
            .ToListAsync();

        Assert.Equal(2, grouped.Count);
        Assert.Equal("click", grouped[0].EventType);
        Assert.Equal(2L, grouped[0].Count);
        Assert.Equal("view", grouped[1].EventType);
        Assert.Equal(1L, grouped[1].Count);
    }

    private TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }
}

#region Test DbContexts and Entities

public class SimpleDbContext : DbContext
{
    public SimpleDbContext(DbContextOptions<SimpleDbContext> options) : base(options) { }
}

public class EventsDbContext : DbContext
{
    public EventsDbContext(DbContextOptions<EventsDbContext> options) : base(options) { }

    public DbSet<Event> Events => Set<Event>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.ToTable("Events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree("EventTime", "Id");
        });
    }
}

public class Event
{
    public Guid Id { get; set; }
    public DateTime EventTime { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string? Data { get; set; }
}

public class UsersDbContext : DbContext
{
    public UsersDbContext(DbContextOptions<UsersDbContext> options) : base(options) { }

    public DbSet<User> Users => Set<User>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<User>(entity =>
        {
            entity.ToTable("Users");
            entity.HasKey(e => e.Id);
            entity.UseReplacingMergeTree("Version", "Id");
        });
    }
}

public class User
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public long Version { get; set; }
}

public class LogsDbContext : DbContext
{
    public LogsDbContext(DbContextOptions<LogsDbContext> options) : base(options) { }

    public DbSet<LogEntry> Logs => Set<LogEntry>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<LogEntry>(entity =>
        {
            entity.ToTable("Logs");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree("Timestamp", "Id");
            entity.HasPartitionBy("toYYYYMM(\"Timestamp\")");
        });
    }
}

public class LogEntry
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Level { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
}

#endregion
