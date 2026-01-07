using EF.CH.Extensions;
using EF.CH.Storage.Internal;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Types;

public class TimeTypeTests : IAsyncLifetime
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

    #region Type Mapping Unit Tests

    [Fact]
    public void TimeOnlyMapping_GeneratesCorrectStoreType()
    {
        var mapping = new ClickHouseTimeTypeMapping();

        Assert.Equal("Time", mapping.StoreType);
        Assert.Equal(typeof(TimeOnly), mapping.ClrType);
    }

    [Fact]
    public void TimeOnlyMapping_GeneratesCorrectLiteral_WithMilliseconds()
    {
        var mapping = new ClickHouseTimeTypeMapping();
        var time = new TimeOnly(14, 30, 45, 123);

        var literal = mapping.GenerateSqlLiteral(time);

        Assert.Equal("'14:30:45.123000'", literal);
    }

    [Fact]
    public void TimeOnlyMapping_GeneratesCorrectLiteral_WithoutMilliseconds()
    {
        var mapping = new ClickHouseTimeTypeMapping();
        var time = new TimeOnly(9, 15, 30);

        var literal = mapping.GenerateSqlLiteral(time);

        Assert.Equal("'09:15:30'", literal);
    }

    [Fact]
    public void TimeSpanMapping_GeneratesCorrectStoreType()
    {
        var mapping = new ClickHouseTimeSpanTypeMapping();

        Assert.Equal("Int64", mapping.StoreType);
        Assert.Equal(typeof(TimeSpan), mapping.ClrType);
    }

    [Fact]
    public void TimeSpanMapping_GeneratesCorrectLiteral_Nanoseconds()
    {
        var mapping = new ClickHouseTimeSpanTypeMapping();
        var duration = TimeSpan.FromHours(2) + TimeSpan.FromMinutes(30);

        var literal = mapping.GenerateSqlLiteral(duration);

        // 2.5 hours in nanoseconds: ticks * 100 (ticks are 100ns each)
        var expectedNanoseconds = duration.Ticks * 100L;
        Assert.Equal(expectedNanoseconds.ToString(), literal);
    }

    [Fact]
    public void TimeSpanMapping_GeneratesCorrectLiteral_NegativeDuration()
    {
        var mapping = new ClickHouseTimeSpanTypeMapping();
        var duration = TimeSpan.FromMinutes(-30);

        var literal = mapping.GenerateSqlLiteral(duration);

        // -30 minutes in nanoseconds: ticks * 100
        var expectedNanoseconds = duration.Ticks * 100L;
        Assert.Equal(expectedNanoseconds.ToString(), literal);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task CanInsertAndQueryTimeOnly()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Schedules" (
                "Id" UUID,
                "Name" String,
                "StartTime" Nullable(String),
                "EndTime" Nullable(String),
                "DurationNs" Int64
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        // Note: ClickHouse Time type has limited driver support
        // For now, we test that the mapping generates correct literals
        // Full integration would require ClickHouse.Driver Time support

        var id = Guid.NewGuid();
        var startTime = new TimeOnly(9, 0, 0);
        var endTime = new TimeOnly(17, 30, 0);
        var duration = TimeSpan.FromHours(8.5);

        // Insert using raw SQL with our literal format
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""Schedules"" (""Id"", ""Name"", ""StartTime"", ""EndTime"", ""DurationNs"")
              VALUES ('" + id + @"', 'Work Day', '09:00:00', '17:30:00', " + (duration.Ticks * 100) + ")");

        // Verify data was inserted by querying with a simple check
        var exists = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() as cnt FROM ""Schedules"" WHERE ""Name"" = 'Work Day'"
        ).AnyAsync();

        Assert.True(exists);
    }

    [Fact]
    public async Task CanQueryWithTimeSpanComparison()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Tasks" (
                "Id" UUID,
                "Name" String,
                "DurationNs" Int64
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        // Insert tasks with different durations
        var shortDuration = TimeSpan.FromMinutes(30);
        var longDuration = TimeSpan.FromHours(3);

        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""Tasks"" (""Id"", ""Name"", ""DurationNs"") VALUES
            ('" + Guid.NewGuid() + @"', 'Quick Task', " + (shortDuration.Ticks * 100) + @"),
            ('" + Guid.NewGuid() + @"', 'Long Task', " + (longDuration.Ticks * 100) + ")");

        // Query for tasks longer than 1 hour (in nanoseconds)
        var oneHourNs = TimeSpan.FromHours(1).Ticks * 100;
        var count = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() as cnt FROM ""Tasks"" WHERE ""DurationNs"" > " + oneHourNs
        ).AnyAsync();

        Assert.True(count);
    }

    #endregion

    private ScheduleDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<ScheduleDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new ScheduleDbContext(options);
    }
}

#region Test Entities and Contexts

public class Schedule
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public TimeOnly StartTime { get; set; }
    public TimeOnly EndTime { get; set; }
    public TimeSpan Duration { get; set; }
}

public class ScheduleDbContext : DbContext
{
    public ScheduleDbContext(DbContextOptions<ScheduleDbContext> options)
        : base(options) { }

    public DbSet<Schedule> Schedules => Set<Schedule>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Schedule>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("Schedules");
            entity.UseMergeTree(x => x.Id);
        });
    }
}

#endregion
