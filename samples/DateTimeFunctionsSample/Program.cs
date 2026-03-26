using EF.CH;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using EfClass = Microsoft.EntityFrameworkCore.EF;

// ============================================================
// Date/Time Functions Sample
// ============================================================
// Demonstrates ClickHouse date/time functions exposed via EF.Functions:
// - Date truncation (toStartOfSecond, toStartOfTenMinutes)
// - Unix timestamp conversion (toUnixTimestamp, fromUnixTimestamp)
// - Relative number functions (toRelativeMonthNum, toRelativeDayNum, etc.)
// - Date arithmetic (date_add, date_sub, age)
// - Interval-based bucketing (toStartOfInterval)
// ============================================================

Console.WriteLine("Date/Time Functions Sample");
Console.WriteLine("=========================\n");

await using var context = new EventDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert sample data
Console.WriteLine("Inserting sample data...\n");

var events = new List<Event>
{
    new()
    {
        Id = 1,
        Name = "deploy-v1.0",
        Timestamp = new DateTime(2025, 3, 15, 10, 23, 45),
        StartTime = new DateTime(2025, 3, 15, 10, 0, 0),
        EndTime = new DateTime(2025, 3, 15, 10, 23, 45),
        UnixTs = 1742036625
    },
    new()
    {
        Id = 2,
        Name = "deploy-v1.1",
        Timestamp = new DateTime(2025, 3, 15, 14, 37, 12),
        StartTime = new DateTime(2025, 1, 1, 0, 0, 0),
        EndTime = new DateTime(2025, 3, 15, 14, 37, 12),
        UnixTs = 1742051832
    },
    new()
    {
        Id = 3,
        Name = "rollback",
        Timestamp = new DateTime(2025, 3, 16, 2, 5, 30),
        StartTime = new DateTime(2025, 3, 15, 14, 37, 12),
        EndTime = new DateTime(2025, 3, 16, 2, 5, 30),
        UnixTs = 1742093130
    }
};

context.Events.AddRange(events);
await context.SaveChangesAsync();
Console.WriteLine($"Inserted {events.Count} events.\n");

// ============================================================
// 1. Date Truncation
// ============================================================
Console.WriteLine("=== 1. Date Truncation ===\n");

var truncated = await context.Events
    .Select(e => new
    {
        e.Name,
        Second = EfClass.Functions.ToStartOfSecond(e.Timestamp),
        TenMin = EfClass.Functions.ToStartOfTenMinutes(e.Timestamp)
    })
    .ToListAsync();

Console.WriteLine("Truncated timestamps:");
foreach (var row in truncated)
{
    Console.WriteLine($"  {row.Name}: second={row.Second:HH:mm:ss}, tenMin={row.TenMin:HH:mm}");
}

// ============================================================
// 2. Unix Timestamp Conversion
// ============================================================
Console.WriteLine("\n=== 2. Unix Timestamps ===\n");

var timestamps = await context.Events
    .Select(e => new
    {
        e.Name,
        Unix = EfClass.Functions.ToUnixTimestamp(e.Timestamp),
        FromUnix = EfClass.Functions.FromUnixTimestamp(e.UnixTs)
    })
    .ToListAsync();

Console.WriteLine("Unix timestamp round-trips:");
foreach (var row in timestamps)
{
    Console.WriteLine($"  {row.Name}: toUnix={row.Unix}, fromUnix={row.FromUnix:yyyy-MM-dd HH:mm:ss}");
}

// ============================================================
// 3. Relative Number Functions
// ============================================================
Console.WriteLine("\n=== 3. Relative Numbers ===\n");

var relativeNums = await context.Events
    .Select(e => new
    {
        e.Name,
        MonthNum = EfClass.Functions.ToRelativeMonthNum(e.Timestamp),
        DayNum = EfClass.Functions.ToRelativeDayNum(e.Timestamp),
        HourNum = EfClass.Functions.ToRelativeHourNum(e.Timestamp)
    })
    .ToListAsync();

Console.WriteLine("Relative numbers from epoch:");
foreach (var row in relativeNums)
{
    Console.WriteLine($"  {row.Name}: month={row.MonthNum}, day={row.DayNum}, hour={row.HourNum}");
}

// ============================================================
// 4. Date Arithmetic (date_add / date_sub / age)
// ============================================================
Console.WriteLine("\n=== 4. Date Arithmetic ===\n");

var arithmetic = await context.Events
    .Select(e => new
    {
        e.Name,
        Plus7Days = EfClass.Functions.DateAdd(ClickHouseIntervalUnit.Day, 7, e.Timestamp),
        Minus1Hour = EfClass.Functions.DateSub(ClickHouseIntervalUnit.Hour, 1, e.Timestamp),
        AgeMonths = EfClass.Functions.Age(ClickHouseIntervalUnit.Month, e.StartTime, e.EndTime)
    })
    .ToListAsync();

Console.WriteLine("Date arithmetic:");
foreach (var row in arithmetic)
{
    Console.WriteLine($"  {row.Name}: +7d={row.Plus7Days:yyyy-MM-dd}, -1h={row.Minus1Hour:HH:mm}, age={row.AgeMonths} months");
}

// ============================================================
// 5. Interval-Based Bucketing (toStartOfInterval)
// ============================================================
Console.WriteLine("\n=== 5. toStartOfInterval ===\n");

var buckets15s = await context.Events
    .Select(e => new
    {
        e.Name,
        Bucket15s = EfClass.Functions.ToStartOfInterval(e.Timestamp, 15, ClickHouseIntervalUnit.Second),
        Bucket5m = EfClass.Functions.ToStartOfInterval(e.Timestamp, 5, ClickHouseIntervalUnit.Minute),
        Bucket2h = EfClass.Functions.ToStartOfInterval(e.Timestamp, 2, ClickHouseIntervalUnit.Hour)
    })
    .ToListAsync();

Console.WriteLine("Interval-based bucketing:");
foreach (var row in buckets15s)
{
    Console.WriteLine($"  {row.Name}: 15s={row.Bucket15s:HH:mm:ss}, 5m={row.Bucket5m:HH:mm}, 2h={row.Bucket2h:HH:mm}");
}

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definitions
// ============================================================

public class Event
{
    public uint Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public long UnixTs { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class EventDbContext : DbContext
{
    public DbSet<Event> Events => Set<Event>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=datetime_functions_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.ToTable("Events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });
        });
    }
}
