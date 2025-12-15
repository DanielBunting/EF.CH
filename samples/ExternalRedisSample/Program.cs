using EF.CH.Extensions;
using EF.CH.External;
using Microsoft.EntityFrameworkCore;

// ============================================================
// External Redis Sample
// ============================================================
// Demonstrates querying Redis key-value stores directly from
// ClickHouse using the redis() table function. Useful for
// reading session data, cache entries, or rate limit counters.
// ============================================================

Console.WriteLine("External Redis Sample");
Console.WriteLine("=====================\n");

// ============================================================
// Prerequisites
// ============================================================

Console.WriteLine("Prerequisites:");
Console.WriteLine("""
   1. ClickHouse server running (localhost:8123)
   2. Redis server running (localhost:6379)
   3. Set environment variables:
      - REDIS_HOST=localhost:6379
      - REDIS_PASSWORD=your_password (or empty if no auth)
   """);

// ============================================================
// Redis vs Relational Databases
// ============================================================

Console.WriteLine("\n--- Redis vs Relational Databases ---\n");

Console.WriteLine("""
   Redis is a key-value store, so configuration differs:

   | Aspect           | Relational (PostgreSQL/MySQL) | Redis                |
   |------------------|-------------------------------|----------------------|
   | Table/Collection | Table name                    | No tables            |
   | Primary Key      | Column(s) in table            | KeyColumn() required |
   | Schema           | From database metadata        | Structure() or auto  |
   | Data Format      | Rows and columns              | Hash fields          |
   """);

// ============================================================
// Entity Definitions
// ============================================================

Console.WriteLine("\n--- Entity Definitions ---\n");

Console.WriteLine("Session cache entity:");
Console.WriteLine("""
   public class SessionCache
   {
       public string SessionId { get; set; } = string.Empty;  // Redis key
       public ulong UserId { get; set; }
       public string Data { get; set; } = string.Empty;
       public DateTime ExpiresAt { get; set; }
   }
   """);

Console.WriteLine("\nRate limit entry:");
Console.WriteLine("""
   public class RateLimitEntry
   {
       public string IpAddress { get; set; } = string.Empty;  // Redis key
       public uint RequestCount { get; set; }
       public DateTime WindowStart { get; set; }
   }
   """);

// ============================================================
// Configuration Examples
// ============================================================

Console.WriteLine("\n--- Configuration Examples ---\n");

Console.WriteLine("1. Auto-generated structure (from entity properties):");
Console.WriteLine("""
   modelBuilder.ExternalRedisEntity<SessionCache>(ext => ext
       .KeyColumn(x => x.SessionId)  // Required: which property is the Redis key
       .Connection(c => c
           .HostPort(env: "REDIS_HOST")
           .Password(env: "REDIS_PASSWORD")
           .DbIndex(0))              // Redis database 0-15
       .ReadOnly());

   // Auto-generates structure:
   // SessionId String, UserId UInt64, Data String, ExpiresAt DateTime64(3)
   """);

Console.WriteLine("\n2. Explicit structure (for custom type mapping):");
Console.WriteLine("""
   modelBuilder.ExternalRedisEntity<RateLimitEntry>(ext => ext
       .KeyColumn("IpAddress")
       .Structure("IpAddress String, RequestCount UInt32, WindowStart DateTime64(3)")
       .Connection(c => c
           .HostPort(value: "redis:6379")
           .Password(value: "secret")
           .DbIndex(1)
           .PoolSize(32)));         // Connection pool size
   """);

Console.WriteLine("\n3. With INSERT support:");
Console.WriteLine("""
   modelBuilder.ExternalRedisEntity<SessionCache>(ext => ext
       .KeyColumn(x => x.SessionId)
       .Connection(c => c.UseProfile("redis-write"))
       .AllowInserts());
   """);

// ============================================================
// Type Mapping
// ============================================================

Console.WriteLine("\n--- Type Mapping (Auto-Generated Structure) ---\n");

Console.WriteLine("""
   | .NET Type       | ClickHouse Type  |
   |-----------------|------------------|
   | string          | String           |
   | int             | Int32            |
   | long            | Int64            |
   | uint            | UInt32           |
   | ulong           | UInt64           |
   | float           | Float32          |
   | double          | Float64          |
   | decimal         | Decimal(18, 4)   |
   | bool            | Bool             |
   | Guid            | UUID             |
   | DateTime        | DateTime64(3)    |
   | DateTimeOffset  | DateTime64(3)    |
   | (other)         | String           |
   """);

// ============================================================
// Query Examples
// ============================================================

Console.WriteLine("\n--- Query Examples ---\n");

Console.WriteLine("1. Basic query:");
Console.WriteLine("""
   // LINQ:
   var sessions = await context.Sessions
       .Where(s => s.UserId == 12345)
       .ToListAsync();

   // Generated SQL:
   SELECT "SessionId", "UserId", "Data", "ExpiresAt"
   FROM redis('localhost:6379', 'SessionId',
       'SessionId String, UserId UInt64, Data String, ExpiresAt DateTime64(3)',
       0, 'password') AS s
   WHERE "UserId" = 12345
   """);

Console.WriteLine("\n2. Check session validity:");
Console.WriteLine("""
   // LINQ:
   var validSessions = await context.Sessions
       .Where(s => s.ExpiresAt > DateTime.UtcNow)
       .Select(s => new { s.SessionId, s.UserId })
       .ToListAsync();
   """);

Console.WriteLine("\n3. Count sessions per user:");
Console.WriteLine("""
   // LINQ:
   var sessionCounts = await context.Sessions
       .GroupBy(s => s.UserId)
       .Select(g => new { UserId = g.Key, SessionCount = g.Count() })
       .OrderByDescending(x => x.SessionCount)
       .Take(10)
       .ToListAsync();
   """);

Console.WriteLine("\n4. JOIN with ClickHouse events:");
Console.WriteLine("""
   // LINQ:
   var userActivity = await context.Events
       .Join(
           context.Sessions,
           e => e.SessionId,
           s => s.SessionId,
           (e, s) => new { e.EventType, s.UserId, e.Timestamp })
       .Where(x => x.Timestamp > DateTime.UtcNow.AddHours(-1))
       .GroupBy(x => x.UserId)
       .Select(g => new { UserId = g.Key, EventCount = g.Count() })
       .ToListAsync();
   """);

// ============================================================
// INSERT Examples
// ============================================================

Console.WriteLine("\n--- INSERT Examples ---\n");

Console.WriteLine("Inserting via raw SQL:");
Console.WriteLine("""
   // Redis supports INSERT INTO FUNCTION
   await context.Database.ExecuteSqlRawAsync(
       "INSERT INTO FUNCTION redis('localhost:6379', 'SessionId', " +
       "'SessionId String, UserId UInt64, Data String', 0, 'pass') " +
       "VALUES ('session-123', 12345, '{\"foo\":\"bar\"}')");
   """);

// ============================================================
// Redis Data Format
// ============================================================

Console.WriteLine("\n--- Redis Data Format ---\n");

Console.WriteLine("""
   ClickHouse reads Redis data as hashes. Each Redis key should be
   a hash with field names matching the structure definition:

   Redis command to set data:
   HSET session-123 SessionId "session-123" UserId "12345" Data '{"foo":"bar"}'

   Or using Redis hash:
   HMSET session-123 SessionId session-123 UserId 12345 Data '{"foo":"bar"}'

   The KeyColumn value (SessionId) should match the Redis key.
   """);

// ============================================================
// Live Demo (if connection available)
// ============================================================

Console.WriteLine("\n--- Live Demo ---\n");

try
{
    await using var context = new ExternalRedisSampleContext();

    Console.WriteLine("Attempting to connect to ClickHouse...");

    // Generate and display the query SQL
    var query = context.Sessions
        .Where(s => s.UserId > 100)
        .OrderBy(s => s.SessionId);

    Console.WriteLine($"Generated SQL:\n{query.ToQueryString()}\n");

    // Try to execute (will fail without actual Redis)
    try
    {
        var sessions = await query.Take(5).ToListAsync();
        Console.WriteLine($"Found {sessions.Count} sessions:");
        foreach (var s in sessions)
        {
            Console.WriteLine($"  - {s.SessionId}: User {s.UserId}");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Query execution requires Redis: {ex.Message}");
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Connection failed: {ex.Message}");
    Console.WriteLine("This is expected if ClickHouse/Redis aren't running.");
}

// ============================================================
// Best Practices
// ============================================================

Console.WriteLine("\n--- Best Practices ---\n");

Console.WriteLine("""
   1. Use KeyColumn() to specify the Redis key property
      - This property's value becomes the Redis key for lookups
      - Required for all Redis external entities

   2. Auto-generate structure for simple entities
      - Let EF.CH infer types from your .NET properties
      - Use explicit Structure() for custom type mapping

   3. Match Redis hash field names to property names
      - Redis HSET field names must match structure column names
      - ClickHouse is case-sensitive

   4. Use DbIndex for logical separation
      - Redis databases 0-15 can separate environments/purposes
      - Default is 0

   5. Consider PoolSize for high-throughput
      - Default is 16 connections
      - Increase for concurrent query loads

   6. External entities are keyless
      - Can't use change tracker (Add/Update/Remove)
      - Use raw SQL or direct Redis client for writes
   """);

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// Session cache stored in Redis.
/// </summary>
public class SessionCache
{
    public string SessionId { get; set; } = string.Empty;
    public ulong UserId { get; set; }
    public string Data { get; set; } = string.Empty;
    public DateTime ExpiresAt { get; set; }
}

/// <summary>
/// Rate limit tracking stored in Redis.
/// </summary>
public class RateLimitEntry
{
    public string IpAddress { get; set; } = string.Empty;
    public uint RequestCount { get; set; }
    public DateTime WindowStart { get; set; }
}

/// <summary>
/// Native ClickHouse event entity.
/// </summary>
public class Event
{
    public Guid Id { get; set; }
    public string SessionId { get; set; } = string.Empty;
    public string EventType { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class ExternalRedisSampleContext : DbContext
{
    // External Redis entities
    public DbSet<SessionCache> Sessions => Set<SessionCache>();
    public DbSet<RateLimitEntry> RateLimits => Set<RateLimitEntry>();

    // Native ClickHouse entity
    public DbSet<Event> Events => Set<Event>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=external_redis_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // External Redis sessions (auto-generated structure)
        modelBuilder.ExternalRedisEntity<SessionCache>(ext => ext
            .KeyColumn(x => x.SessionId)
            .Connection(c => c
                .HostPort(env: "REDIS_HOST")
                .Password(env: "REDIS_PASSWORD")
                .DbIndex(0))
            .ReadOnly());

        // External Redis rate limits (explicit structure)
        modelBuilder.ExternalRedisEntity<RateLimitEntry>(ext => ext
            .KeyColumn(x => x.IpAddress)
            .Structure("IpAddress String, RequestCount UInt32, WindowStart DateTime64(3)")
            .Connection(c => c
                .HostPort(env: "REDIS_HOST")
                .Password(env: "REDIS_PASSWORD")
                .DbIndex(1)));

        // Native ClickHouse events
        modelBuilder.Entity<Event>(entity =>
        {
            entity.ToTable("events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });
            entity.HasPartitionByDay(x => x.Timestamp);
        });
    }
}
