// TypesSample - ClickHouse type system demonstrations with EF.CH
//
// Demonstrates:
// - Arrays (string[], List<int>) with Contains and Length queries
// - Maps (Dictionary<string,string>) with ContainsKey and Keys queries
// - Enums with automatic Enum8 mapping
// - JSON (JsonElement) with GetPath<T> and HasPath queries (requires CH 24.8+)
// - Nested types (List<TRecord>) mapped to ClickHouse Nested columns
// - IP addresses (ClickHouseIPv4) with IP function queries
// - Tuples ((string, int)) mapped to ClickHouse Tuple type

using System.Text.Json;
using EF.CH;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;

var container = new ClickHouseBuilder()
    .WithImage("clickhouse/clickhouse-server:latest")
    .Build();

Console.WriteLine("Starting ClickHouse container...");
await container.StartAsync();
Console.WriteLine("ClickHouse container started.\n");

try
{
    var connectionString = container.GetConnectionString();

    Console.WriteLine("=== EF.CH Types Sample ===");
    Console.WriteLine();

    await DemoArraysAsync(connectionString);
    await DemoMapsAsync(connectionString);
    await DemoEnumsAsync(connectionString);
    await DemoJsonAsync(connectionString);
    await DemoNestedAsync(connectionString);
    await DemoIpAddressesAsync(connectionString);
    await DemoTuplesAsync(connectionString);

    Console.WriteLine("=== Done ===");
}
finally
{
    Console.WriteLine("\nStopping container...");
    await container.DisposeAsync();
    Console.WriteLine("Done.");
}

// ---------------------------------------------------------------------------
// 1. Arrays
// ---------------------------------------------------------------------------

static async Task DemoArraysAsync(string connectionString)
{
    Console.WriteLine("[1] Arrays (string[], List<int>)");
    Console.WriteLine("    ClickHouse type: Array(String), Array(Int32)");
    Console.WriteLine();

    await using var context = new ArrayDbContext(connectionString);
    await context.Database.EnsureCreatedAsync();

    context.Articles.AddRange(
        new Article { Id = Guid.NewGuid(), Title = "EF Core Guide", Tags = ["csharp", "efcore", "dotnet"], Scores = [95, 88, 72] },
        new Article { Id = Guid.NewGuid(), Title = "ClickHouse Tips", Tags = ["clickhouse", "analytics"], Scores = [100, 90] },
        new Article { Id = Guid.NewGuid(), Title = "LINQ Deep Dive", Tags = ["csharp", "linq"], Scores = [85] }
    );
    await context.SaveChangesAsync();

    // Query: articles tagged with "csharp"
    var csharpArticles = await context.Articles
        .Where(a => a.Tags.Contains("csharp"))
        .OrderBy(a => a.Title)
        .ToListAsync();

    Console.WriteLine("    Articles tagged 'csharp':");
    foreach (var a in csharpArticles)
    {
        Console.WriteLine($"      - {a.Title} (tags: [{string.Join(", ", a.Tags)}], scores count: {a.Scores.Count})");
    }
    Console.WriteLine();

    await context.Database.EnsureDeletedAsync();
}

// ---------------------------------------------------------------------------
// 2. Maps
// ---------------------------------------------------------------------------

static async Task DemoMapsAsync(string connectionString)
{
    Console.WriteLine("[2] Maps (Dictionary<string, string>)");
    Console.WriteLine("    ClickHouse type: Map(String, String)");
    Console.WriteLine();

    await using var context = new MapDbContext(connectionString);
    await context.Database.EnsureCreatedAsync();

    context.Configs.AddRange(
        new ConfigEntry
        {
            Id = Guid.NewGuid(),
            Name = "web-server",
            Metadata = new Dictionary<string, string> { ["env"] = "production", ["region"] = "us-east-1" }
        },
        new ConfigEntry
        {
            Id = Guid.NewGuid(),
            Name = "worker",
            Metadata = new Dictionary<string, string> { ["env"] = "staging", ["queue"] = "high-priority" }
        }
    );
    await context.SaveChangesAsync();

    // Query: entries that have a "region" key
    var withRegion = await context.Configs
        .Where(c => c.Metadata.ContainsKey("region"))
        .ToListAsync();

    Console.WriteLine("    Entries with 'region' key:");
    foreach (var c in withRegion)
    {
        Console.WriteLine($"      - {c.Name}: region={c.Metadata["region"]}");
    }
    Console.WriteLine();

    await context.Database.EnsureDeletedAsync();
}

// ---------------------------------------------------------------------------
// 3. Enums
// ---------------------------------------------------------------------------

static async Task DemoEnumsAsync(string connectionString)
{
    Console.WriteLine("[3] Enums (Priority enum)");
    Console.WriteLine("    ClickHouse type: Enum8('Low'=0, 'Medium'=1, 'High'=2)");
    Console.WriteLine();

    await using var context = new EnumDbContext(connectionString);
    await context.Database.EnsureCreatedAsync();

    context.Tickets.AddRange(
        new Ticket { Id = Guid.NewGuid(), Summary = "Fix login bug", Priority = Priority.High },
        new Ticket { Id = Guid.NewGuid(), Summary = "Update docs", Priority = Priority.Low },
        new Ticket { Id = Guid.NewGuid(), Summary = "Add caching", Priority = Priority.Medium },
        new Ticket { Id = Guid.NewGuid(), Summary = "Security patch", Priority = Priority.High }
    );
    await context.SaveChangesAsync();

    // Query: high-priority tickets
    var highPriority = await context.Tickets
        .Where(t => t.Priority == Priority.High)
        .OrderBy(t => t.Summary)
        .ToListAsync();

    Console.WriteLine("    High-priority tickets:");
    foreach (var t in highPriority)
    {
        Console.WriteLine($"      - {t.Summary} ({t.Priority})");
    }
    Console.WriteLine();

    await context.Database.EnsureDeletedAsync();
}

// ---------------------------------------------------------------------------
// 4. JSON (requires ClickHouse 24.8+)
// ---------------------------------------------------------------------------

static async Task DemoJsonAsync(string connectionString)
{
    Console.WriteLine("[4] JSON (JsonElement)");
    Console.WriteLine("    ClickHouse type: JSON (requires CH 24.8+)");
    Console.WriteLine();

    await using var context = new JsonDbContext(connectionString);
    await context.Database.EnsureCreatedAsync();

    context.Telemetry.AddRange(
        new TelemetryEvent
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow,
            Data = JsonDocument.Parse("""{"user": {"name": "Alice", "plan": "pro"}, "score": 95}""").RootElement
        },
        new TelemetryEvent
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow.AddMinutes(-5),
            Data = JsonDocument.Parse("""{"user": {"name": "Bob"}, "score": 42}""").RootElement
        }
    );
    await context.SaveChangesAsync();

    // Query: extract typed values from JSON paths
    var results = await context.Telemetry
        .Select(e => new
        {
            UserName = e.Data.GetPath<string>("user.name"),
            Score = e.Data.GetPath<long>("score"),
            HasPlan = e.Data.HasPath("user.plan")
        })
        .ToListAsync();

    Console.WriteLine("    Telemetry data:");
    foreach (var r in results)
    {
        Console.WriteLine($"      - User={r.UserName}, Score={r.Score}, HasPlan={r.HasPlan}");
    }
    Console.WriteLine();

    await context.Database.EnsureDeletedAsync();
}

// ---------------------------------------------------------------------------
// 5. Nested types
// ---------------------------------------------------------------------------

static async Task DemoNestedAsync(string connectionString)
{
    Console.WriteLine("[5] Nested (List<AddressRecord>)");
    Console.WriteLine("    ClickHouse type: Nested(Street String, City String)");
    Console.WriteLine();

    await using var context = new NestedDbContext(connectionString);
    await context.Database.EnsureCreatedAsync();

    context.Contacts.AddRange(
        new Contact
        {
            Id = Guid.NewGuid(),
            Name = "Alice",
            Addresses =
            [
                new AddressRecord { Street = "123 Main St", City = "Springfield" },
                new AddressRecord { Street = "456 Oak Ave", City = "Shelbyville" }
            ]
        },
        new Contact
        {
            Id = Guid.NewGuid(),
            Name = "Bob",
            Addresses = [new AddressRecord { Street = "789 Elm Blvd", City = "Capital City" }]
        }
    );
    await context.SaveChangesAsync();

    // Query: all contacts
    var contacts = await context.Contacts
        .OrderBy(c => c.Name)
        .ToListAsync();

    Console.WriteLine("    Contacts:");
    foreach (var c in contacts)
    {
        Console.WriteLine($"      - {c.Name} ({c.Addresses.Count} address(es))");
        foreach (var addr in c.Addresses)
        {
            Console.WriteLine($"          {addr.Street}, {addr.City}");
        }
    }
    Console.WriteLine();

    await context.Database.EnsureDeletedAsync();
}

// ---------------------------------------------------------------------------
// 6. IP Addresses
// ---------------------------------------------------------------------------

static async Task DemoIpAddressesAsync(string connectionString)
{
    Console.WriteLine("[6] IP Addresses (ClickHouseIPv4)");
    Console.WriteLine("    ClickHouse type: IPv4");
    Console.WriteLine();

    await using var context = new IpDbContext(connectionString);
    await context.Database.EnsureCreatedAsync();

    context.AccessLogs.AddRange(
        new AccessLog { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, ClientIp = ClickHouseIPv4.Parse("192.168.1.1"), Path = "/api/users" },
        new AccessLog { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, ClientIp = ClickHouseIPv4.Parse("10.0.0.50"), Path = "/api/orders" },
        new AccessLog { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, ClientIp = ClickHouseIPv4.Parse("192.168.1.100"), Path = "/api/users" },
        new AccessLog { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow, ClientIp = ClickHouseIPv4.Parse("172.16.0.5"), Path = "/api/health" }
    );
    await context.SaveChangesAsync();

    // Query: filter by IP address value
    var targetIp = ClickHouseIPv4.Parse("192.168.1.1");
    var matchingLogs = await context.AccessLogs
        .Where(l => l.ClientIp == targetIp)
        .ToListAsync();

    Console.WriteLine($"    Access logs from {targetIp}:");
    foreach (var l in matchingLogs)
    {
        Console.WriteLine($"      - {l.ClientIp} -> {l.Path}");
    }

    // Query all and show IP addresses
    var allLogs = await context.AccessLogs
        .OrderBy(l => l.Path)
        .ToListAsync();

    Console.WriteLine("    All access logs:");
    foreach (var l in allLogs)
    {
        Console.WriteLine($"      - {l.ClientIp} -> {l.Path}");
    }
    Console.WriteLine();

    await context.Database.EnsureDeletedAsync();
}

// ---------------------------------------------------------------------------
// 7. Tuples
// ---------------------------------------------------------------------------

static async Task DemoTuplesAsync(string connectionString)
{
    Console.WriteLine("[7] Tuples ((string, int))");
    Console.WriteLine("    ClickHouse type: Tuple(String, Int32)");
    Console.WriteLine();

    await using var context = new TupleDbContext(connectionString);
    await context.Database.EnsureCreatedAsync();

    context.Measurements.AddRange(
        new Measurement { Id = Guid.NewGuid(), Label = "Temperature", Location = ("Building A", 3) },
        new Measurement { Id = Guid.NewGuid(), Label = "Humidity", Location = ("Building B", 1) },
        new Measurement { Id = Guid.NewGuid(), Label = "Pressure", Location = ("Building A", 2) }
    );
    await context.SaveChangesAsync();

    // Query: all measurements
    var all = await context.Measurements
        .OrderBy(m => m.Label)
        .ToListAsync();

    Console.WriteLine("    Measurements:");
    foreach (var m in all)
    {
        Console.WriteLine($"      - {m.Label}: Building={m.Location.Item1}, Floor={m.Location.Item2}");
    }
    Console.WriteLine();

    await context.Database.EnsureDeletedAsync();
}

// ===========================================================================
// Entity and DbContext definitions
// ===========================================================================

// --- Arrays ---

public class Article
{
    public Guid Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public string[] Tags { get; set; } = [];
    public List<int> Scores { get; set; } = [];
}

public class ArrayDbContext(string connectionString) : DbContext
{
    public DbSet<Article> Articles => Set<Article>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Article>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });
    }
}

// --- Maps ---

public class ConfigEntry
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public Dictionary<string, string> Metadata { get; set; } = new();
}

public class MapDbContext(string connectionString) : DbContext
{
    public DbSet<ConfigEntry> Configs => Set<ConfigEntry>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ConfigEntry>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });
    }
}

// --- Enums ---

public enum Priority
{
    Low = 0,
    Medium = 1,
    High = 2
}

public class Ticket
{
    public Guid Id { get; set; }
    public string Summary { get; set; } = string.Empty;
    public Priority Priority { get; set; }
}

public class EnumDbContext(string connectionString) : DbContext
{
    public DbSet<Ticket> Tickets => Set<Ticket>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Ticket>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });
    }
}

// --- JSON ---

public class TelemetryEvent
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public JsonElement Data { get; set; }
}

public class JsonDbContext(string connectionString) : DbContext
{
    public DbSet<TelemetryEvent> Telemetry => Set<TelemetryEvent>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TelemetryEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });
        });
    }
}

// --- Nested ---

public record AddressRecord
{
    public string Street { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
}

public class Contact
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public List<AddressRecord> Addresses { get; set; } = [];
}

public class NestedDbContext(string connectionString) : DbContext
{
    public DbSet<Contact> Contacts => Set<Contact>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Contact>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });
    }
}

// --- IP Addresses ---

public class AccessLog
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public ClickHouseIPv4 ClientIp { get; set; }
    public string Path { get; set; } = string.Empty;
}

public class IpDbContext(string connectionString) : DbContext
{
    public DbSet<AccessLog> AccessLogs => Set<AccessLog>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AccessLog>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });
        });
    }
}

// --- Tuples ---

public class Measurement
{
    public Guid Id { get; set; }
    public string Label { get; set; } = string.Empty;
    public (string, int) Location { get; set; }
}

public class TupleDbContext(string connectionString) : DbContext
{
    public DbSet<Measurement> Measurements => Set<Measurement>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Measurement>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });
    }
}
