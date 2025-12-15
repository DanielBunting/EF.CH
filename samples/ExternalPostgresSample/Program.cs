using EF.CH.Extensions;
using EF.CH.External;
using Microsoft.EntityFrameworkCore;

// ============================================================
// External PostgreSQL Sample
// ============================================================
// Demonstrates querying PostgreSQL tables directly from ClickHouse
// using the postgresql() table function. This allows federated
// queries that JOIN ClickHouse analytics with PostgreSQL master data.
// ============================================================

Console.WriteLine("External PostgreSQL Sample");
Console.WriteLine("==========================\n");

// ============================================================
// Prerequisites
// ============================================================

Console.WriteLine("Prerequisites:");
Console.WriteLine("""
   1. ClickHouse server running (localhost:8123)
   2. PostgreSQL server running (localhost:5432)
   3. Set environment variables:
      - PG_HOST=localhost:5432
      - PG_DATABASE=mydb
      - PG_USER=postgres
      - PG_PASSWORD=your_password
   """);

// ============================================================
// Entity Definitions
// ============================================================

Console.WriteLine("\n--- Entity Definitions ---\n");

Console.WriteLine("External PostgreSQL entity (keyless):");
Console.WriteLine("""
   public class ExternalCustomer
   {
       public int id { get; set; }
       public string name { get; set; } = string.Empty;
       public string email { get; set; } = string.Empty;
   }
   """);

Console.WriteLine("\nNative ClickHouse entity:");
Console.WriteLine("""
   public class Order
   {
       public Guid Id { get; set; }
       public int CustomerId { get; set; }
       public decimal Amount { get; set; }
       public DateTime OrderDate { get; set; }
   }
   """);

// ============================================================
// Configuration Examples
// ============================================================

Console.WriteLine("\n--- Configuration Examples ---\n");

Console.WriteLine("1. Basic configuration with environment variables:");
Console.WriteLine("""
   modelBuilder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
       .FromTable("customers", schema: "public")
       .Connection(c => c
           .HostPort(env: "PG_HOST")
           .Database(env: "PG_DATABASE")
           .Credentials("PG_USER", "PG_PASSWORD"))
       .ReadOnly());
   """);

Console.WriteLine("\n2. Configuration with literal values (for testing):");
Console.WriteLine("""
   modelBuilder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
       .FromTable("customers")
       .Connection(c => c
           .HostPort(value: "localhost:5432")
           .Database(value: "mydb")
           .User(value: "postgres")
           .Password(value: "secret")));
   """);

Console.WriteLine("\n3. Configuration profile (from appsettings.json):");
Console.WriteLine("""
   modelBuilder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
       .FromTable("customers")
       .Connection(c => c.UseProfile("production-pg")));

   // appsettings.json:
   {
     "ExternalConnections": {
       "production-pg": {
         "HostPort": "pg.prod.internal:5432",
         "Database": "production",
         "User": "readonly_user",
         "Password": "secret"
       }
     }
   }
   """);

Console.WriteLine("\n4. With INSERT support:");
Console.WriteLine("""
   modelBuilder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
       .FromTable("customers")
       .Connection(c => c.UseProfile("pg-write"))
       .AllowInserts());  // Enable INSERT INTO FUNCTION
   """);

// ============================================================
// Query Examples
// ============================================================

Console.WriteLine("\n--- Query Examples ---\n");

Console.WriteLine("1. Basic query:");
Console.WriteLine("""
   // LINQ:
   var customers = await context.ExternalCustomers
       .Where(c => c.name.StartsWith("A"))
       .OrderBy(c => c.name)
       .ToListAsync();

   // Generated SQL:
   SELECT "id", "name", "email"
   FROM postgresql('localhost:5432', 'mydb', 'customers', 'user', 'pass', 'public') AS c
   WHERE "name" LIKE 'A%'
   ORDER BY "name"
   """);

Console.WriteLine("\n2. Projection:");
Console.WriteLine("""
   // LINQ:
   var customerNames = await context.ExternalCustomers
       .Where(c => c.id > 100)
       .Select(c => new { c.id, c.name })
       .ToListAsync();

   // Generated SQL:
   SELECT "id", "name"
   FROM postgresql('localhost:5432', 'mydb', 'customers', 'user', 'pass', 'public') AS c
   WHERE "id" > 100
   """);

Console.WriteLine("\n3. Aggregations:");
Console.WriteLine("""
   // LINQ:
   var count = await context.ExternalCustomers.CountAsync();
   var emails = await context.ExternalCustomers
       .GroupBy(c => c.email.Substring(c.email.IndexOf('@')))
       .Select(g => new { Domain = g.Key, Count = g.Count() })
       .ToListAsync();
   """);

Console.WriteLine("\n4. JOIN with native ClickHouse table:");
Console.WriteLine("""
   // LINQ:
   var orderSummary = await context.Orders
       .Join(
           context.ExternalCustomers,
           o => o.CustomerId,
           c => c.id,
           (o, c) => new { CustomerName = c.name, o.Amount, o.OrderDate })
       .Where(x => x.OrderDate > DateTime.UtcNow.AddDays(-30))
       .GroupBy(x => x.CustomerName)
       .Select(g => new {
           Customer = g.Key,
           TotalAmount = g.Sum(x => x.Amount),
           OrderCount = g.Count()
       })
       .OrderByDescending(x => x.TotalAmount)
       .ToListAsync();

   // Generated SQL:
   SELECT c."name" AS "Customer",
          sum(o."Amount") AS "TotalAmount",
          count() AS "OrderCount"
   FROM "orders" AS o
   INNER JOIN postgresql('localhost:5432', 'mydb', 'customers', 'user', 'pass', 'public') AS c
       ON o."CustomerId" = c."id"
   WHERE o."OrderDate" > subtractDays(now(), 30)
   GROUP BY c."name"
   ORDER BY "TotalAmount" DESC
   """);

// ============================================================
// INSERT Examples
// ============================================================

Console.WriteLine("\n--- INSERT Examples ---\n");

Console.WriteLine("Inserting via raw SQL (keyless entities can't use change tracker):");
Console.WriteLine("""
   // Note: External entities are keyless, so you can't use:
   // context.ExternalCustomers.Add(new ExternalCustomer { ... });

   // Instead, use raw SQL:
   await context.Database.ExecuteSqlRawAsync(
       "INSERT INTO FUNCTION postgresql('host:5432', 'db', 'customers', 'user', 'pass', 'public') " +
       "(name, email) VALUES ('Alice', 'alice@example.com')");

   // Or with parameters:
   await context.Database.ExecuteSqlAsync(
       $"INSERT INTO FUNCTION postgresql(...) (name, email) VALUES ({name}, {email})");
   """);

// ============================================================
// Live Demo (if connection available)
// ============================================================

Console.WriteLine("\n--- Live Demo ---\n");

try
{
    await using var context = new ExternalPostgresSampleContext();

    // Check if we can connect
    Console.WriteLine("Attempting to connect to ClickHouse...");

    // Generate and display the query SQL
    var query = context.ExternalCustomers
        .Where(c => c.name.StartsWith("A"))
        .OrderBy(c => c.name);

    Console.WriteLine($"Generated SQL:\n{query.ToQueryString()}\n");

    // Try to execute (will fail without actual PostgreSQL)
    try
    {
        var customers = await query.Take(5).ToListAsync();
        Console.WriteLine($"Found {customers.Count} customers:");
        foreach (var c in customers)
        {
            Console.WriteLine($"  - {c.name} ({c.email})");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Query execution requires PostgreSQL: {ex.Message}");
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Connection failed: {ex.Message}");
    Console.WriteLine("This is expected if ClickHouse/PostgreSQL aren't running.");
}

// ============================================================
// Best Practices
// ============================================================

Console.WriteLine("\n--- Best Practices ---\n");

Console.WriteLine("""
   1. Use environment variables for credentials in production
      - Never hardcode passwords in source code
      - Use .Connection(c => c.Credentials("USER_ENV", "PASS_ENV"))

   2. Consider data locality for JOINs
      - JOINs between ClickHouse and PostgreSQL involve network latency
      - For frequently accessed data, consider syncing to ClickHouse

   3. Use read-only database users
      - Create PostgreSQL users with SELECT-only permissions
      - Only use AllowInserts() when absolutely necessary

   4. External entities are keyless
      - Can't use change tracker (Add/Update/Remove)
      - Use raw SQL for inserts

   5. Monitor query performance
      - External table functions add network overhead
      - Profile queries to identify bottlenecks

   6. Property naming conventions
      - Match PostgreSQL column names (often lowercase/snake_case)
      - Use entity.Property(x => x.Name).HasColumnName("name") if needed
   """);

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// External PostgreSQL customer entity.
/// This is a keyless entity that maps to a PostgreSQL table.
/// </summary>
public class ExternalCustomer
{
    public int id { get; set; }
    public string name { get; set; } = string.Empty;
    public string email { get; set; } = string.Empty;
}

/// <summary>
/// Native ClickHouse order entity.
/// </summary>
public class Order
{
    public Guid Id { get; set; }
    public int CustomerId { get; set; }
    public decimal Amount { get; set; }
    public DateTime OrderDate { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class ExternalPostgresSampleContext : DbContext
{
    // External PostgreSQL entity
    public DbSet<ExternalCustomer> ExternalCustomers => Set<ExternalCustomer>();

    // Native ClickHouse entity
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=external_pg_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // External PostgreSQL customers
        modelBuilder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers", schema: "public")
            .Connection(c => c
                .HostPort(env: "PG_HOST")
                .Database(env: "PG_DATABASE")
                .Credentials("PG_USER", "PG_PASSWORD"))
            .ReadOnly());

        // Native ClickHouse orders
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("orders");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });
            entity.HasPartitionByMonth(x => x.OrderDate);
        });
    }
}
