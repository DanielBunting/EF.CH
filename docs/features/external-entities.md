# External Entities

External entities allow you to query remote databases (PostgreSQL, MySQL, SQL Server via ODBC, Redis) directly through ClickHouse table functions. Data is read from the remote source at query time—no data is copied to ClickHouse unless you explicitly do so.

## When to Use External Entities

- **Federated queries** - JOIN ClickHouse analytics data with PostgreSQL/MySQL master data
- **Real-time lookups** - Query current state from OLTP databases without ETL lag
- **Cache integration** - Read session data from Redis alongside ClickHouse events
- **Migration bridge** - Gradually move from PostgreSQL/MySQL to ClickHouse

## Supported Providers

| Provider | ClickHouse Function | Connection Style |
|----------|---------------------|------------------|
| PostgreSQL | `postgresql()` | Direct credentials |
| MySQL | `mysql()` | Direct credentials |
| ODBC | `odbc()` | DSN reference (odbc.ini) |
| Redis | `redis()` | Key-value with inline schema |

## PostgreSQL

Query PostgreSQL tables directly from ClickHouse:

```csharp
// Entity class (note: lowercase property names match PostgreSQL conventions)
public class ExternalCustomer
{
    public int id { get; set; }
    public string name { get; set; } = string.Empty;
    public string email { get; set; } = string.Empty;
}

// Configuration in OnModelCreating
modelBuilder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
    .FromTable("customers", schema: "public")  // Remote table and schema
    .Connection(c => c
        .HostPort(env: "PG_HOST")              // "localhost:5432"
        .Database(env: "PG_DATABASE")          // "mydb"
        .Credentials("PG_USER", "PG_PASSWORD")) // User and password env vars
    .ReadOnly());                               // Default - no writes
```

**Connection Options:**

```csharp
// Environment variables (recommended for production)
.Connection(c => c
    .HostPort(env: "PG_HOST")
    .Database(env: "PG_DATABASE")
    .User(env: "PG_USER")
    .Password(env: "PG_PASSWORD"))

// Literal values (for development/testing)
.Connection(c => c
    .HostPort(value: "localhost:5432")
    .Database(value: "mydb")
    .User(value: "postgres")
    .Password(value: "secret"))

// Mix of both
.Connection(c => c
    .HostPort(value: "pg.prod.internal:5432")
    .Database(env: "PG_DATABASE")
    .Credentials("PG_USER", "PG_PASSWORD"))  // Shorthand for User + Password

// Configuration profile (from appsettings.json)
.Connection(c => c.UseProfile("production-pg"))
```

**Configuration Profile Format:**

```json
{
  "ExternalConnections": {
    "production-pg": {
      "HostPort": "pg.prod.internal:5432",
      "Database": "production",
      "User": "readonly_user",
      "Password": "secret",
      "Schema": "public"
    }
  }
}
```

**Generated SQL:**

```sql
SELECT "id", "name", "email"
FROM postgresql('localhost:5432', 'mydb', 'customers', 'postgres', 'secret', 'public') AS c
WHERE "name" LIKE 'A%'
```

**Enabling Writes:**

```csharp
modelBuilder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
    .FromTable("customers")
    .Connection(c => c.UseProfile("pg-write"))
    .AllowInserts());  // Enable INSERT INTO FUNCTION

// Insert via raw SQL (EF change tracker doesn't work with keyless entities)
await context.Database.ExecuteSqlRawAsync(
    "INSERT INTO FUNCTION postgresql('host', 'db', 'customers', 'user', 'pass', 'public') " +
    "(name, email) VALUES ('Alice', 'alice@example.com')");
```

## MySQL

Similar to PostgreSQL, with MySQL-specific options:

```csharp
modelBuilder.ExternalMySqlEntity<ExternalProduct>(ext => ext
    .FromTable("products")
    .Connection(c => c
        .HostPort(env: "MYSQL_HOST")        // "localhost:3306"
        .Database(env: "MYSQL_DATABASE")
        .Credentials("MYSQL_USER", "MYSQL_PASSWORD")));
```

**MySQL-Specific Options:**

```csharp
// Use REPLACE INTO instead of INSERT (for upsert semantics)
modelBuilder.ExternalMySqlEntity<Inventory>(ext => ext
    .FromTable("inventory")
    .Connection(c => c.UseProfile("mysql-prod"))
    .AllowInserts()
    .UseReplaceForInserts());

// Use ON DUPLICATE KEY UPDATE
modelBuilder.ExternalMySqlEntity<Inventory>(ext => ext
    .FromTable("inventory")
    .Connection(c => c.UseProfile("mysql-prod"))
    .AllowInserts()
    .OnDuplicateKey("UPDATE quantity = VALUES(quantity), updated_at = NOW()"));
```

**Generated SQL:**

```sql
SELECT "id", "sku", "name", "price"
FROM mysql('localhost:3306', 'mydb', 'products', 'user', 'pass') AS p
WHERE "price" > 20
```

## ODBC (SQL Server, Oracle, etc.)

ODBC uses pre-configured Data Source Names (DSN) from `/etc/odbc.ini`:

```csharp
modelBuilder.ExternalOdbcEntity<ExternalSalesData>(ext => ext
    .FromTable("sales")
    .Dsn(env: "MSSQL_DSN")     // DSN name from odbc.ini
    .Database("reporting"));   // Database within the DSN
```

**odbc.ini Configuration (on ClickHouse server):**

```ini
[MsSqlProd]
Driver = /opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.3.so.2.1
Server = sql-server.prod.internal
Port = 1433
Database = analytics
TrustServerCertificate = yes
```

**Generated SQL:**

```sql
SELECT "SalesId", "Territory", "Revenue"
FROM odbc('MsSqlProd', 'reporting', 'sales') AS s
WHERE "Revenue" > 10000
```

**Note:** ODBC requires the `clickhouse-odbc-bridge` process and appropriate ODBC drivers installed on the ClickHouse server.

## Redis

Redis is a key-value store, so configuration differs from relational databases:

```csharp
// Entity for Redis data
public class SessionCache
{
    public string SessionId { get; set; } = string.Empty;
    public ulong UserId { get; set; }
    public string Data { get; set; } = string.Empty;
    public DateTime ExpiresAt { get; set; }
}

// Configuration - structure auto-generated from entity properties
modelBuilder.ExternalRedisEntity<SessionCache>(ext => ext
    .KeyColumn(x => x.SessionId)  // Required: which property is the Redis key
    .Connection(c => c
        .HostPort(env: "REDIS_HOST")
        .Password(env: "REDIS_PASSWORD")
        .DbIndex(0)));            // Redis database 0-15
```

**Explicit Structure (for custom type mapping):**

```csharp
modelBuilder.ExternalRedisEntity<RateLimitEntry>(ext => ext
    .KeyColumn("ip_address")
    .Structure("ip_address String, count UInt32, expires_at DateTime64(3)")
    .Connection(c => c
        .HostPort(value: "redis:6379")
        .PoolSize(32)));          // Connection pool size
```

**Auto-Generated Structure:**

When `.Structure()` is not specified, it's generated from entity properties:

| .NET Type | ClickHouse Type |
|-----------|-----------------|
| `string` | `String` |
| `int` | `Int32` |
| `long` | `Int64` |
| `uint` | `UInt32` |
| `ulong` | `UInt64` |
| `float` | `Float32` |
| `double` | `Float64` |
| `decimal` | `Decimal(18, 4)` |
| `bool` | `Bool` |
| `Guid` | `UUID` |
| `DateTime` | `DateTime64(3)` |

**Generated SQL:**

```sql
SELECT "SessionId", "UserId", "Data", "ExpiresAt"
FROM redis('localhost:6379', 'SessionId', 'SessionId String, UserId UInt64, Data String, ExpiresAt DateTime64(3)', 0, 'password') AS s
WHERE "UserId" = 12345
```

## JOINing External and Native Tables

A common pattern is joining external master data with ClickHouse analytics:

```csharp
// External PostgreSQL customers
modelBuilder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
    .FromTable("customers")
    .Connection(c => c.UseProfile("pg-prod")));

// Native ClickHouse orders
modelBuilder.Entity<Order>(entity =>
{
    entity.UseMergeTree(x => new { x.OrderDate, x.Id });
});

// Query joining both
var customerOrders = await context.Orders
    .Join(
        context.ExternalCustomers,
        o => o.CustomerId,
        c => c.id,
        (o, c) => new { CustomerName = c.name, o.Amount, o.OrderDate })
    .Where(x => x.OrderDate > DateTime.UtcNow.AddDays(-30))
    .GroupBy(x => x.CustomerName)
    .Select(g => new { Customer = g.Key, TotalAmount = g.Sum(x => x.Amount) })
    .ToListAsync();
```

**Generated SQL:**

```sql
SELECT c."name" AS "Customer", sum(o."Amount") AS "TotalAmount"
FROM "orders" AS o
INNER JOIN postgresql('pg.prod:5432', 'prod', 'customers', 'user', 'pass', 'public') AS c
    ON o."CustomerId" = c."id"
WHERE o."OrderDate" > subtractDays(now(), 30)
GROUP BY c."name"
```

## Key-ID Resolver Service

For scenarios where you need to map external string/GUID keys to local integer IDs (optimizing storage and JOINs):

```csharp
// 1. Create resolver implementation
public class CustomerIdResolver : ExternalKeyIdResolverBase<string, uint, MyContext>
{
    public CustomerIdResolver(MyContext context)
        : base(context, new ExternalKeyIdResolverOptions
        {
            DictionaryName = "customer_id_map",       // ClickHouse dictionary
            MappingTableName = "customer_id_mappings" // ReplacingMergeTree table
        })
    {
    }
}

// 2. Expose from DbContext
public class MyContext : DbContext
{
    private CustomerIdResolver? _customerIdResolver;

    public IExternalKeyIdResolver<string, uint> CustomerIds
        => _customerIdResolver ??= new CustomerIdResolver(this);
}

// 3. Use in application code
public async Task ProcessOrder(string externalCustomerId, decimal amount)
{
    // Get or create local ID for external customer
    var localId = await _context.CustomerIds.GetOrCreateIdAsync(externalCustomerId);

    // Store order with efficient integer foreign key
    _context.Orders.Add(new Order
    {
        Id = Guid.NewGuid(),
        CustomerId = localId,  // uint instead of string
        Amount = amount
    });
    await _context.SaveChangesAsync();
}
```

**How It Works:**

1. `TryGetIdAsync()` - Fast dictionary lookup via `dictHas()`/`dictGet()`
2. `GetOrCreateIdAsync()` - If not found, generates new ID, inserts mapping, reloads dictionary
3. Dictionary backed by ReplacingMergeTree for persistence

**Configuration Options:**

```csharp
new ExternalKeyIdResolverOptions
{
    DictionaryName = "customer_id_map",
    MappingTableName = "customer_id_mappings",
    MappingTableDatabase = "default",     // Database for mapping table
    IdType = "UInt32",                    // ClickHouse ID type
    KeyColumnName = "Key",                // Column name for external key
    IdColumnName = "Id",                  // Column name for local ID
    MaxRetries = 2,                       // Retries for dictionary reload
    RetryDelay = TimeSpan.FromMilliseconds(100)
}
```

## Limitations

### External entities are keyless

External entities use table functions, not physical tables. They're automatically marked `HasNoKey()`:

```csharp
// This throws - keyless entities can't be tracked
context.ExternalCustomers.Add(new ExternalCustomer { ... });

// Use raw SQL for inserts instead
await context.Database.ExecuteSqlRawAsync(
    "INSERT INTO FUNCTION postgresql(...) VALUES (...)");
```

### No transactions

ClickHouse doesn't support transactions. Inserts to external sources are individual operations.

### Performance considerations

- Each query executes against the remote database in real-time
- No caching between queries
- Network latency affects query performance
- Consider materializing frequently-accessed data to ClickHouse

### Read-only by default

External entities are read-only unless you call `.AllowInserts()`. This is a safety feature.

### Connection credentials at query time

Credentials are resolved when queries execute, not at model creation. Ensure environment variables or configuration are available at runtime.

## Best Practices

1. **Use environment variables** for credentials in production
2. **Consider data locality** - JOIN performance depends on network latency
3. **Materialize hot data** - For frequently accessed external data, consider periodic sync to ClickHouse
4. **Use read-only connections** - Create database users with minimal permissions
5. **Monitor query performance** - External table functions add network overhead
6. **Test with realistic data volumes** - Performance varies with data size and network conditions
