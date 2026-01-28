# Connection Routing (Read/Write Splitting)

EF.CH supports routing queries to different endpoints based on operation type, enabling read scaling and write isolation in multi-node deployments.

## Overview

In a typical production setup, you might want:
- **Writes** to go to a primary node
- **Reads** to be distributed across replicas

```
┌─────────────────────────────────────────────────────────────────┐
│                       EF.CH Application                          │
│                                                                 │
│   INSERT/UPDATE/DELETE ────────► Write Endpoint (Node 1)        │
│                                                                 │
│   SELECT ──────────────────────► Read Endpoints                 │
│                                    ├─► Node 1                   │
│                                    ├─► Node 2                   │
│                                    └─► Node 3                   │
└─────────────────────────────────────────────────────────────────┘
```

## Configuration

### Enable Connection Routing

```csharp
services.AddDbContext<MyDbContext>(options =>
    options.UseClickHouse("Host=primary;Database=myapp", ch =>
    {
        ch.AddConnection("Primary", conn => conn
            .Database("myapp")
            .WriteEndpoint("primary:8123")
            .ReadEndpoints("replica1:8123", "replica2:8123", "replica3:8123"));

        ch.UseConnectionRouting();  // Enable routing
    }));
```

### ConnectionConfig Structure

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Database` | string | `"default"` | Database name |
| `WriteEndpoint` | string | required | Endpoint for write operations |
| `ReadEndpoints` | List&lt;string&gt; | empty | Endpoints for read operations |
| `ReadStrategy` | ReadStrategy | `PreferFirst` | Strategy for selecting read endpoints |
| `Failover` | FailoverConfig | see below | Failover configuration |
| `Username` | string? | null | Authentication username |
| `Password` | string? | null | Authentication password |

### Endpoint Format

Endpoints are specified as `host:port`:

```csharp
.WriteEndpoint("clickhouse-primary:8123")
.ReadEndpoints("clickhouse-replica1:8123", "clickhouse-replica2:8123")
```

## Read Strategies

The `ReadStrategy` determines how read endpoints are selected:

### PreferFirst (Default)

Uses the first healthy endpoint, failing over to others only when needed:

```csharp
conn.ReadStrategy(ReadStrategy.PreferFirst)
```

**Behavior:**
1. Try first read endpoint
2. If unhealthy, try second
3. Continue until a healthy endpoint is found
4. Stick with healthy endpoint until it fails

**Best for:** Consistent routing, minimizing cross-datacenter reads

### RoundRobin

Rotates through healthy endpoints in order:

```csharp
conn.ReadStrategy(ReadStrategy.RoundRobin)
```

**Behavior:**
1. Request 1 → Endpoint A
2. Request 2 → Endpoint B
3. Request 3 → Endpoint C
4. Request 4 → Endpoint A (wraps around)

**Best for:** Load balancing across replicas

### Random

Randomly selects from healthy endpoints:

```csharp
conn.ReadStrategy(ReadStrategy.Random)
```

**Behavior:**
- Each request randomly selects a healthy endpoint
- Provides statistical load distribution

**Best for:** Large clusters where deterministic distribution doesn't matter

## Failover Configuration

Configure automatic failover behavior:

```csharp
ch.AddConnection("Primary", conn => conn
    .WriteEndpoint("primary:8123")
    .ReadEndpoints("replica1:8123", "replica2:8123")
    .WithFailover(f => f
        .Enabled(true)              // Enable failover (default: true)
        .MaxRetries(3)              // Retry attempts (default: 3)
        .RetryDelayMs(1000)         // Delay between retries (default: 1000ms)
        .HealthCheckIntervalMs(30000))); // Health check interval (default: 30000ms)
```

### FailoverConfig Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Enabled` | bool | `true` | Whether failover is enabled |
| `MaxRetries` | int | `3` | Maximum retry attempts |
| `RetryDelayMs` | int | `1000` | Milliseconds between retries |
| `HealthCheckIntervalMs` | int | `30000` | Milliseconds between health checks |

### Health Checking

EF.CH performs periodic health checks against all endpoints:

1. Executes `SELECT 1` against each endpoint
2. Marks endpoints as healthy or unhealthy
3. Routes requests only to healthy endpoints
4. Continuously monitors and updates health status

## How Routing Works

When connection routing is enabled, EF.CH intercepts commands and routes them based on type:

| Operation | Routed To |
|-----------|-----------|
| `SELECT` | Read endpoint |
| `INSERT` | Write endpoint |
| `DELETE` | Write endpoint |
| `ALTER TABLE` | Write endpoint |
| `CREATE TABLE` | Write endpoint |
| `DROP TABLE` | Write endpoint |

### Command Interception

```csharp
// This SELECT goes to a read endpoint
var users = await context.Users.ToListAsync();

// This INSERT goes to the write endpoint
context.Users.Add(new User { Name = "Alice" });
await context.SaveChangesAsync();

// This DELETE goes to the write endpoint
await context.Users
    .Where(u => u.IsDeleted)
    .ExecuteDeleteAsync();
```

## JSON Configuration

Configure connection routing via `appsettings.json`:

```json
{
  "ClickHouse": {
    "Connections": {
      "Primary": {
        "Database": "myapp",
        "WriteEndpoint": "primary.clickhouse.local:8123",
        "ReadEndpoints": [
          "replica1.clickhouse.local:8123",
          "replica2.clickhouse.local:8123"
        ],
        "ReadStrategy": "RoundRobin",
        "Failover": {
          "Enabled": true,
          "MaxRetries": 3,
          "RetryDelayMs": 1000,
          "HealthCheckIntervalMs": 30000
        },
        "Username": "default",
        "Password": ""
      }
    }
  }
}
```

Load and enable routing:

```csharp
options.UseClickHouse("...", ch =>
{
    ch.FromConfiguration(configuration.GetSection("ClickHouse"));
    ch.UseConnectionRouting();
});
```

## Multiple Connections

Define multiple named connections for different use cases:

```csharp
ch.AddConnection("Primary", conn => conn
    .Database("myapp")
    .WriteEndpoint("dc1-primary:8123")
    .ReadEndpoints("dc1-replica1:8123", "dc1-replica2:8123"));

ch.AddConnection("Analytics", conn => conn
    .Database("analytics")
    .WriteEndpoint("dc2-primary:8123")
    .ReadEndpoints("dc2-replica1:8123", "dc2-replica2:8123")
    .ReadStrategy(ReadStrategy.RoundRobin));
```

Assign connections to clusters:

```csharp
ch.AddCluster("main_cluster", cluster => cluster
    .UseConnection("Primary"));

ch.AddCluster("analytics_cluster", cluster => cluster
    .UseConnection("Analytics"));
```

## Complete Example

```csharp
public class AppDbContext : DbContext
{
    public DbSet<User> Users => Set<User>();
    public DbSet<Event> Events => Set<Event>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=myapp", ch =>
        {
            // Define connections with read/write separation
            ch.AddConnection("Primary", conn => conn
                .Database("myapp")
                .WriteEndpoint("primary.clickhouse:8123")
                .ReadEndpoints(
                    "replica1.clickhouse:8123",
                    "replica2.clickhouse:8123",
                    "replica3.clickhouse:8123")
                .ReadStrategy(ReadStrategy.RoundRobin)
                .WithFailover(f => f
                    .MaxRetries(5)
                    .RetryDelayMs(500)));

            // Define cluster using the connection
            ch.AddCluster("main_cluster", cluster => cluster
                .UseConnection("Primary")
                .WithReplication(r => r
                    .ZooKeeperBasePath("/clickhouse/tables/{database}/{table}")));

            // Enable routing
            ch.UseConnectionRouting();
            ch.UseCluster("main_cluster");
        });
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<User>(entity =>
        {
            entity.UseReplicatedReplacingMergeTree(x => x.Version, x => x.Id)
                  .WithCluster("main_cluster")
                  .WithReplication("/clickhouse/tables/{database}/{table}");
        });

        modelBuilder.Entity<Event>(entity =>
        {
            entity.UseReplicatedMergeTree(x => new { x.Timestamp, x.Id })
                  .WithCluster("main_cluster")
                  .WithReplication("/clickhouse/tables/{database}/{table}");
        });
    }
}
```

## Limitations

### No Automatic Write Promotion

EF.CH doesn't automatically promote a read replica to write if the write endpoint fails. Handle this at the infrastructure level:
- Use a load balancer with health checks
- Configure DNS failover
- Use a service mesh with traffic management

### Eventual Consistency

Data written to the write endpoint may take time to replicate to read endpoints:

```csharp
// Insert on write endpoint
context.Users.Add(new User { Name = "Bob" });
await context.SaveChangesAsync();

// Immediate read might miss the new user
// (data hasn't replicated yet)
var user = await context.Users
    .FirstOrDefaultAsync(u => u.Name == "Bob");  // Might be null!
```

**Mitigation strategies:**
1. Read from write endpoint for critical reads after writes
2. Add appropriate delays for eventual consistency
3. Use FINAL modifier for ReplacingMergeTree tables

### Transaction Scope

ClickHouse doesn't support distributed transactions. Each command executes independently:

```csharp
// These are NOT atomic across endpoints
context.Users.Add(new User { Name = "Alice" });  // Goes to write
context.Events.Add(new Event { Type = "signup" }); // Goes to write
await context.SaveChangesAsync();  // Batched to write, but not transactional
```

## See Also

- [Clustering and Replication](clustering.md) - Full clustering setup
- [Replicated Engines](replicated-engines.md) - Engine configuration
- [ClusterSample](../../samples/ClusterSample/) - Working example
