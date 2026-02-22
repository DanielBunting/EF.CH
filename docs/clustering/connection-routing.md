# Connection Routing

Connection routing splits read and write operations across different ClickHouse endpoints. SELECT queries go to read replicas while INSERT, UPDATE, DELETE, and DDL operations go to the write primary. This improves read throughput and reduces load on the primary node.

## Enabling Connection Routing

Enable routing and define connections at the options level.

```csharp
options.UseClickHouse("Host=localhost", o => o
    .UseConnectionRouting()
    .AddConnection("Primary", conn => conn
        .Database("production")
        .WriteEndpoint("dc1-clickhouse:8123")
        .ReadEndpoints("dc2-clickhouse:8123", "dc1-clickhouse:8123")
        .ReadStrategy(ReadStrategy.PreferFirst)
    )
);
```

With this configuration:
- All `SELECT` queries route to `dc2-clickhouse:8123` first (the preferred read endpoint)
- All `INSERT`, `UPDATE`, `DELETE`, and `ALTER` operations route to `dc1-clickhouse:8123`

## AddConnection

The `AddConnection` builder configures a named connection with separate read and write endpoints.

```csharp
.AddConnection("Primary", conn => conn
    .Database("production")
    .WriteEndpoint("dc1-clickhouse:8123")
    .ReadEndpoints("dc2-clickhouse:8123", "dc3-clickhouse:8123")
    .ReadStrategy(ReadStrategy.RoundRobin)
    .Credentials("app_user", "password")
)
```

### Configuration Options

| Method | Description |
|--------|-------------|
| `Database(name)` | The database name on the endpoint |
| `WriteEndpoint(host)` | The endpoint for write operations (host:port) |
| `ReadEndpoints(hosts)` | One or more endpoints for read operations |
| `ReadStrategy(strategy)` | How to select among multiple read endpoints |
| `Credentials(user, pass)` | Authentication credentials |
| `WithFailover(config)` | Failover settings |

### Read Strategies

| Strategy | Behavior |
|----------|----------|
| `PreferFirst` | Always uses the first read endpoint; falls back to others on failure |
| `RoundRobin` | Distributes reads evenly across all read endpoints |
| `Random` | Randomly selects a read endpoint for each query |

## Failover Configuration

Configure automatic failover when an endpoint becomes unreachable.

```csharp
.AddConnection("Primary", conn => conn
    .WriteEndpoint("dc1-clickhouse:8123")
    .ReadEndpoints("dc2-clickhouse:8123", "dc1-clickhouse:8123")
    .WithFailover(failover => failover
        .Enabled()
        .MaxRetries(3)
        .RetryDelayMs(1000)
        .HealthCheckIntervalMs(30000)
    )
)
```

| Setting | Default | Description |
|---------|---------|-------------|
| `Enabled()` | `false` | Enables automatic failover |
| `MaxRetries(n)` | `3` | Maximum retry attempts before failing |
| `RetryDelayMs(ms)` | `1000` | Delay between retry attempts |
| `HealthCheckIntervalMs(ms)` | `30000` | Interval for background health checks |

## Configuration from appsettings.json

Connection routing can be defined entirely in configuration.

```json
{
    "ClickHouse": {
        "Connections": {
            "Primary": {
                "Database": "production",
                "WriteEndpoint": "dc1-clickhouse:8123",
                "ReadEndpoints": [
                    "dc2-clickhouse:8123",
                    "dc1-clickhouse:8123"
                ],
                "ReadStrategy": "PreferFirst",
                "Username": "app_user",
                "Password": "secret",
                "Failover": {
                    "Enabled": true,
                    "MaxRetries": 3,
                    "RetryDelayMs": 1000,
                    "HealthCheckIntervalMs": 30000
                }
            }
        }
    }
}
```

```csharp
options.UseClickHouse("Host=localhost", o => o
    .FromConfiguration(config.GetSection("ClickHouse"))
    .UseConnectionRouting()
);
```

## Multiple Named Connections

Define multiple connections for different workloads or data centers.

```csharp
options.UseClickHouse("Host=localhost", o => o
    .UseConnectionRouting()
    .AddConnection("Primary", conn => conn
        .Database("production")
        .WriteEndpoint("dc1-clickhouse:8123")
        .ReadEndpoints("dc1-clickhouse:8123"))
    .AddConnection("Analytics", conn => conn
        .Database("analytics")
        .WriteEndpoint("dc1-analytics:8123")
        .ReadEndpoints("dc2-analytics:8123", "dc3-analytics:8123")
        .ReadStrategy(ReadStrategy.RoundRobin))
);
```

## Full Example

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<AppDbContext>(options =>
{
    options.UseClickHouse("Host=localhost", o => o
        .FromConfiguration(builder.Configuration.GetSection("ClickHouse"))
        .UseConnectionRouting()
        .UseCluster("geo_cluster")
        .AddConnection("Primary", conn => conn
            .Database("production")
            .WriteEndpoint("dc1-clickhouse:8123")
            .ReadEndpoints("dc2-clickhouse:8123", "dc1-clickhouse:8123")
            .ReadStrategy(ReadStrategy.PreferFirst)
            .WithFailover(f => f
                .Enabled()
                .MaxRetries(3)
                .RetryDelayMs(500)))
    );
});
```

> **Note:** The connection string passed to `UseClickHouse` serves as the default connection. When connection routing is enabled, it is used as a fallback if no named connection matches the operation type.

## See Also

- [Clustering Overview](overview.md) -- architecture and decision guide
- [Cluster DDL](cluster-ddl.md) -- ON CLUSTER and table groups
- [Replicated Engines](replicated-engines.md) -- engine configuration for replication
