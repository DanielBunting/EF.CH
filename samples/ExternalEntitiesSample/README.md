# External Entities Sample

Demonstrates querying external data sources (PostgreSQL, Redis) directly from ClickHouse using EF.CH.

## What This Sample Shows

1. **PostgreSQL external entity** - Query a PostgreSQL table directly from ClickHouse via the `postgresql()` table function, configured with `ExternalPostgresEntity`
2. **Redis external entity** - Query Redis key-value data via the `redis()` table function, configured with `ExternalRedisEntity`
3. **Cross-engine JOINs** - Join external PostgreSQL data with native ClickHouse tables in a single LINQ query

## Prerequisites

- .NET 8.0 SDK
- Docker and Docker Compose

## Running

```bash
# Start the infrastructure (ClickHouse, PostgreSQL, Redis)
cd samples/ExternalEntitiesSample
docker compose up -d

# Wait a few seconds for services to initialize, then run
dotnet run

# Clean up
docker compose down
```

## Infrastructure

The `docker-compose.yml` provides:

| Service    | Port | Purpose                                  |
|------------|------|------------------------------------------|
| ClickHouse | 8123 | Query engine                             |
| PostgreSQL | 5432 | External data source (customers table)   |
| Redis      | 6379 | External data source (session cache)     |

PostgreSQL is initialized with `init/postgres/01-schema.sql` and `init/postgres/02-seed.sql`.

## Key Concepts

### External Entities vs Regular Entities

Regular EF.CH entities map to ClickHouse tables. External entities map to table functions that query remote databases. No ClickHouse table is created for external entities.

### Connection Configuration

Credentials are resolved from environment variables at runtime (not stored in migration files):

```csharp
modelBuilder.ExternalPostgresEntity<Customer>(ext => ext
    .FromTable("customers", schema: "public")
    .Connection(c => c
        .HostPort(env: "PG_HOSTPORT")
        .Database(env: "PG_DATABASE")
        .Credentials("PG_USER", "PG_PASSWORD"))
    .ReadOnly());
```

### Cross-Engine JOINs

ClickHouse can join data from different engines transparently:

```csharp
var enriched = await context.Orders          // ClickHouse native table
    .Join(context.Customers,                 // PostgreSQL external entity
        o => o.CustomerId, c => c.Id,
        (o, c) => new { o.Id, c.Name, o.Amount })
    .ToListAsync();
```
