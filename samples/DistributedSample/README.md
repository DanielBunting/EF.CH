# EF.CH Distributed Table Sample

This sample demonstrates how to use EF.CH with a 3-node ClickHouse cluster using **sharding** (Distributed tables) to split data across nodes.

## Sharding vs Replication

| Aspect | ClusterSample (Replication) | DistributedSample (Sharding) |
|--------|----------------------------|------------------------------|
| Data location | ALL data on ALL nodes | Data SPLIT across nodes |
| Purpose | High availability, redundancy | Horizontal scaling |
| Cluster config | 1 shard × 3 replicas | 3 shards × 1 replica |
| Use case | Read scaling, fault tolerance | Large datasets, write scaling |

```
Replication (ClusterSample):          Sharding (This Sample):
┌─────────┐ ┌─────────┐ ┌─────────┐   ┌─────────┐ ┌─────────┐ ┌─────────┐
│ Node 1  │ │ Node 2  │ │ Node 3  │   │ Shard 1 │ │ Shard 2 │ │ Shard 3 │
│ ALL     │ │ ALL     │ │ ALL     │   │ Events  │ │ Events  │ │ Events  │
│ DATA    │ │ DATA    │ │ DATA    │   │ 1-33    │ │ 34-66   │ │ 67-100  │
└─────────┘ └─────────┘ └─────────┘   └─────────┘ └─────────┘ └─────────┘
     Same data on all nodes                 Data distributed by hash
                                                    ↓
                                         ┌─────────────────────┐
                                         │ Distributed Table   │
                                         │ (Unified View)      │
                                         └─────────────────────┘
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        ClickHouse Sharded Cluster                        │
│                                                                         │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                │
│   │ clickhouse1 │    │ clickhouse2 │    │ clickhouse3 │                │
│   │  :8123 HTTP │    │  :8124 HTTP │    │  :8125 HTTP │                │
│   │  :9001 TCP  │    │  :9002 TCP  │    │  :9003 TCP  │                │
│   │  Shard 1    │    │  Shard 2    │    │  Shard 3    │                │
│   │  Keeper #1  │    │  Keeper #2  │    │  Keeper #3  │                │
│   └──────┬──────┘    └──────┬──────┘    └──────┬──────┘                │
│          │                  │                  │                        │
│          └──────────────────┴──────────────────┘                        │
│                    Distributed Table Layer                              │
│                                                                         │
│   Features:                                                             │
│   - 3 shards, 1 replica each (data split across nodes)                 │
│   - Distributed table provides unified view                            │
│   - cityHash64(UserId) sharding for even distribution                  │
│   - Integrated ClickHouse Keeper for coordination                      │
└─────────────────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Docker and Docker Compose
- .NET 8.0 SDK

## Running the Sample

### 1. Start the ClickHouse Cluster

```bash
cd samples/DistributedSample
docker compose up -d
```

Wait for all nodes to become healthy (about 30 seconds):

```bash
docker compose ps
```

You should see all 3 containers with status `healthy`.

### 2. Run the Sample Application

```bash
dotnet run
```

### 3. Clean Up

```bash
docker compose down -v
```

## What This Sample Demonstrates

### Two-Table Pattern

The sample uses a common ClickHouse pattern with two entities:

1. **EventLocal (MergeTree)** - The actual data storage table on each shard
2. **Event (Distributed)** - A unified view that fans out queries to all shards

```csharp
// Local table - actual data storage on each shard
modelBuilder.Entity<EventLocal>(entity =>
{
    entity.ToTable("events_local");
    entity.UseMergeTree(x => new { x.EventTime, x.Id });
    entity.HasPartitionByMonth(x => x.EventTime);
});

// Distributed table - unified view across all shards
modelBuilder.Entity<Event>(entity =>
{
    entity.ToTable("events");
    entity.UseDistributed("shard_cluster", "events_local")
          .WithShardingKey("cityHash64(UserId)");
});
```

### Sharding Key

The sharding key `cityHash64(UserId)` determines which shard receives each row:

- **Consistent**: Same UserId always goes to the same shard
- **Even distribution**: cityHash64 provides good distribution across shards
- **Locality**: All events for a user are on the same shard (efficient for user-based queries)

### Database Creation

When creating the database on a cluster, connect to the `default` database first, then use ON CLUSTER DDL:

```csharp
// Connect to default database first
await using var context = new DbContext("Host=localhost;Port=8123;Database=default");
await context.Database.OpenConnectionAsync();
await using var cmd = context.Database.GetDbConnection().CreateCommand();
cmd.CommandText = "CREATE DATABASE IF NOT EXISTS my_db ON CLUSTER shard_cluster";
await cmd.ExecuteNonQueryAsync();
```

### Query Routing

- **Queries through Event (Distributed)**: Fanned out to all shards, results merged
- **Queries through EventLocal**: Only hit the local shard (useful for debugging)
- **Inserts through Event**: Routed to appropriate shard based on sharding key

## Expected Output

```
Step 5: Checking data distribution across shards...

  Shard 1 (host: clickhouse1):
    - Local events: 32
  Shard 2 (host: clickhouse2):
    - Local events: 35
  Shard 3 (host: clickhouse3):
    - Local events: 33

  Total events across all shards: 100

Step 6: Querying through distributed table (unified view)...

  Total events (distributed query): 100

  Events by type:
    - view: 24
    - click: 22
    - purchase: 20
    - logout: 17
    - signup: 17
```

## Configuration Files

| File | Description |
|------|-------------|
| `docker-compose.yml` | Docker Compose configuration for 3-node sharded cluster |
| `config/users.xml` | Shared user configuration |
| `config/clickhouse1/config.xml` | Node 1: Keeper #1, shard=1 |
| `config/clickhouse2/config.xml` | Node 2: Keeper #2, shard=2 |
| `config/clickhouse3/config.xml` | Node 3: Keeper #3, shard=3 |

## Useful Commands

### Check cluster status

```bash
docker exec clickhouse1 clickhouse-client --query "SELECT * FROM system.clusters WHERE cluster = 'shard_cluster'"
```

### Check data distribution

```bash
# Events on each shard
docker exec clickhouse1 clickhouse-client --query "SELECT hostName(), count() FROM distributed_demo.events_local"
docker exec clickhouse2 clickhouse-client --query "SELECT hostName(), count() FROM distributed_demo.events_local"
docker exec clickhouse3 clickhouse-client --query "SELECT hostName(), count() FROM distributed_demo.events_local"

# Total via distributed table
docker exec clickhouse1 clickhouse-client --query "SELECT count() FROM distributed_demo.events"
```

### Connect to individual nodes

```bash
# Shard 1
docker exec -it clickhouse1 clickhouse-client

# Shard 2
docker exec -it clickhouse2 clickhouse-client

# Shard 3
docker exec -it clickhouse3 clickhouse-client
```

## When to Use Sharding vs Replication

### Use Sharding (Distributed tables) when:
- Dataset is too large for a single node
- You need to scale write throughput
- Queries can benefit from parallelization across shards
- You have a natural sharding key (user_id, tenant_id, etc.)

### Use Replication when:
- You need high availability / fault tolerance
- You want to scale read throughput
- Dataset fits comfortably on a single node
- You need strong consistency guarantees

### Combine Both for Production:
In production, you typically combine both - multiple shards for scaling, with replicas per shard for HA:

```
3 shards × 2 replicas = 6 nodes
- Shard 1: Node 1a, Node 1b (replicas)
- Shard 2: Node 2a, Node 2b (replicas)
- Shard 3: Node 3a, Node 3b (replicas)
```

## See Also

- [ClusterSample](../ClusterSample/README.md) - Replication example (1 shard × 3 replicas)
- [Clustering and Replication](../../docs/features/clustering.md) - Full clustering setup guide
- [Distributed Engine Docs](https://clickhouse.com/docs/en/engines/table-engines/special/distributed) - ClickHouse documentation
