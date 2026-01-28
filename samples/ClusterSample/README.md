# EF.CH Cluster Sample

This sample demonstrates how to use EF.CH with a 3-node ClickHouse cluster with full data replication.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        ClickHouse Cluster                                │
│                                                                         │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                │
│   │ clickhouse1 │◄──►│ clickhouse2 │◄──►│ clickhouse3 │                │
│   │  :8123 HTTP │    │  :8124 HTTP │    │  :8125 HTTP │                │
│   │  :9001 TCP  │    │  :9002 TCP  │    │  :9003 TCP  │                │
│   │             │    │             │    │             │                │
│   │  Keeper #1  │    │  Keeper #2  │    │  Keeper #3  │                │
│   └──────┬──────┘    └──────┬──────┘    └──────┬──────┘                │
│          │                  │                  │                        │
│          └──────────────────┴──────────────────┘                        │
│                   Raft Consensus (Keeper)                               │
│                                                                         │
│   Features:                                                             │
│   - Single shard, 3 replicas (all data on all nodes)                   │
│   - Integrated ClickHouse Keeper (no external ZooKeeper)               │
│   - Survives 1 node failure with full quorum                           │
└─────────────────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Docker and Docker Compose
- .NET 8.0 SDK

## Running the Sample

### 1. Start the ClickHouse Cluster

```bash
cd samples/ClusterSample
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

### Fluent Chain API for Replicated Engines

The sample uses the new fluent chain pattern for configuring replicated engines:

```csharp
entity.UseReplicatedReplacingMergeTree(x => x.Version, x => new { x.OrderDate, x.Id })
      .WithCluster("sample_cluster")
      .WithReplication("/clickhouse/tables/{database}/{table}");
```

This is equivalent to (but more ergonomic than) the separate calls pattern:

```csharp
entity.UseReplicatedReplacingMergeTree(x => x.Version, x => new { x.OrderDate, x.Id });
entity.UseCluster("sample_cluster");
entity.HasReplication("/clickhouse/tables/{database}/{table}");
```

### Cluster Configuration

The cluster is configured with:

- **Single shard**: All data exists on all nodes (no sharding)
- **3 replicas**: Each node has a complete copy of the data
- **Internal replication**: ClickHouse handles replication automatically via Keeper
- **Integrated Keeper**: Each ClickHouse node also runs a Keeper instance

### ON CLUSTER DDL

When `UseCluster("sample_cluster")` is configured, DDL statements include `ON CLUSTER`:

```sql
CREATE TABLE "Orders" ON CLUSTER sample_cluster
(
    "Id" UUID,
    "OrderDate" DateTime64(3),
    ...
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{database}/{table}', '{replica}', "Version")
ORDER BY ("OrderDate", "Id")
```

This ensures the table is created on all cluster nodes.

## Configuration Files

| File | Description |
|------|-------------|
| `docker-compose.yml` | Docker Compose configuration for 3-node cluster |
| `config/users.xml` | Shared user configuration |
| `config/clickhouse1/config.xml` | Node 1: Keeper #1, replica=clickhouse1 |
| `config/clickhouse2/config.xml` | Node 2: Keeper #2, replica=clickhouse2 |
| `config/clickhouse3/config.xml` | Node 3: Keeper #3, replica=clickhouse3 |

## Verifying Replication

The sample inserts data through Node 1 and then queries all 3 nodes to verify replication:

```
Step 4: Verifying data replication across all nodes...

  Node 1 (host: clickhouse1):
    - Order count: 3
    - Total revenue: $949.83

  Node 2 (host: clickhouse2):
    - Order count: 3
    - Total revenue: $949.83

  Node 3 (host: clickhouse3):
    - Order count: 3
    - Total revenue: $949.83
```

## Useful Commands

### Check cluster status

```bash
# From any node
docker exec clickhouse1 clickhouse-client --query "SELECT * FROM system.clusters WHERE cluster = 'sample_cluster'"
```

### Check Keeper status

```bash
docker exec clickhouse1 clickhouse-client --query "SELECT * FROM system.zookeeper WHERE path = '/clickhouse'"
```

### Check replication queue

```bash
docker exec clickhouse1 clickhouse-client --query "SELECT * FROM system.replication_queue"
```

### Connect to individual nodes

```bash
# Node 1
docker exec -it clickhouse1 clickhouse-client

# Node 2
docker exec -it clickhouse2 clickhouse-client

# Node 3
docker exec -it clickhouse3 clickhouse-client
```

## Failure Testing

### Simulate a node failure

```bash
# Stop node 2
docker compose stop clickhouse2

# The cluster continues to operate with 2/3 nodes
# (Keeper quorum: 2/3, data still available on nodes 1 and 3)
```

### Recover the node

```bash
# Start node 2
docker compose start clickhouse2

# Node 2 will automatically sync from the other replicas
```
