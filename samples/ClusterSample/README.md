# Cluster Sample

Demonstrates multi-node ClickHouse cluster features using EF.CH.

## What This Sample Shows

1. **3-node cluster setup** - Docker Compose with three ClickHouse nodes configured as a cluster
2. **Replicated engine** - `UseReplicatedMergeTree` with `.WithCluster()` and `.WithReplication()` for data replication across nodes
3. **ON CLUSTER DDL** - `UseCluster()` causes DDL statements to execute on all cluster nodes
4. **Connection routing** - `UseConnectionRouting()` splits SELECT queries to read endpoints and mutations to write endpoints
5. **Table groups** - `AddTableGroup()` for logical grouping of tables with shared cluster/replication settings

## Prerequisites

- .NET 8.0 SDK
- Docker and Docker Compose

## Running

```bash
# Start the 3-node cluster
cd samples/ClusterSample
docker compose up -d

# Wait for all nodes to be ready (~10 seconds)
sleep 10

# Run the sample
dotnet run

# Clean up
docker compose down
```

## Cluster Architecture

```
+-------------+    +-------------+    +-------------+
| clickhouse1 |    | clickhouse2 |    | clickhouse3 |
| Shard 1     |    | Shard 2     |    | Shard 3     |
| Port 8123   |    | Port 8124   |    | Port 8125   |
+-------------+    +-------------+    +-------------+
       |                  |                  |
       +------ sample_cluster ---------------+
```

Each node has its own `config.xml` with:
- Shard/replica macros (`{shard}`, `{replica}`)
- Cluster topology definition (`sample_cluster`)

The shared `users.xml` configures the default user for all nodes.

## Key Concepts

### Replicated Engines

```csharp
entity.UseReplicatedMergeTree<Event>(x => new { x.EventDate, x.EventId })
    .WithCluster("sample_cluster")
    .WithReplication("/clickhouse/{database}/{table}", "{replica}");
```

### Connection Routing

```csharp
options.UseClickHouse(connectionString, o => o
    .UseConnectionRouting()
    .AddConnection("Primary", conn => conn
        .WriteEndpoint("node1:8123")
        .ReadEndpoints("node2:8123", "node3:8123")));
```

### Table Groups

```csharp
options.UseClickHouse(connectionString, o => o
    .AddTableGroup("Core", group => group
        .UseCluster("sample_cluster")
        .Replicated())
    .AddTableGroup("LocalCache", group => group
        .NoCluster()
        .NotReplicated()));
```
