# Clustering Overview

ClickHouse supports two complementary scaling strategies: **replication** for high availability and **sharding** for horizontal throughput. EF.CH provides first-class support for both through engine configuration, DDL extensions, and connection routing.

## Replication vs Sharding

| Strategy | Purpose | Mechanism | Data Distribution |
|----------|---------|-----------|-------------------|
| **Replication** | High availability, read scaling | Replicated engines + ZooKeeper/Keeper | Every replica holds a full copy of the data |
| **Sharding** | Horizontal write/storage scaling | Distributed engine over local tables | Each shard holds a subset of the data |

These strategies are often combined: each shard is itself a group of replicas.

## Decision Guide

**Use replication when:**
- You need high availability (automatic failover if a node goes down)
- Read throughput is the bottleneck (spread SELECTs across replicas)
- Your data fits on a single node's storage

**Use sharding when:**
- Data volume exceeds a single node's storage capacity
- Write throughput is the bottleneck (distribute INSERTs across shards)
- Queries benefit from parallel execution across nodes

**Use both when:**
- You need both high availability and horizontal scaling
- Production deployments that cannot tolerate data loss

## Infrastructure Requirements

### ZooKeeper / ClickHouse Keeper

Replicated engines require a coordination service for consensus. ClickHouse supports two options:

| Service | Description |
|---------|-------------|
| **ClickHouse Keeper** | Built-in, recommended for new deployments. Configured in ClickHouse server config. |
| **Apache ZooKeeper** | External service. Requires separate deployment and maintenance. |

A minimum of 3 Keeper/ZooKeeper nodes is recommended for production to maintain quorum.

### Cluster Configuration

Clusters are defined in the ClickHouse server configuration (`config.xml` or `config.d/*.xml`):

```xml
<clickhouse>
    <remote_servers>
        <my_cluster>
            <shard>
                <replica>
                    <host>clickhouse1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>clickhouse2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </my_cluster>
    </remote_servers>
</clickhouse>
```

EF.CH references these cluster names in entity configuration and DDL operations.

## EF.CH Clustering Features

### Replicated Engines

Six replicated engine variants mirror the non-replicated MergeTree family, adding automatic data synchronization between replicas.

```csharp
entity.UseReplicatedMergeTree(x => x.Id)
    .WithCluster("my_cluster")
    .WithReplication("/clickhouse/{database}/{table}");
```

See [Replicated Engines](replicated-engines.md) for full details.

### Cluster DDL

The `ON CLUSTER` clause causes DDL statements to execute across all cluster nodes automatically.

```csharp
entity.UseCluster("my_cluster");
```

See [Cluster DDL](cluster-ddl.md) for configuration options.

### Connection Routing

Read/write splitting directs SELECT queries to read replicas and mutations to the write primary.

```csharp
options.UseClickHouse("Host=localhost", o => o
    .UseConnectionRouting()
    .AddConnection("Primary", conn => conn
        .WriteEndpoint("dc1-clickhouse:8123")
        .ReadEndpoints("dc2-clickhouse:8123", "dc1-clickhouse:8123")
    )
);
```

See [Connection Routing](connection-routing.md) for configuration and failover.

## Typical Topology

A common production topology with 2 shards and 2 replicas per shard:

```
                    +-----------+
                    |  Keeper   |
                    |  Cluster  |
                    |  (3 nodes)|
                    +-----------+
                         |
          +--------------+--------------+
          |                             |
     Shard 1                       Shard 2
  +----------+  +----------+   +----------+  +----------+
  | Replica  |  | Replica  |   | Replica  |  | Replica  |
  |   1a     |  |   1b     |   |   2a     |  |   2b     |
  +----------+  +----------+   +----------+  +----------+
```

EF.CH entities in this topology would use:
- `UseReplicatedMergeTree` for the local tables on each replica
- `UseDistributed` for the distributed table that spans all shards
- `WithCluster` for DDL to propagate across nodes

## See Also

- [Replicated Engines](replicated-engines.md) -- engine configuration for replication
- [Cluster DDL](cluster-ddl.md) -- ON CLUSTER and table group management
- [Connection Routing](connection-routing.md) -- read/write splitting and failover
