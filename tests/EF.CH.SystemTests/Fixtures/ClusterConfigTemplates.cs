using System.Text;

namespace EF.CH.SystemTests.Fixtures;

public static class ClusterConfigTemplates
{
    // Pinned to a modern release that understands all settings the current
    // ClickHouse.Driver emits. Bump together with the driver package when
    // necessary — the fleet-wide integration tests track `:latest`.
    public const string ClickHouseImage = "clickhouse/clickhouse-server:25.6";

    public const string ReplicatedClusterName = "repl_cluster";
    public const string ShardedClusterName = "shard_cluster";

    public static byte[] BuildReplicatedClusterConfig(int serverId, IReadOnlyList<string> replicaHostnames)
    {
        var replicas = string.Concat(replicaHostnames.Select(h =>
            $@"
                <replica>
                    <host>{h}</host>
                    <port>9000</port>
                    <user>clickhouse</user>
                    <password>clickhouse</password>
                </replica>"));

        var xml = $@"<clickhouse>
    <listen_host>0.0.0.0</listen_host>
    <logger><level>warning</level><console>1</console></logger>
    <remote_servers>
        <{ReplicatedClusterName}>
            <shard>{replicas}
            </shard>
        </{ReplicatedClusterName}>
    </remote_servers>
{BuildKeeperXml(serverId, replicaHostnames)}
{BuildZooKeeperClientXml(replicaHostnames)}
    <distributed_ddl>
        <path>/clickhouse/task_queue/ddl</path>
    </distributed_ddl>
    <keeper_map_path_prefix>/clickhouse/keeper_map</keeper_map_path_prefix>
</clickhouse>";
        return Encoding.UTF8.GetBytes(xml);
    }

    public static byte[] BuildShardedClusterConfig(int serverId, IReadOnlyList<string> shardHostnames)
    {
        // Include user/password so Distributed queries authenticate when ClickHouse forwards
        // a sub-query to another shard. Without this, internode uses the `default` user
        // which has a different password and the subquery fails with code 516.
        var shards = string.Concat(shardHostnames.Select(h =>
            $@"
            <shard>
                <replica>
                    <host>{h}</host>
                    <port>9000</port>
                    <user>clickhouse</user>
                    <password>clickhouse</password>
                </replica>
            </shard>"));

        var xml = $@"<clickhouse>
    <listen_host>0.0.0.0</listen_host>
    <logger><level>warning</level><console>1</console></logger>
    <remote_servers>
        <{ShardedClusterName}>{shards}
        </{ShardedClusterName}>
    </remote_servers>
{BuildKeeperXml(serverId, shardHostnames)}
{BuildZooKeeperClientXml(shardHostnames)}
    <distributed_ddl>
        <path>/clickhouse/task_queue/ddl</path>
    </distributed_ddl>
    <keeper_map_path_prefix>/clickhouse/keeper_map</keeper_map_path_prefix>
</clickhouse>";
        return Encoding.UTF8.GetBytes(xml);
    }

    public static byte[] BuildMacros(string clusterName, int shardNum, string replicaName)
    {
        var xml = $@"<clickhouse>
    <macros>
        <cluster>{clusterName}</cluster>
        <shard>{shardNum}</shard>
        <replica>{replicaName}</replica>
    </macros>
</clickhouse>";
        return Encoding.UTF8.GetBytes(xml);
    }

    public static byte[] BuildAccessManagementGrant(string username)
    {
        var xml = $@"<clickhouse>
    <users>
        <{username}>
            <access_management>1</access_management>
            <named_collection_control>1</named_collection_control>
            <show_named_collections>1</show_named_collections>
            <show_named_collections_secrets>1</show_named_collections_secrets>
        </{username}>
    </users>
</clickhouse>";
        return Encoding.UTF8.GetBytes(xml);
    }

    private static string BuildKeeperXml(int serverId, IReadOnlyList<string> hostnames)
    {
        var raftServers = string.Concat(hostnames.Select((h, i) =>
            $"\n            <server><id>{i + 1}</id><hostname>{h}</hostname><port>9234</port></server>"));

        return $@"    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>{serverId}</server_id>
        <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>30000</session_timeout_ms>
            <raft_logs_level>warning</raft_logs_level>
        </coordination_settings>
        <raft_configuration>{raftServers}
        </raft_configuration>
    </keeper_server>";
    }

    private static string BuildZooKeeperClientXml(IReadOnlyList<string> hostnames)
    {
        var nodes = string.Concat(hostnames.Select(h =>
            $"\n        <node><host>{h}</host><port>9181</port></node>"));

        return $@"    <zookeeper>{nodes}
    </zookeeper>";
    }
}
