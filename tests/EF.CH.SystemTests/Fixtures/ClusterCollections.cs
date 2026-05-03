using Xunit;

namespace EF.CH.SystemTests.Fixtures;

[CollectionDefinition(Name)]
public sealed class SingleNodeCollection : ICollectionFixture<SingleNodeClickHouseFixture>
{
    public const string Name = "SingleNode";
}

[CollectionDefinition(Name)]
public sealed class ReplicatedClusterCollection : ICollectionFixture<ReplicatedClusterFixture>
{
    public const string Name = "ReplicatedCluster";
}

[CollectionDefinition(Name)]
public sealed class ShardedClusterCollection : ICollectionFixture<ShardedClusterFixture>
{
    public const string Name = "ShardedCluster";
}

[CollectionDefinition(Name)]
public sealed class PostgresCollection : ICollectionFixture<PostgresFixture>
{
    public const string Name = "Postgres";
}

[CollectionDefinition(Name)]
public sealed class MySqlCollection : ICollectionFixture<MySqlFixture>
{
    public const string Name = "MySql";
}

[CollectionDefinition(Name)]
public sealed class RedisCollection : ICollectionFixture<RedisFixture>
{
    public const string Name = "Redis";
}

[CollectionDefinition(Name)]
public sealed class HttpJsonEachRowCollection : ICollectionFixture<HttpJsonEachRowFixture>
{
    public const string Name = "HttpJsonEachRow";
}

[CollectionDefinition(Name)]
public sealed class OdbcMsSqlCollection : ICollectionFixture<OdbcMsSqlFixture>
{
    public const string Name = "OdbcMsSql";
}

[CollectionDefinition(Name)]
public sealed class TwoEndpointCollection : ICollectionFixture<TwoEndpointClickHouseFixture>
{
    public const string Name = "TwoEndpoint";
}
