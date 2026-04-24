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
