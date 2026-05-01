using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.SystemTests.Fixtures;

public sealed class SingleNodeClickHouseFixture : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage(ClusterConfigTemplates.ClickHouseImage)
        .Build();

    /// <summary>Native-protocol connection string (mapped 9000 port). Used by EF queries.</summary>
    public string ConnectionString => _container.GetConnectionString();

    /// <summary>The mapped host port for the ClickHouse HTTP interface (8123).</summary>
    public int HttpPort => _container.GetMappedPublicPort(8123);

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}
