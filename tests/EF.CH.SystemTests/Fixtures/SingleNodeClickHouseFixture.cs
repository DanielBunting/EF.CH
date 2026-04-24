using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.SystemTests.Fixtures;

public sealed class SingleNodeClickHouseFixture : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage(ClusterConfigTemplates.ClickHouseImage)
        .Build();

    public string ConnectionString => _container.GetConnectionString();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}
