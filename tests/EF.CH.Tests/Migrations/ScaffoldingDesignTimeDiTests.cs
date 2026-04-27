using EF.CH.Design.Internal;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Reproduces issue #82: <c>dotnet ef dbcontext scaffold</c> activates
/// <see cref="IDatabaseModelFactory"/> through the design-time service
/// container that <see cref="IDesignTimeServices"/> populates. That container
/// does not register open-generic <c>ILogger&lt;T&gt;</c>, so a factory that
/// asks for one fails to activate.
/// </summary>
public class ScaffoldingDesignTimeDiTests
{
    /// <summary>
    /// Mirrors the exact resolution that <c>dotnet ef</c> performs and that
    /// throws in issue #82 — no database needed, the failure is purely DI.
    /// </summary>
    [Fact]
    public void DatabaseModelFactory_resolves_from_design_time_services()
    {
        var services = new ServiceCollection();
        new ClickHouseDesignTimeServices().ConfigureDesignTimeServices(services);
        var provider = services.BuildServiceProvider();

        var factory = provider.GetRequiredService<IDatabaseModelFactory>();

        Assert.NotNull(factory);
    }

    /// <summary>
    /// End-to-end: build the same DI container, resolve the factory, and
    /// scaffold a real ClickHouse database to prove the wired-up factory
    /// is functional, not just constructible.
    /// </summary>
    public class EndToEnd : IAsyncLifetime
    {
        private readonly ClickHouseContainer _container = new ClickHouseBuilder()
            .WithImage("clickhouse/clickhouse-server:25.6")
            .Build();

        public async Task InitializeAsync() => await _container.StartAsync();
        public async Task DisposeAsync() => await _container.DisposeAsync();

        [Fact]
        public void Resolved_factory_scaffolds_database()
        {
            var services = new ServiceCollection();
            new ClickHouseDesignTimeServices().ConfigureDesignTimeServices(services);
            var provider = services.BuildServiceProvider();
            var factory = provider.GetRequiredService<IDatabaseModelFactory>();

            SeedSchema();
            var model = factory.Create(_container.GetConnectionString(), new DatabaseModelFactoryOptions());

            var probe = Assert.Single(model.Tables, t => t.Name == "scaffold_di_probe");
            Assert.Equal(2, probe.Columns.Count);
        }

        private void SeedSchema()
        {
            using var connection = new ClickHouse.Driver.ADO.ClickHouseConnection(_container.GetConnectionString());
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = """
                CREATE TABLE scaffold_di_probe (
                    id   UInt32,
                    name String
                ) ENGINE = MergeTree() ORDER BY id
                """;
            command.ExecuteNonQuery();
        }
    }
}
