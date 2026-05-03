using EF.CH.Extensions;
using EF.CH.External;
using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EF.CH.Tests.Features;

/// <summary>
/// Companion to issue #82's logger-DI fix: verifies that the
/// <see cref="IExternalConfigResolver"/> registered by
/// <c>AddEntityFrameworkClickHouse()</c> can actually reach the application's
/// <see cref="IConfiguration"/>. Today the factory at
/// <c>ClickHouseOptionsExtension.cs:393</c> calls <c>sp.GetService&lt;IConfiguration&gt;()</c>
/// against EF's internal service provider, which never registers IConfiguration —
/// so connection profiles never resolve, regardless of what the host DI has.
/// </summary>
public class ExternalConfigResolverDiTests
{
    private const string ProfileName = "TestPg";

    private static IConfiguration BuildConfiguration() =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [$"ExternalConnections:{ProfileName}:HostPort"] = "pg.example:5432",
                [$"ExternalConnections:{ProfileName}:Database"] = "ordersdb",
                [$"ExternalConnections:{ProfileName}:User"] = "ef_user",
                [$"ExternalConnections:{ProfileName}:Password"] = "ef_pass",
                [$"ExternalConnections:{ProfileName}:Schema"] = "public",
            })
            .Build();

    [Fact]
    public void Profile_based_external_resolver_picks_up_host_IConfiguration()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(BuildConfiguration());
        services.AddDbContext<ProfileTestContext>(options =>
            options.UseClickHouse("Host=fake;Port=8123;Username=default"));

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<ProfileTestContext>();

        var resolver = context.GetService<IExternalConfigResolver>();
        var entityType = context.Model.FindEntityType(typeof(RemoteOrder))!;

        var sql = resolver.ResolvePostgresTableFunction(entityType);

        Assert.Equal(
            "postgresql('pg.example:5432', 'ordersdb', 'remote_order', 'ef_user', 'ef_pass', 'public')",
            sql);
    }

    /// <summary>
    /// Validates the entire EF service graph with <c>validateScopes: true</c>.
    /// This surfaces any Singleton-capturing-Scoped captive dependencies that
    /// the default permissive provider hides — e.g. a Singleton query factory
    /// holding a Scoped <c>IExternalConfigResolver</c>.
    /// </summary>
    [Fact]
    public void EF_service_graph_passes_strict_scope_validation()
    {
        var efServices = new ServiceCollection();
        efServices.AddEntityFrameworkClickHouse();
        var internalProvider = efServices.BuildServiceProvider(validateScopes: true);

        var hostServices = new ServiceCollection();
        hostServices.AddSingleton<IConfiguration>(BuildConfiguration());
        hostServices.AddDbContext<ProfileTestContext>(options =>
            options.UseClickHouse("Host=fake;Port=8123;Username=default")
                   .UseInternalServiceProvider(internalProvider));

        using var provider = hostServices.BuildServiceProvider(validateScopes: true);
        using var scope = provider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<ProfileTestContext>();

        // Force every singleton factory in the EF graph to be activated. Any
        // captive Scoped dependency throws here under ValidateScopes.
        Assert.NotNull(context.Model);
        Assert.NotNull(context.GetService<Microsoft.EntityFrameworkCore.Query.IQueryTranslationPostprocessorFactory>());
        Assert.NotNull(context.GetService<Microsoft.EntityFrameworkCore.Update.IModificationCommandBatchFactory>());
    }

    /// <summary>
    /// Documents the actual EF Core 10 lifetimes of the two factories that
    /// consume <see cref="IExternalConfigResolver"/>. If a future EF version
    /// promotes either to Singleton, the Scoped resolver registration in
    /// <c>ClickHouseOptionsExtension</c> will start producing captive
    /// dependencies — this test will catch that regression.
    /// </summary>
    [Fact]
    public void External_resolver_consumers_remain_Scoped_in_ef_internal_provider()
    {
        var services = new ServiceCollection();
        services.AddEntityFrameworkClickHouse();

        var qtpf = services.Single(s => s.ServiceType ==
            typeof(Microsoft.EntityFrameworkCore.Query.IQueryTranslationPostprocessorFactory));
        var mcbf = services.Single(s => s.ServiceType ==
            typeof(Microsoft.EntityFrameworkCore.Update.IModificationCommandBatchFactory));

        Assert.Equal(ServiceLifetime.Scoped, qtpf.Lifetime);
        Assert.Equal(ServiceLifetime.Scoped, mcbf.Lifetime);
    }

    /// <summary>
    /// Confirms that switching the resolvers to Scoped did not create a captive
    /// dependency in any singleton consumer (e.g. <c>IQueryTranslationPostprocessorFactory</c>).
    /// If it had, building the model / running a query would throw at construction time.
    /// </summary>
    [Fact]
    public void Scoped_resolver_is_consumable_by_singleton_query_pipeline()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(BuildConfiguration());
        services.AddDbContext<ProfileTestContext>(options =>
            options.UseClickHouse("Host=fake;Port=8123;Username=default"));

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<ProfileTestContext>();

        // Forces the entire EF service graph (query factories, batch factory) to be
        // wired up — fails fast if a singleton captures a scoped resolver illegally.
        Assert.NotNull(context.Model);
        Assert.NotNull(context.GetService<Microsoft.EntityFrameworkCore.Query.IQueryTranslationPostprocessorFactory>());
        Assert.NotNull(context.GetService<Microsoft.EntityFrameworkCore.Update.IModificationCommandBatchFactory>());
    }

    private class RemoteOrder
    {
        public long Id { get; set; }
        public string Customer { get; set; } = "";
    }

    private class ProfileTestContext : DbContext
    {
        public ProfileTestContext(DbContextOptions<ProfileTestContext> options) : base(options) { }

        public DbSet<RemoteOrder> Orders => Set<RemoteOrder>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ExternalPostgresEntity<RemoteOrder>(ext => ext
                .Connection(c => c.UseProfile(ProfileName)));
        }
    }
}
