using EF.CH.Design.Internal;
using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Design.Internal;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Design;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Locks in the EF Core 10 default lifetimes for the design-time services we
/// override via <see cref="ServiceCollectionDescriptorExtensions.Replace"/>.
/// If a future EF version changes a default lifetime, our <c>Replace</c> calls
/// would silently shift behaviour — these tests will catch that regression.
/// </summary>
public class DesignTimeServiceLifetimeTests
{
    /// <summary>
    /// Builds the design-time service collection EF Core itself sets up before
    /// invoking <see cref="IDesignTimeServices.ConfigureDesignTimeServices"/>,
    /// so the captured lifetimes are EF Core's defaults, not ours.
    /// </summary>
    private static IServiceCollection BuildEfDefaults()
    {
        var services = new ServiceCollection();
        services.AddEntityFrameworkClickHouse();
#pragma warning disable EF1001
        services.AddEntityFrameworkDesignTimeServices(
            new OperationReporter(handler: null),
            applicationServiceProviderAccessor: null);
        new EntityFrameworkRelationalDesignServicesBuilder(services).TryAddCoreServices();
#pragma warning restore EF1001
        return services;
    }

    [Fact]
    public void IMigrationsCodeGenerator_default_is_Singleton()
    {
        var services = BuildEfDefaults();
        var descriptor = services.Last(s => s.ServiceType == typeof(IMigrationsCodeGenerator));

        // ClickHouseDesignTimeServices replaces with Singleton — must match EF's default.
        Assert.Equal(ServiceLifetime.Singleton, descriptor.Lifetime);
    }

    [Fact]
    public void IMigrationsScaffolder_default_is_Scoped()
    {
        var services = BuildEfDefaults();
        var descriptor = services.Last(s => s.ServiceType == typeof(IMigrationsScaffolder));

        // ClickHouseDesignTimeServices replaces with Scoped — must match EF's default.
        Assert.Equal(ServiceLifetime.Scoped, descriptor.Lifetime);
    }
}
