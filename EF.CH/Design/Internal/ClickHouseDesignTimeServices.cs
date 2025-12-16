using EF.CH.Infrastructure;
using EF.CH.Scaffolding.Internal;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Migrations.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EF.CH.Design.Internal;

/// <summary>
/// Design-time services entry point for the ClickHouse EF Core provider.
/// This is discovered by the dotnet ef CLI tools via the assembly attribute.
/// </summary>
public class ClickHouseDesignTimeServices : IDesignTimeServices
{
    /// <summary>
    /// Configures design-time services for ClickHouse.
    /// </summary>
    public void ConfigureDesignTimeServices(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        // Register all ClickHouse runtime services first
        services.AddEntityFrameworkClickHouse();

        // Use the design services builder to add design-time specific services
#pragma warning disable EF1001 // Internal EF Core API usage
        new EntityFrameworkRelationalDesignServicesBuilder(services)
            .TryAdd<IAnnotationCodeGenerator, ClickHouseAnnotationCodeGenerator>()
            .TryAdd<IProviderConfigurationCodeGenerator, ClickHouseCodeGenerator>()
            .TryAdd<IDatabaseModelFactory, ClickHouseDatabaseModelFactory>()
            .TryAddCoreServices();
#pragma warning restore EF1001

        // Register model code generator (provider-specific service in EF Core 10+)
        services.TryAddSingleton<IModelCodeGenerator, ClickHouseCSharpModelGenerator>();

        // Register migrations code generator to include ClickHouse extension namespaces
        services.Replace(ServiceDescriptor.Singleton<IMigrationsCodeGenerator, ClickHouseCSharpMigrationsGenerator>());
    }
}
