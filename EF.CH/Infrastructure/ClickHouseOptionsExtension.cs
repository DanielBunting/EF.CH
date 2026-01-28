using System.Data.Common;
using System.Globalization;
using System.Text;
using EF.CH.Configuration;
using EF.CH.Diagnostics;
using EF.CH.Dictionaries;
using EF.CH.External;
using EF.CH.Metadata.Conventions;
using EF.CH.Metadata.Internal;
using EF.CH.Migrations.Internal;
using EF.CH.Query.Internal;
using EF.CH.Query.Internal.Translators;
using EF.CH.Storage.Internal;
using EF.CH.Update.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EF.CH.Infrastructure;

/// <summary>
/// Represents the ClickHouse-specific options for a <see cref="DbContext"/>.
/// </summary>
public class ClickHouseOptionsExtension : RelationalOptionsExtension
{
    private bool _useQuerySplitting;
    private bool _useKeylessEntitiesByDefault;
    private ClickHouseDeleteStrategy _deleteStrategy = ClickHouseDeleteStrategy.Lightweight;
    private DbContextOptionsExtensionInfo? _info;
    private ClickHouseConfiguration? _configuration;
    private string? _clusterName;
    private bool _useConnectionRouting;

    /// <summary>
    /// Creates a new instance of <see cref="ClickHouseOptionsExtension"/>.
    /// </summary>
    public ClickHouseOptionsExtension()
    {
    }

    /// <summary>
    /// Creates a new instance by copying from an existing instance.
    /// </summary>
    protected ClickHouseOptionsExtension(ClickHouseOptionsExtension copyFrom)
        : base(copyFrom)
    {
        _useQuerySplitting = copyFrom._useQuerySplitting;
        _useKeylessEntitiesByDefault = copyFrom._useKeylessEntitiesByDefault;
        _deleteStrategy = copyFrom._deleteStrategy;
        _configuration = copyFrom._configuration;
        _clusterName = copyFrom._clusterName;
        _useConnectionRouting = copyFrom._useConnectionRouting;
    }

    /// <summary>
    /// Gets information about the extension.
    /// </summary>
    public override DbContextOptionsExtensionInfo Info
        => _info ??= new ExtensionInfo(this);

    /// <summary>
    /// Gets whether query splitting is enabled.
    /// </summary>
    public virtual bool UseQuerySplitting => _useQuerySplitting;

    /// <summary>
    /// Gets whether entities are keyless by default.
    /// When enabled, all entities default to HasNoKey() unless explicitly configured with HasKey().
    /// This is useful for append-only ClickHouse tables where EF Core change tracking isn't needed.
    /// </summary>
    public virtual bool UseKeylessEntitiesByDefault => _useKeylessEntitiesByDefault;

    /// <summary>
    /// Gets the DELETE strategy to use for ClickHouse operations.
    /// Defaults to <see cref="ClickHouseDeleteStrategy.Lightweight"/>.
    /// </summary>
    public virtual ClickHouseDeleteStrategy DeleteStrategy => _deleteStrategy;

    /// <summary>
    /// Gets the multi-datacenter configuration for ClickHouse.
    /// </summary>
    public virtual ClickHouseConfiguration? Configuration => _configuration;

    /// <summary>
    /// Gets the default cluster name for ON CLUSTER DDL operations.
    /// This is used when entities don't specify a cluster or table group.
    /// </summary>
    public virtual string? ClusterName => _clusterName;

    /// <summary>
    /// Gets whether connection routing (read/write splitting) is enabled.
    /// </summary>
    public virtual bool UseConnectionRouting => _useConnectionRouting;

    /// <summary>
    /// Creates a copy with the specified connection string.
    /// </summary>
    public new virtual ClickHouseOptionsExtension WithConnectionString(string? connectionString)
    {
        // Call base method which handles the connection string properly
        return (ClickHouseOptionsExtension)base.WithConnectionString(connectionString);
    }

    /// <summary>
    /// Creates a copy with the specified connection.
    /// </summary>
    public new virtual ClickHouseOptionsExtension WithConnection(DbConnection? connection)
    {
        return (ClickHouseOptionsExtension)base.WithConnection(connection);
    }

    /// <summary>
    /// Creates a copy with the specified command timeout.
    /// </summary>
    public new virtual ClickHouseOptionsExtension WithCommandTimeout(int? commandTimeout)
    {
        return (ClickHouseOptionsExtension)base.WithCommandTimeout(commandTimeout);
    }

    /// <summary>
    /// Creates a copy with the specified query splitting setting.
    /// </summary>
    public virtual ClickHouseOptionsExtension WithQuerySplitting(bool useQuerySplitting)
    {
        var clone = (ClickHouseOptionsExtension)Clone();
        clone._useQuerySplitting = useQuerySplitting;
        return clone;
    }

    /// <summary>
    /// Creates a copy with keyless entities enabled by default.
    /// When enabled, all entities default to HasNoKey() unless explicitly configured with HasKey().
    /// </summary>
    public virtual ClickHouseOptionsExtension WithKeylessEntitiesByDefault(bool useKeylessEntities = true)
    {
        var clone = (ClickHouseOptionsExtension)Clone();
        clone._useKeylessEntitiesByDefault = useKeylessEntities;
        return clone;
    }

    /// <summary>
    /// Creates a copy with the specified delete strategy.
    /// </summary>
    /// <param name="deleteStrategy">The delete strategy to use.</param>
    public virtual ClickHouseOptionsExtension WithDeleteStrategy(ClickHouseDeleteStrategy deleteStrategy)
    {
        var clone = (ClickHouseOptionsExtension)Clone();
        clone._deleteStrategy = deleteStrategy;
        return clone;
    }

    /// <summary>
    /// Creates a copy with the specified max batch size.
    /// </summary>
    public virtual ClickHouseOptionsExtension WithMaxBatchSize(int maxBatchSize)
    {
        return (ClickHouseOptionsExtension)base.WithMaxBatchSize(maxBatchSize);
    }

    /// <summary>
    /// Creates a copy with the specified multi-datacenter configuration.
    /// </summary>
    /// <param name="configuration">The cluster configuration.</param>
    public virtual ClickHouseOptionsExtension WithConfiguration(ClickHouseConfiguration configuration)
    {
        var clone = (ClickHouseOptionsExtension)Clone();
        clone._configuration = configuration;
        return clone;
    }

    /// <summary>
    /// Creates a copy with the specified default cluster name.
    /// </summary>
    /// <param name="clusterName">The cluster name for ON CLUSTER DDL operations.</param>
    public virtual ClickHouseOptionsExtension WithClusterName(string? clusterName)
    {
        var clone = (ClickHouseOptionsExtension)Clone();
        clone._clusterName = clusterName;
        return clone;
    }

    /// <summary>
    /// Creates a copy with connection routing (read/write splitting) enabled.
    /// </summary>
    /// <param name="useConnectionRouting">Whether to enable connection routing.</param>
    public virtual ClickHouseOptionsExtension WithConnectionRouting(bool useConnectionRouting = true)
    {
        var clone = (ClickHouseOptionsExtension)Clone();
        clone._useConnectionRouting = useConnectionRouting;
        return clone;
    }

    /// <summary>
    /// Creates a copy of this extension.
    /// </summary>
    protected override RelationalOptionsExtension Clone()
        => new ClickHouseOptionsExtension(this);

    /// <summary>
    /// Adds ClickHouse-specific services to the service collection.
    /// </summary>
    public override void ApplyServices(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        // Register ClickHouse-specific services
        services.AddEntityFrameworkClickHouse();
    }

    /// <summary>
    /// Validates the options are consistent and correct.
    /// </summary>
    public override void Validate(IDbContextOptions options)
    {
        base.Validate(options);

        if (Connection is null && ConnectionString is null)
        {
            throw new InvalidOperationException(
                "A connection string or connection must be specified when using ClickHouse.");
        }
    }

    /// <summary>
    /// Information about the extension for logging and debugging.
    /// </summary>
    private sealed class ExtensionInfo : RelationalExtensionInfo
    {
        private string? _logFragment;

        public ExtensionInfo(IDbContextOptionsExtension extension)
            : base(extension)
        {
        }

        private new ClickHouseOptionsExtension Extension
            => (ClickHouseOptionsExtension)base.Extension;

        public override bool IsDatabaseProvider => true;

        public override string LogFragment
        {
            get
            {
                if (_logFragment is null)
                {
                    var builder = new StringBuilder();
                    builder.Append("Using ClickHouse ");

                    if (Extension.CommandTimeout.HasValue)
                    {
                        builder.Append(CultureInfo.InvariantCulture, $"CommandTimeout={Extension.CommandTimeout} ");
                    }

                    if (Extension.UseQuerySplitting)
                    {
                        builder.Append("QuerySplitting ");
                    }

                    if (Extension.UseConnectionRouting)
                    {
                        builder.Append("ConnectionRouting ");
                    }

                    if (!string.IsNullOrEmpty(Extension.ClusterName))
                    {
                        builder.Append(CultureInfo.InvariantCulture, $"Cluster={Extension.ClusterName} ");
                    }

                    builder.Append(CultureInfo.InvariantCulture, $"MaxBatchSize={Extension.MaxBatchSize} ");

                    _logFragment = builder.ToString();
                }

                return _logFragment;
            }
        }

        public override int GetServiceProviderHashCode()
        {
            var hashCode = new HashCode();
            hashCode.Add(base.GetServiceProviderHashCode());
            hashCode.Add(Extension.UseQuerySplitting);
            hashCode.Add(Extension.UseKeylessEntitiesByDefault);
            hashCode.Add(Extension.DeleteStrategy);
            hashCode.Add(Extension.ClusterName);
            hashCode.Add(Extension.UseConnectionRouting);
            // Configuration is reference-compared intentionally - same object = same hash
            hashCode.Add(Extension.Configuration?.GetHashCode() ?? 0);
            return hashCode.ToHashCode();
        }

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is ExtensionInfo otherInfo
               && base.ShouldUseSameServiceProvider(other)
               && Extension.UseQuerySplitting == otherInfo.Extension.UseQuerySplitting
               && Extension.UseKeylessEntitiesByDefault == otherInfo.Extension.UseKeylessEntitiesByDefault
               && Extension.DeleteStrategy == otherInfo.Extension.DeleteStrategy
               && Extension.ClusterName == otherInfo.Extension.ClusterName
               && Extension.UseConnectionRouting == otherInfo.Extension.UseConnectionRouting
               && ReferenceEquals(Extension.Configuration, otherInfo.Extension.Configuration);

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            ArgumentNullException.ThrowIfNull(debugInfo);

            debugInfo["ClickHouse:CommandTimeout"] = Extension.CommandTimeout?.ToString() ?? "(null)";
            debugInfo["ClickHouse:QuerySplitting"] = Extension.UseQuerySplitting.ToString();
            debugInfo["ClickHouse:KeylessEntitiesByDefault"] = Extension.UseKeylessEntitiesByDefault.ToString();
            debugInfo["ClickHouse:MaxBatchSize"] = Extension.MaxBatchSize.ToString()!;
            debugInfo["ClickHouse:DeleteStrategy"] = Extension.DeleteStrategy.ToString();
            debugInfo["ClickHouse:ClusterName"] = Extension.ClusterName ?? "(null)";
            debugInfo["ClickHouse:ConnectionRouting"] = Extension.UseConnectionRouting.ToString();
            debugInfo["ClickHouse:HasConfiguration"] = (Extension.Configuration != null).ToString();
        }
    }
}

/// <summary>
/// Extension methods for registering ClickHouse services.
/// </summary>
public static class ClickHouseServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Entity Framework Core ClickHouse provider services.
    /// </summary>
    public static IServiceCollection AddEntityFrameworkClickHouse(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        var builder = new EntityFrameworkRelationalServicesBuilder(services)
            // Core services
            .TryAdd<IDatabaseProvider, DatabaseProvider<ClickHouseOptionsExtension>>()
            .TryAdd<IRelationalTypeMappingSource, ClickHouseTypeMappingSource>()
            .TryAdd<ISqlGenerationHelper, ClickHouseSqlGenerationHelper>()
            .TryAdd<LoggingDefinitions, ClickHouseLoggingDefinitions>()
            .TryAdd<IRelationalConnection, ClickHouseRelationalConnection>()

            // Metadata services
            .TryAdd<IRelationalAnnotationProvider, ClickHouseAnnotationProvider>()
            .TryAdd<IProviderConventionSetBuilder, ClickHouseConventionSetBuilder>()
            .TryAdd<IModelValidator, ClickHouseModelValidator>()

            // Query services
            .TryAdd<IQuerySqlGeneratorFactory, ClickHouseQuerySqlGeneratorFactory>()
            .TryAdd<IQueryTranslationPostprocessorFactory, ClickHouseQueryTranslationPostprocessorFactory>()
            .TryAdd<IRelationalParameterBasedSqlProcessorFactory, ClickHouseParameterBasedSqlProcessorFactory>()
            .TryAdd<IMethodCallTranslatorProvider, ClickHouseMethodCallTranslatorProvider>()
            .TryAdd<IMemberTranslatorProvider, ClickHouseMemberTranslatorProvider>()
            .TryAdd<IQueryableMethodTranslatingExpressionVisitorFactory, ClickHouseQueryableMethodTranslatingExpressionVisitorFactory>()
            .TryAdd<IRelationalSqlTranslatingExpressionVisitorFactory, ClickHouseSqlTranslatingExpressionVisitorFactory>()
            .TryAdd<ISqlExpressionFactory, ClickHouseSqlExpressionFactory>()
            .TryAdd<IAggregateMethodCallTranslatorProvider, ClickHouseAggregateMethodCallTranslatorProvider>()

            // Update services
            .TryAdd<IModificationCommandBatchFactory, ClickHouseModificationCommandBatchFactory>()
            .TryAdd<IUpdateSqlGenerator, ClickHouseUpdateSqlGenerator>()

            // Migration services
            .TryAdd<IMigrationsSqlGenerator, ClickHouseMigrationsSqlGenerator>()
            .TryAdd<IHistoryRepository, ClickHouseHistoryRepository>()

            // Database creator
            .TryAdd<IRelationalDatabaseCreator, ClickHouseDatabaseCreator>()

            // Relational transaction factory - ClickHouse specific (throws on transaction start)
            .TryAdd<IRelationalTransactionFactory, ClickHouseTransactionFactory>();

        builder.TryAddCoreServices();

        // Register the evaluatable expression filter plugin to prevent parameterization
        // of arguments to Sample(), WithSetting(), and WithSettings() methods.
        // This must be registered as an enumerable service since multiple plugins can exist.
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IEvaluatableExpressionFilterPlugin, ClickHouseEvaluatableExpressionFilterPlugin>());

        // Register external config resolver for postgresql() table function support.
        // Optionally injects IConfiguration for connection profile support.
        services.TryAddSingleton<IExternalConfigResolver>(sp =>
        {
            var configuration = sp.GetService<IConfiguration>();
            return new ExternalConfigResolver(configuration);
        });

        // Register dictionary config resolver for external dictionary sources (PostgreSQL, MySQL, HTTP).
        // Resolves credentials from environment variables or IConfiguration at runtime.
        services.TryAddSingleton<IDictionaryConfigResolver>(sp =>
        {
            var configuration = sp.GetService<IConfiguration>();
            return new DictionaryConfigResolver(configuration);
        });

        return services;
    }
}
