using EF.CH.Configuration;
using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.Configuration;

namespace EF.CH.Extensions;

/// <summary>
/// ClickHouse-specific extension methods for <see cref="DbContextOptionsBuilder"/>.
/// </summary>
public static class ClickHouseDbContextOptionsExtensions
{
    /// <summary>
    /// Configures the context to connect to a ClickHouse database.
    /// </summary>
    /// <param name="optionsBuilder">The builder being used to configure the context.</param>
    /// <param name="connectionString">The connection string of the database to connect to.</param>
    /// <param name="clickHouseOptionsAction">An optional action to allow additional ClickHouse-specific configuration.</param>
    /// <returns>The options builder so that further configuration can be chained.</returns>
    /// <example>
    /// <code>
    /// options.UseClickHouse("Host=localhost;Port=8123;Database=default");
    /// </code>
    /// </example>
    public static DbContextOptionsBuilder UseClickHouse(
        this DbContextOptionsBuilder optionsBuilder,
        string connectionString,
        Action<ClickHouseDbContextOptionsBuilder>? clickHouseOptionsAction = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var extension = GetOrCreateExtension(optionsBuilder)
            .WithConnectionString(connectionString);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        ConfigureWarnings(optionsBuilder);

        clickHouseOptionsAction?.Invoke(new ClickHouseDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    /// <summary>
    /// Configures the context to connect to a ClickHouse database using a connection.
    /// </summary>
    /// <param name="optionsBuilder">The builder being used to configure the context.</param>
    /// <param name="connection">An existing ClickHouse connection to use.</param>
    /// <param name="clickHouseOptionsAction">An optional action to allow additional ClickHouse-specific configuration.</param>
    /// <returns>The options builder so that further configuration can be chained.</returns>
    public static DbContextOptionsBuilder UseClickHouse(
        this DbContextOptionsBuilder optionsBuilder,
        System.Data.Common.DbConnection connection,
        Action<ClickHouseDbContextOptionsBuilder>? clickHouseOptionsAction = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(connection);

        var extension = GetOrCreateExtension(optionsBuilder)
            .WithConnection(connection);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        ConfigureWarnings(optionsBuilder);

        clickHouseOptionsAction?.Invoke(new ClickHouseDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    /// <summary>
    /// Configures the context to connect to a ClickHouse database (generic version).
    /// </summary>
    /// <typeparam name="TContext">The type of context to be configured.</typeparam>
    /// <param name="optionsBuilder">The builder being used to configure the context.</param>
    /// <param name="connectionString">The connection string of the database to connect to.</param>
    /// <param name="clickHouseOptionsAction">An optional action to allow additional ClickHouse-specific configuration.</param>
    /// <returns>The options builder so that further configuration can be chained.</returns>
    public static DbContextOptionsBuilder<TContext> UseClickHouse<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        string connectionString,
        Action<ClickHouseDbContextOptionsBuilder>? clickHouseOptionsAction = null)
        where TContext : DbContext
    {
        ((DbContextOptionsBuilder)optionsBuilder).UseClickHouse(connectionString, clickHouseOptionsAction);
        return optionsBuilder;
    }

    /// <summary>
    /// Gets or creates the ClickHouse options extension.
    /// </summary>
    private static ClickHouseOptionsExtension GetOrCreateExtension(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.Options.FindExtension<ClickHouseOptionsExtension>()
           ?? new ClickHouseOptionsExtension();

    /// <summary>
    /// Configures warnings for ClickHouse-specific scenarios.
    /// </summary>
    private static void ConfigureWarnings(DbContextOptionsBuilder optionsBuilder)
    {
        // Future: Configure ClickHouse-specific warnings here
    }
}

/// <summary>
/// Allows ClickHouse-specific configuration for a <see cref="DbContext"/>.
/// </summary>
public class ClickHouseDbContextOptionsBuilder
{
    private readonly DbContextOptionsBuilder _optionsBuilder;

    /// <summary>
    /// Initializes a new instance of <see cref="ClickHouseDbContextOptionsBuilder"/>.
    /// </summary>
    public ClickHouseDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
    {
        _optionsBuilder = optionsBuilder;
    }

    /// <summary>
    /// Gets the core options builder.
    /// </summary>
    protected virtual DbContextOptionsBuilder OptionsBuilder => _optionsBuilder;

    /// <summary>
    /// Sets the command timeout (in seconds) for database commands.
    /// </summary>
    /// <param name="commandTimeout">The command timeout in seconds.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public virtual ClickHouseDbContextOptionsBuilder CommandTimeout(int? commandTimeout)
    {
        var extension = GetOrCreateExtension().WithCommandTimeout(commandTimeout);
        ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(extension);
        return this;
    }

    /// <summary>
    /// Enables or disables query splitting for collection includes.
    /// Note: ClickHouse doesn't support traditional joins well, so this affects query strategy.
    /// </summary>
    /// <param name="useQuerySplitting">Whether to use query splitting.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public virtual ClickHouseDbContextOptionsBuilder UseQuerySplitting(bool useQuerySplitting = true)
    {
        var extension = GetOrCreateExtension().WithQuerySplitting(useQuerySplitting);
        ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(extension);
        return this;
    }

    /// <summary>
    /// Sets the maximum batch size for bulk insert operations.
    /// ClickHouse is optimized for large batch inserts.
    /// </summary>
    /// <param name="maxBatchSize">Maximum number of rows per batch (default: 10000).</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public virtual ClickHouseDbContextOptionsBuilder MaxBatchSize(int maxBatchSize)
    {
        if (maxBatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBatchSize), "Batch size must be positive.");
        }

        var extension = GetOrCreateExtension().WithMaxBatchSize(maxBatchSize);
        ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(extension);
        return this;
    }

    /// <summary>
    /// Configures all entities to be keyless by default (no primary key).
    /// This is appropriate for append-only ClickHouse tables where EF Core change tracking isn't needed.
    /// Entities configured with explicit HasKey() in OnModelCreating will override this default.
    /// </summary>
    /// <remarks>
    /// When enabled:
    /// - All entities default to HasNoKey()
    /// - Use UseMergeTree() to specify ORDER BY columns
    /// - SaveChanges() will not work (entities are read-only)
    /// - Entities can still be queried via LINQ
    ///
    /// Example:
    /// <code>
    /// options.UseClickHouse("...", o => o.UseKeylessEntitiesByDefault());
    ///
    /// // In OnModelCreating:
    /// modelBuilder.Entity&lt;EventLog&gt;()
    ///     .UseMergeTree("Timestamp", "EventType");  // ORDER BY columns
    /// </code>
    /// </remarks>
    /// <param name="useKeylessEntities">Whether to make entities keyless by default (default: true).</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public virtual ClickHouseDbContextOptionsBuilder UseKeylessEntitiesByDefault(bool useKeylessEntities = true)
    {
        var extension = GetOrCreateExtension().WithKeylessEntitiesByDefault(useKeylessEntities);
        ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(extension);
        return this;
    }

    /// <summary>
    /// Configures the DELETE strategy for ClickHouse operations.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Lightweight</b> (default): Uses <c>DELETE FROM ... WHERE ...</c> syntax.
    /// Rows are marked as deleted immediately and filtered from queries.
    /// Physical deletion occurs during background merges. Returns affected row count.
    /// </para>
    /// <para>
    /// <b>Mutation</b>: Uses <c>ALTER TABLE ... DELETE WHERE ...</c> syntax.
    /// Asynchronous operation that rewrites data parts. Does not return affected row count.
    /// Use for bulk maintenance operations only.
    /// </para>
    /// </remarks>
    /// <param name="strategy">The delete strategy to use.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public virtual ClickHouseDbContextOptionsBuilder UseDeleteStrategy(ClickHouseDeleteStrategy strategy)
    {
        var extension = GetOrCreateExtension().WithDeleteStrategy(strategy);
        ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(extension);
        return this;
    }

    #region Multi-Datacenter Configuration

    /// <summary>
    /// Configures ClickHouse from an IConfiguration section.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Binds configuration from appsettings.json to <see cref="ClickHouseConfiguration"/>.
    /// This enables multi-datacenter setups with connection routing and table groups.
    /// </para>
    /// </remarks>
    /// <param name="configurationSection">The configuration section containing ClickHouse settings.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <example>
    /// <code>
    /// // In appsettings.json:
    /// // {
    /// //   "ClickHouse": {
    /// //     "Connections": { "Primary": { "WriteEndpoint": "dc1:8123", ... } },
    /// //     "Clusters": { "geo_cluster": { "Connection": "Primary" } },
    /// //     "TableGroups": { "Core": { "Cluster": "geo_cluster", "Replicated": true } }
    /// //   }
    /// // }
    ///
    /// options.UseClickHouse("Host=localhost", o => o
    ///     .FromConfiguration(config.GetSection("ClickHouse"))
    ///     .UseConnectionRouting());
    /// </code>
    /// </example>
    public virtual ClickHouseDbContextOptionsBuilder FromConfiguration(IConfigurationSection configurationSection)
    {
        ArgumentNullException.ThrowIfNull(configurationSection);

        var configuration = new ClickHouseConfiguration();
        configurationSection.Bind(configuration);

        var extension = GetOrCreateExtension().WithConfiguration(configuration);
        ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(extension);
        return this;
    }

    /// <summary>
    /// Configures ClickHouse with a pre-built configuration object.
    /// </summary>
    /// <param name="configuration">The cluster configuration.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public virtual ClickHouseDbContextOptionsBuilder WithConfiguration(ClickHouseConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var extension = GetOrCreateExtension().WithConfiguration(configuration);
        ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(extension);
        return this;
    }

    /// <summary>
    /// Enables connection routing (read/write splitting) for multi-datacenter setups.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When enabled, SELECT queries are routed to read endpoints while
    /// INSERT/UPDATE/DELETE/ALTER operations go to write endpoints.
    /// </para>
    /// <para>
    /// Requires <see cref="FromConfiguration"/> or programmatic endpoint configuration.
    /// </para>
    /// </remarks>
    /// <param name="enabled">Whether to enable connection routing (default: true).</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public virtual ClickHouseDbContextOptionsBuilder UseConnectionRouting(bool enabled = true)
    {
        var extension = GetOrCreateExtension().WithConnectionRouting(enabled);
        ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(extension);
        return this;
    }

    /// <summary>
    /// Sets the default cluster name for DDL operations (ON CLUSTER clause).
    /// </summary>
    /// <remarks>
    /// <para>
    /// When a cluster is specified, DDL statements (CREATE TABLE, ALTER TABLE, DROP TABLE)
    /// will include an ON CLUSTER clause, causing them to execute on all cluster nodes.
    /// </para>
    /// <para>
    /// Entity-level <c>UseCluster()</c> or table group configuration takes precedence over this default.
    /// </para>
    /// </remarks>
    /// <param name="clusterName">The cluster name as defined in ClickHouse server configuration.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public virtual ClickHouseDbContextOptionsBuilder UseCluster(string clusterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterName);

        var extension = GetOrCreateExtension().WithClusterName(clusterName);
        ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(extension);
        return this;
    }

    /// <summary>
    /// Adds a named connection configuration using a fluent builder.
    /// </summary>
    /// <param name="name">The connection name (e.g., "Primary", "Analytics").</param>
    /// <param name="configure">Action to configure the connection.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <example>
    /// <code>
    /// options.UseClickHouse("Host=localhost", o => o
    ///     .AddConnection("Primary", conn => conn
    ///         .Database("production")
    ///         .WriteEndpoint("dc1-clickhouse:8123")
    ///         .ReadEndpoints("dc2-clickhouse:8123", "dc1-clickhouse:8123")
    ///         .ReadStrategy(ReadStrategy.PreferFirst)));
    /// </code>
    /// </example>
    public virtual ClickHouseDbContextOptionsBuilder AddConnection(
        string name,
        Action<ConnectionConfigBuilder> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var extension = GetOrCreateExtension();
        var configuration = extension.Configuration ?? new ClickHouseConfiguration();

        var connectionBuilder = new ConnectionConfigBuilder();
        configure(connectionBuilder);
        configuration.Connections[name] = connectionBuilder.Build();

        extension = extension.WithConfiguration(configuration);
        ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(extension);
        return this;
    }

    /// <summary>
    /// Adds a named cluster configuration using a fluent builder.
    /// </summary>
    /// <param name="name">The cluster name (e.g., "geo_cluster").</param>
    /// <param name="configure">Action to configure the cluster.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <example>
    /// <code>
    /// options.UseClickHouse("Host=localhost", o => o
    ///     .AddCluster("geo_cluster", cluster => cluster
    ///         .UseConnection("Primary")
    ///         .WithReplication(r => r
    ///             .ZooKeeperBasePath("/clickhouse/geo/{database}"))));
    /// </code>
    /// </example>
    public virtual ClickHouseDbContextOptionsBuilder AddCluster(
        string name,
        Action<ClusterConfigBuilder> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var extension = GetOrCreateExtension();
        var configuration = extension.Configuration ?? new ClickHouseConfiguration();

        var clusterBuilder = new ClusterConfigBuilder();
        configure(clusterBuilder);
        configuration.Clusters[name] = clusterBuilder.Build();

        extension = extension.WithConfiguration(configuration);
        ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(extension);
        return this;
    }

    /// <summary>
    /// Adds a named table group configuration using a fluent builder.
    /// </summary>
    /// <param name="name">The table group name (e.g., "Core", "LocalCache").</param>
    /// <param name="configure">Action to configure the table group.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    /// <example>
    /// <code>
    /// options.UseClickHouse("Host=localhost", o => o
    ///     .AddTableGroup("Core", group => group
    ///         .UseCluster("geo_cluster")
    ///         .Replicated())
    ///     .AddTableGroup("LocalCache", group => group
    ///         .NoCluster()
    ///         .NotReplicated()));
    /// </code>
    /// </example>
    public virtual ClickHouseDbContextOptionsBuilder AddTableGroup(
        string name,
        Action<TableGroupConfigBuilder> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var extension = GetOrCreateExtension();
        var configuration = extension.Configuration ?? new ClickHouseConfiguration();

        var groupBuilder = new TableGroupConfigBuilder();
        configure(groupBuilder);
        configuration.TableGroups[name] = groupBuilder.Build();

        extension = extension.WithConfiguration(configuration);
        ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(extension);
        return this;
    }

    /// <summary>
    /// Sets the default table group for entities that don't specify one explicitly.
    /// </summary>
    /// <param name="tableGroupName">The default table group name.</param>
    /// <returns>The same builder instance for method chaining.</returns>
    public virtual ClickHouseDbContextOptionsBuilder DefaultTableGroup(string tableGroupName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableGroupName);

        var extension = GetOrCreateExtension();
        var configuration = extension.Configuration ?? new ClickHouseConfiguration();
        configuration.Defaults.TableGroup = tableGroupName;

        extension = extension.WithConfiguration(configuration);
        ((IDbContextOptionsBuilderInfrastructure)_optionsBuilder).AddOrUpdateExtension(extension);
        return this;
    }

    #endregion

    private ClickHouseOptionsExtension GetOrCreateExtension()
        => _optionsBuilder.Options.FindExtension<ClickHouseOptionsExtension>()
           ?? new ClickHouseOptionsExtension();
}

#region Fluent Builders

/// <summary>
/// Builder for configuring a connection.
/// </summary>
public class ConnectionConfigBuilder
{
    private readonly ConnectionConfig _config = new();

    /// <summary>
    /// Sets the database name.
    /// </summary>
    public ConnectionConfigBuilder Database(string database)
    {
        _config.Database = database;
        return this;
    }

    /// <summary>
    /// Sets the write endpoint (host:port).
    /// </summary>
    public ConnectionConfigBuilder WriteEndpoint(string endpoint)
    {
        _config.WriteEndpoint = endpoint;
        return this;
    }

    /// <summary>
    /// Sets the read endpoints (host:port).
    /// </summary>
    public ConnectionConfigBuilder ReadEndpoints(params string[] endpoints)
    {
        _config.ReadEndpoints = [.. endpoints];
        return this;
    }

    /// <summary>
    /// Sets the read strategy.
    /// </summary>
    public ConnectionConfigBuilder ReadStrategy(ReadStrategy strategy)
    {
        _config.ReadStrategy = strategy;
        return this;
    }

    /// <summary>
    /// Configures failover settings.
    /// </summary>
    public ConnectionConfigBuilder WithFailover(Action<FailoverConfigBuilder> configure)
    {
        var builder = new FailoverConfigBuilder();
        configure(builder);
        _config.Failover = builder.Build();
        return this;
    }

    /// <summary>
    /// Sets authentication credentials.
    /// </summary>
    public ConnectionConfigBuilder Credentials(string username, string password)
    {
        _config.Username = username;
        _config.Password = password;
        return this;
    }

    internal ConnectionConfig Build() => _config;
}

/// <summary>
/// Builder for configuring failover settings.
/// </summary>
public class FailoverConfigBuilder
{
    private readonly FailoverConfig _config = new();

    /// <summary>
    /// Enables or disables failover.
    /// </summary>
    public FailoverConfigBuilder Enabled(bool enabled = true)
    {
        _config.Enabled = enabled;
        return this;
    }

    /// <summary>
    /// Sets the maximum retry attempts.
    /// </summary>
    public FailoverConfigBuilder MaxRetries(int retries)
    {
        _config.MaxRetries = retries;
        return this;
    }

    /// <summary>
    /// Sets the retry delay in milliseconds.
    /// </summary>
    public FailoverConfigBuilder RetryDelayMs(int delayMs)
    {
        _config.RetryDelayMs = delayMs;
        return this;
    }

    /// <summary>
    /// Sets the health check interval in milliseconds.
    /// </summary>
    public FailoverConfigBuilder HealthCheckIntervalMs(int intervalMs)
    {
        _config.HealthCheckIntervalMs = intervalMs;
        return this;
    }

    internal FailoverConfig Build() => _config;
}

/// <summary>
/// Builder for configuring a cluster.
/// </summary>
public class ClusterConfigBuilder
{
    private readonly ClusterConfig _config = new();

    /// <summary>
    /// Sets the connection name for this cluster.
    /// </summary>
    public ClusterConfigBuilder UseConnection(string connectionName)
    {
        _config.Connection = connectionName;
        return this;
    }

    /// <summary>
    /// Configures replication settings.
    /// </summary>
    public ClusterConfigBuilder WithReplication(Action<ReplicationConfigBuilder> configure)
    {
        var builder = new ReplicationConfigBuilder();
        configure(builder);
        _config.Replication = builder.Build();
        return this;
    }

    internal ClusterConfig Build() => _config;
}

/// <summary>
/// Builder for configuring replication settings.
/// </summary>
public class ReplicationConfigBuilder
{
    private readonly ReplicationConfig _config = new();

    /// <summary>
    /// Sets the ZooKeeper/Keeper base path.
    /// Supports placeholders: {database}, {table}, {uuid}
    /// </summary>
    public ReplicationConfigBuilder ZooKeeperBasePath(string path)
    {
        _config.ZooKeeperBasePath = path;
        return this;
    }

    /// <summary>
    /// Sets the replica name macro (default: "{replica}").
    /// </summary>
    public ReplicationConfigBuilder ReplicaNameMacro(string macro)
    {
        _config.ReplicaNameMacro = macro;
        return this;
    }

    internal ReplicationConfig Build() => _config;
}

/// <summary>
/// Builder for configuring a table group.
/// </summary>
public class TableGroupConfigBuilder
{
    private readonly TableGroupConfig _config = new();

    /// <summary>
    /// Sets the cluster for this table group.
    /// </summary>
    public TableGroupConfigBuilder UseCluster(string clusterName)
    {
        _config.Cluster = clusterName;
        return this;
    }

    /// <summary>
    /// Marks tables in this group as not using any cluster (local only).
    /// </summary>
    public TableGroupConfigBuilder NoCluster()
    {
        _config.Cluster = null;
        return this;
    }

    /// <summary>
    /// Marks tables in this group as using replicated engines.
    /// </summary>
    public TableGroupConfigBuilder Replicated()
    {
        _config.Replicated = true;
        return this;
    }

    /// <summary>
    /// Marks tables in this group as not using replicated engines.
    /// </summary>
    public TableGroupConfigBuilder NotReplicated()
    {
        _config.Replicated = false;
        return this;
    }

    /// <summary>
    /// Sets a description for the table group.
    /// </summary>
    public TableGroupConfigBuilder Description(string description)
    {
        _config.Description = description;
        return this;
    }

    internal TableGroupConfig Build() => _config;
}

#endregion
