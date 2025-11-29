using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

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

    private ClickHouseOptionsExtension GetOrCreateExtension()
        => _optionsBuilder.Options.FindExtension<ClickHouseOptionsExtension>()
           ?? new ClickHouseOptionsExtension();
}
