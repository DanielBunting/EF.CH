using EF.CH.External;
using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for configuring external entities that use ClickHouse table functions.
/// </summary>
public static class ExternalEntityExtensions
{
    /// <summary>
    /// Configures an entity that reads from (and optionally writes to) an external PostgreSQL table
    /// via ClickHouse's postgresql() table function. No ClickHouse table is created for this entity.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="modelBuilder">The model builder.</param>
    /// <param name="configure">Optional action to configure the external entity.</param>
    /// <returns>The builder for further configuration.</returns>
    /// <example>
    /// <code>
    /// modelBuilder.ExternalPostgresEntity&lt;Customer&gt;(ext => ext
    ///     .FromTable("customers", schema: "public")
    ///     .Connection(c => c
    ///         .HostPort(env: "PG_HOSTPORT")
    ///         .Database(env: "PG_DATABASE")
    ///         .Credentials(userEnv: "PG_USER", passwordEnv: "PG_PASSWORD"))
    ///     .ReadOnly());
    /// </code>
    /// </example>
    public static ExternalPostgresEntityBuilder<TEntity> ExternalPostgresEntity<TEntity>(
        this ModelBuilder modelBuilder,
        Action<ExternalPostgresEntityBuilder<TEntity>>? configure = null)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        var builder = new ExternalPostgresEntityBuilder<TEntity>(modelBuilder);
        configure?.Invoke(builder);
        builder.Build();
        return builder;
    }
}
