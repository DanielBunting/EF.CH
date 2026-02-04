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

    /// <summary>
    /// Configures an entity that reads from (and optionally writes to) an external MySQL table
    /// via ClickHouse's mysql() table function. No ClickHouse table is created for this entity.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="modelBuilder">The model builder.</param>
    /// <param name="configure">Optional action to configure the external entity.</param>
    /// <returns>The builder for further configuration.</returns>
    /// <example>
    /// <code>
    /// modelBuilder.ExternalMySqlEntity&lt;Customer&gt;(ext => ext
    ///     .FromTable("customers")
    ///     .Connection(c => c
    ///         .HostPort(env: "MYSQL_HOSTPORT")
    ///         .Database(env: "MYSQL_DATABASE")
    ///         .Credentials(userEnv: "MYSQL_USER", passwordEnv: "MYSQL_PASSWORD"))
    ///     .ReadOnly());
    /// </code>
    /// </example>
    public static ExternalMySqlEntityBuilder<TEntity> ExternalMySqlEntity<TEntity>(
        this ModelBuilder modelBuilder,
        Action<ExternalMySqlEntityBuilder<TEntity>>? configure = null)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        var builder = new ExternalMySqlEntityBuilder<TEntity>(modelBuilder);
        configure?.Invoke(builder);
        builder.Build();
        return builder;
    }

    /// <summary>
    /// Configures an entity that reads from (and optionally writes to) an external database
    /// via ClickHouse's odbc() table function. The ODBC DSN must be pre-configured in odbc.ini.
    /// No ClickHouse table is created for this entity.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="modelBuilder">The model builder.</param>
    /// <param name="configure">Optional action to configure the external entity.</param>
    /// <returns>The builder for further configuration.</returns>
    /// <example>
    /// <code>
    /// modelBuilder.ExternalOdbcEntity&lt;SalesData&gt;(ext => ext
    ///     .FromTable("sales")
    ///     .Dsn(env: "MSSQL_DSN")
    ///     .Database("reporting")
    ///     .ReadOnly());
    /// </code>
    /// </example>
    public static ExternalOdbcEntityBuilder<TEntity> ExternalOdbcEntity<TEntity>(
        this ModelBuilder modelBuilder,
        Action<ExternalOdbcEntityBuilder<TEntity>>? configure = null)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        var builder = new ExternalOdbcEntityBuilder<TEntity>(modelBuilder);
        configure?.Invoke(builder);
        builder.Build();
        return builder;
    }

    /// <summary>
    /// Configures an entity that reads from (and optionally writes to) Redis
    /// via ClickHouse's redis() table function. No ClickHouse table is created for this entity.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="modelBuilder">The model builder.</param>
    /// <param name="configure">Optional action to configure the external entity.</param>
    /// <returns>The builder for further configuration.</returns>
    /// <example>
    /// <code>
    /// modelBuilder.ExternalRedisEntity&lt;SessionCache&gt;(ext => ext
    ///     .KeyColumn(x => x.SessionId)
    ///     .Connection(c => c
    ///         .HostPort(env: "REDIS_HOST")
    ///         .Password(env: "REDIS_PASSWORD")
    ///         .DbIndex(0))
    ///     .ReadOnly());
    /// </code>
    /// </example>
    public static ExternalRedisEntityBuilder<TEntity> ExternalRedisEntity<TEntity>(
        this ModelBuilder modelBuilder,
        Action<ExternalRedisEntityBuilder<TEntity>>? configure = null)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        var builder = new ExternalRedisEntityBuilder<TEntity>(modelBuilder);
        configure?.Invoke(builder);
        builder.Build();
        return builder;
    }

    /// <summary>
    /// Configures an entity that reads from S3-compatible storage
    /// via ClickHouse's s3() table function. No ClickHouse table is created for this entity.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="modelBuilder">The model builder.</param>
    /// <param name="configure">Optional action to configure the external entity.</param>
    /// <returns>The builder for further configuration.</returns>
    /// <example>
    /// <code>
    /// modelBuilder.ExternalS3Entity&lt;LogEntry&gt;(ext => ext
    ///     .FromPath("s3://bucket/logs/*.parquet")
    ///     .WithFormat("Parquet")
    ///     .Connection(c => c
    ///         .AccessKey(env: "AWS_ACCESS_KEY_ID")
    ///         .SecretKey(env: "AWS_SECRET_ACCESS_KEY")));
    /// </code>
    /// </example>
    public static ExternalS3EntityBuilder<TEntity> ExternalS3Entity<TEntity>(
        this ModelBuilder modelBuilder,
        Action<ExternalS3EntityBuilder<TEntity>>? configure = null)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        var builder = new ExternalS3EntityBuilder<TEntity>(modelBuilder);
        configure?.Invoke(builder);
        builder.Build();
        return builder;
    }

    /// <summary>
    /// Configures an entity that reads from an HTTP/HTTPS URL
    /// via ClickHouse's url() table function. No ClickHouse table is created for this entity.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="modelBuilder">The model builder.</param>
    /// <param name="configure">Optional action to configure the external entity.</param>
    /// <returns>The builder for further configuration.</returns>
    /// <example>
    /// <code>
    /// modelBuilder.ExternalUrlEntity&lt;WeatherData&gt;(ext => ext
    ///     .FromUrl("https://api.example.com/data.csv")
    ///     .WithFormat("CSVWithNames")
    ///     .WithHeader("Authorization", "Bearer token"));
    /// </code>
    /// </example>
    public static ExternalUrlEntityBuilder<TEntity> ExternalUrlEntity<TEntity>(
        this ModelBuilder modelBuilder,
        Action<ExternalUrlEntityBuilder<TEntity>>? configure = null)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        var builder = new ExternalUrlEntityBuilder<TEntity>(modelBuilder);
        configure?.Invoke(builder);
        builder.Build();
        return builder;
    }

    /// <summary>
    /// Configures an entity that reads from a remote ClickHouse server
    /// via ClickHouse's remote() table function. No ClickHouse table is created for this entity.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="modelBuilder">The model builder.</param>
    /// <param name="configure">Optional action to configure the external entity.</param>
    /// <returns>The builder for further configuration.</returns>
    /// <example>
    /// <code>
    /// modelBuilder.ExternalRemoteEntity&lt;AnalyticsData&gt;(ext => ext
    ///     .FromAddresses("remote-ch-01:9000,remote-ch-02:9000")
    ///     .FromTable("analytics", "events")
    ///     .Connection(c => c.Credentials("CH_USER", "CH_PASSWORD")));
    /// </code>
    /// </example>
    public static ExternalRemoteEntityBuilder<TEntity> ExternalRemoteEntity<TEntity>(
        this ModelBuilder modelBuilder,
        Action<ExternalRemoteEntityBuilder<TEntity>>? configure = null)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        var builder = new ExternalRemoteEntityBuilder<TEntity>(modelBuilder);
        configure?.Invoke(builder);
        builder.Build();
        return builder;
    }

    /// <summary>
    /// Configures an entity that reads from local files
    /// via ClickHouse's file() table function. No ClickHouse table is created for this entity.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="modelBuilder">The model builder.</param>
    /// <param name="configure">Optional action to configure the external entity.</param>
    /// <returns>The builder for further configuration.</returns>
    /// <example>
    /// <code>
    /// modelBuilder.ExternalFileEntity&lt;ImportData&gt;(ext => ext
    ///     .FromPath("/data/imports/*.csv")
    ///     .WithFormat("CSVWithNames"));
    /// </code>
    /// </example>
    public static ExternalFileEntityBuilder<TEntity> ExternalFileEntity<TEntity>(
        this ModelBuilder modelBuilder,
        Action<ExternalFileEntityBuilder<TEntity>>? configure = null)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        var builder = new ExternalFileEntityBuilder<TEntity>(modelBuilder);
        configure?.Invoke(builder);
        builder.Build();
        return builder;
    }

    /// <summary>
    /// Configures an entity that reads from tables across a ClickHouse cluster
    /// via ClickHouse's cluster() table function. No ClickHouse table is created for this entity.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="modelBuilder">The model builder.</param>
    /// <param name="configure">Optional action to configure the external entity.</param>
    /// <returns>The builder for further configuration.</returns>
    /// <example>
    /// <code>
    /// modelBuilder.ExternalClusterEntity&lt;DistributedData&gt;(ext => ext
    ///     .FromCluster("my_cluster")
    ///     .FromCurrentDatabase("local_events")
    ///     .WithShardingKey(x => x.UserId));
    /// </code>
    /// </example>
    public static ExternalClusterEntityBuilder<TEntity> ExternalClusterEntity<TEntity>(
        this ModelBuilder modelBuilder,
        Action<ExternalClusterEntityBuilder<TEntity>>? configure = null)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        var builder = new ExternalClusterEntityBuilder<TEntity>(modelBuilder);
        configure?.Invoke(builder);
        builder.Build();
        return builder;
    }
}
