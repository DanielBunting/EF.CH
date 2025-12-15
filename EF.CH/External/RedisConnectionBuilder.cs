using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.External;

/// <summary>
/// Builder for configuring Redis connection parameters for external entities.
/// </summary>
public class RedisConnectionBuilder
{
    private readonly RedisConnectionConfig _config;

    internal RedisConnectionBuilder(RedisConnectionConfig config)
    {
        _config = config;
    }

    /// <summary>
    /// Sets host:port from an environment variable or literal value.
    /// Default Redis port is 6379.
    /// </summary>
    /// <param name="env">Environment variable name (e.g., "REDIS_HOST").</param>
    /// <param name="value">Literal value (e.g., "localhost:6379") - not recommended for production.</param>
    /// <returns>The builder for chaining.</returns>
    public RedisConnectionBuilder HostPort(string? env = null, string? value = null)
    {
        if (env != null) _config.HostPortEnv = env;
        if (value != null) _config.HostPortValue = value;
        return this;
    }

    /// <summary>
    /// Sets password from an environment variable or literal value.
    /// </summary>
    /// <param name="env">Environment variable name (e.g., "REDIS_PASSWORD").</param>
    /// <param name="value">Literal value - not recommended for production.</param>
    /// <returns>The builder for chaining.</returns>
    public RedisConnectionBuilder Password(string? env = null, string? value = null)
    {
        if (env != null) _config.PasswordEnv = env;
        if (value != null) _config.PasswordValue = value;
        return this;
    }

    /// <summary>
    /// Sets the Redis database index (0-15).
    /// Default is 0.
    /// </summary>
    /// <param name="index">The database index (0-15).</param>
    /// <returns>The builder for chaining.</returns>
    public RedisConnectionBuilder DbIndex(int index)
    {
        if (index < 0 || index > 15)
        {
            throw new ArgumentOutOfRangeException(nameof(index), "Redis database index must be between 0 and 15.");
        }

        _config.DbIndex = index;
        return this;
    }

    /// <summary>
    /// Sets the Redis connection pool size.
    /// Default is 16.
    /// </summary>
    /// <param name="size">The pool size.</param>
    /// <returns>The builder for chaining.</returns>
    public RedisConnectionBuilder PoolSize(int size)
    {
        if (size < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(size), "Pool size must be at least 1.");
        }

        _config.PoolSize = size;
        return this;
    }

    /// <summary>
    /// Uses a pre-configured connection profile from appsettings.json.
    /// Looks up IConfiguration["ExternalConnections:{profileName}:*"].
    /// </summary>
    /// <param name="profileName">The profile name from configuration.</param>
    /// <returns>The builder for chaining.</returns>
    public RedisConnectionBuilder UseProfile(string profileName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profileName);
        _config.ProfileName = profileName;
        return this;
    }
}

/// <summary>
/// Internal storage for Redis connection configuration.
/// </summary>
internal class RedisConnectionConfig
{
    public string? HostPortEnv { get; set; }
    public string? HostPortValue { get; set; }
    public string? PasswordEnv { get; set; }
    public string? PasswordValue { get; set; }
    public int? DbIndex { get; set; }
    public int? PoolSize { get; set; }
    public string? ProfileName { get; set; }

    /// <summary>
    /// Applies the connection configuration as annotations on the entity type builder.
    /// </summary>
    internal void ApplyTo<T>(EntityTypeBuilder<T> builder) where T : class
    {
        // If using a profile, just store the profile name
        if (!string.IsNullOrEmpty(ProfileName))
        {
            builder.HasAnnotation(ClickHouseAnnotationNames.ExternalConnectionProfile, ProfileName);
            return;
        }

        // Store individual connection settings
        if (HostPortEnv != null)
            builder.HasAnnotation(ClickHouseAnnotationNames.ExternalHostPortEnv, HostPortEnv);
        if (HostPortValue != null)
            builder.HasAnnotation(ClickHouseAnnotationNames.ExternalHostPortValue, HostPortValue);
        if (PasswordEnv != null)
            builder.HasAnnotation(ClickHouseAnnotationNames.ExternalPasswordEnv, PasswordEnv);
        if (PasswordValue != null)
            builder.HasAnnotation(ClickHouseAnnotationNames.ExternalPasswordValue, PasswordValue);
        if (DbIndex.HasValue)
            builder.HasAnnotation(ClickHouseAnnotationNames.ExternalRedisDbIndex, DbIndex.Value);
        if (PoolSize.HasValue)
            builder.HasAnnotation(ClickHouseAnnotationNames.ExternalRedisPoolSize, PoolSize.Value);
    }
}
