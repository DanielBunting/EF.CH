using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.External;

/// <summary>
/// Builder for configuring PostgreSQL connection parameters for external entities.
/// Credentials can be provided via environment variables (recommended) or literal values.
/// </summary>
public class PostgresConnectionBuilder
{
    private readonly PostgresConnectionConfig _config;

    internal PostgresConnectionBuilder(PostgresConnectionConfig config)
    {
        _config = config;
    }

    /// <summary>
    /// Sets host:port from an environment variable or literal value.
    /// </summary>
    /// <param name="env">Environment variable name (e.g., "PG_HOSTPORT").</param>
    /// <param name="value">Literal value (e.g., "localhost:5432") - not recommended for production.</param>
    /// <returns>The builder for chaining.</returns>
    public PostgresConnectionBuilder HostPort(string? env = null, string? value = null)
    {
        if (env != null) _config.HostPortEnv = env;
        if (value != null) _config.HostPortValue = value;
        return this;
    }

    /// <summary>
    /// Sets database from an environment variable or literal value.
    /// </summary>
    /// <param name="env">Environment variable name (e.g., "PG_DATABASE").</param>
    /// <param name="value">Literal value (e.g., "mydb").</param>
    /// <returns>The builder for chaining.</returns>
    public PostgresConnectionBuilder Database(string? env = null, string? value = null)
    {
        if (env != null) _config.DatabaseEnv = env;
        if (value != null) _config.DatabaseValue = value;
        return this;
    }

    /// <summary>
    /// Sets username from an environment variable or literal value.
    /// </summary>
    /// <param name="env">Environment variable name (e.g., "PG_USER").</param>
    /// <param name="value">Literal value - not recommended for production.</param>
    /// <returns>The builder for chaining.</returns>
    public PostgresConnectionBuilder User(string? env = null, string? value = null)
    {
        if (env != null) _config.UserEnv = env;
        if (value != null) _config.UserValue = value;
        return this;
    }

    /// <summary>
    /// Sets password from an environment variable or literal value.
    /// </summary>
    /// <param name="env">Environment variable name (e.g., "PG_PASSWORD").</param>
    /// <param name="value">Literal value - not recommended for production.</param>
    /// <returns>The builder for chaining.</returns>
    public PostgresConnectionBuilder Password(string? env = null, string? value = null)
    {
        if (env != null) _config.PasswordEnv = env;
        if (value != null) _config.PasswordValue = value;
        return this;
    }

    /// <summary>
    /// Sets both user and password from environment variables.
    /// Convenience method for common configuration pattern.
    /// </summary>
    /// <param name="userEnv">Environment variable name for username.</param>
    /// <param name="passwordEnv">Environment variable name for password.</param>
    /// <returns>The builder for chaining.</returns>
    public PostgresConnectionBuilder Credentials(string userEnv, string passwordEnv)
    {
        _config.UserEnv = userEnv;
        _config.PasswordEnv = passwordEnv;
        return this;
    }

    /// <summary>
    /// Uses a pre-configured connection profile from appsettings.json.
    /// Looks up IConfiguration["ExternalConnections:{profileName}:*"].
    /// </summary>
    /// <param name="profileName">The profile name from configuration.</param>
    /// <returns>The builder for chaining.</returns>
    public PostgresConnectionBuilder UseProfile(string profileName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profileName);
        _config.ProfileName = profileName;
        return this;
    }
}

/// <summary>
/// Internal storage for PostgreSQL connection configuration.
/// </summary>
internal class PostgresConnectionConfig
{
    public string? HostPortEnv { get; set; }
    public string? HostPortValue { get; set; }
    public string? DatabaseEnv { get; set; }
    public string? DatabaseValue { get; set; }
    public string? UserEnv { get; set; }
    public string? UserValue { get; set; }
    public string? PasswordEnv { get; set; }
    public string? PasswordValue { get; set; }
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
        if (DatabaseEnv != null)
            builder.HasAnnotation(ClickHouseAnnotationNames.ExternalDatabaseEnv, DatabaseEnv);
        if (DatabaseValue != null)
            builder.HasAnnotation(ClickHouseAnnotationNames.ExternalDatabaseValue, DatabaseValue);
        if (UserEnv != null)
            builder.HasAnnotation(ClickHouseAnnotationNames.ExternalUserEnv, UserEnv);
        if (UserValue != null)
            builder.HasAnnotation(ClickHouseAnnotationNames.ExternalUserValue, UserValue);
        if (PasswordEnv != null)
            builder.HasAnnotation(ClickHouseAnnotationNames.ExternalPasswordEnv, PasswordEnv);
        if (PasswordValue != null)
            builder.HasAnnotation(ClickHouseAnnotationNames.ExternalPasswordValue, PasswordValue);
    }
}
