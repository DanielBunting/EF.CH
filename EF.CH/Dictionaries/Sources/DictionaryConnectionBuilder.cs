namespace EF.CH.Dictionaries.Sources;

/// <summary>
/// Builder for configuring database connection parameters for dictionary sources.
/// Credentials can be provided via environment variables (recommended) or literal values.
/// </summary>
public class DictionaryConnectionBuilder
{
    private readonly DictionaryConnectionConfig _config;

    internal DictionaryConnectionBuilder(DictionaryConnectionConfig config)
    {
        _config = config;
    }

    /// <summary>
    /// Sets host:port from an environment variable or literal value.
    /// </summary>
    /// <param name="env">Environment variable name (e.g., "PG_HOSTPORT").</param>
    /// <param name="value">Literal value (e.g., "localhost:5432") - not recommended for production.</param>
    /// <returns>The builder for chaining.</returns>
    public DictionaryConnectionBuilder HostPort(string? env = null, string? value = null)
    {
        if (env != null) _config.HostPortEnv = env;
        if (value != null) _config.HostPortValue = value;
        return this;
    }

    /// <summary>
    /// Sets host from an environment variable or literal value.
    /// Use this with Port() for separate host/port configuration.
    /// </summary>
    /// <param name="env">Environment variable name.</param>
    /// <param name="value">Literal value.</param>
    /// <returns>The builder for chaining.</returns>
    public DictionaryConnectionBuilder Host(string? env = null, string? value = null)
    {
        if (env != null) _config.HostEnv = env;
        if (value != null) _config.HostValue = value;
        return this;
    }

    /// <summary>
    /// Sets port from an environment variable or literal value.
    /// Use this with Host() for separate host/port configuration.
    /// </summary>
    /// <param name="env">Environment variable name.</param>
    /// <param name="value">Literal value.</param>
    /// <returns>The builder for chaining.</returns>
    public DictionaryConnectionBuilder Port(string? env = null, int? value = null)
    {
        if (env != null) _config.PortEnv = env;
        if (value != null) _config.PortValue = value;
        return this;
    }

    /// <summary>
    /// Sets database from an environment variable or literal value.
    /// </summary>
    /// <param name="env">Environment variable name (e.g., "PG_DATABASE").</param>
    /// <param name="value">Literal value (e.g., "mydb").</param>
    /// <returns>The builder for chaining.</returns>
    public DictionaryConnectionBuilder Database(string? env = null, string? value = null)
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
    public DictionaryConnectionBuilder User(string? env = null, string? value = null)
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
    public DictionaryConnectionBuilder Password(string? env = null, string? value = null)
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
    public DictionaryConnectionBuilder Credentials(string userEnv, string passwordEnv)
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
    public DictionaryConnectionBuilder UseProfile(string profileName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profileName);
        _config.ProfileName = profileName;
        return this;
    }
}

/// <summary>
/// Internal storage for dictionary connection configuration.
/// </summary>
internal class DictionaryConnectionConfig
{
    // Combined host:port (e.g., "localhost:5432")
    public string? HostPortEnv { get; set; }
    public string? HostPortValue { get; set; }

    // Separate host and port (alternative to HostPort)
    public string? HostEnv { get; set; }
    public string? HostValue { get; set; }
    public string? PortEnv { get; set; }
    public int? PortValue { get; set; }

    // Database
    public string? DatabaseEnv { get; set; }
    public string? DatabaseValue { get; set; }

    // Credentials
    public string? UserEnv { get; set; }
    public string? UserValue { get; set; }
    public string? PasswordEnv { get; set; }
    public string? PasswordValue { get; set; }

    // Profile-based configuration
    public string? ProfileName { get; set; }
}
