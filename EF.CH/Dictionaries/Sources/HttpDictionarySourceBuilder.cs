namespace EF.CH.Dictionaries.Sources;

/// <summary>
/// Builder for configuring HTTP as a dictionary source.
/// </summary>
public class HttpDictionarySourceBuilder
{
    private readonly HttpDictionarySourceConfig _config = new();

    /// <summary>
    /// Specifies the URL endpoint for the HTTP source.
    /// </summary>
    /// <param name="env">Environment variable name containing the URL.</param>
    /// <param name="value">Literal URL value.</param>
    /// <returns>The builder for chaining.</returns>
    public HttpDictionarySourceBuilder Url(string? env = null, string? value = null)
    {
        if (env != null) _config.UrlEnv = env;
        if (value != null) _config.UrlValue = value;
        return this;
    }

    /// <summary>
    /// Specifies the data format of the HTTP response.
    /// Common formats: JSONEachRow, CSV, TSV, Values.
    /// </summary>
    /// <param name="format">The data format (e.g., "JSONEachRow").</param>
    /// <returns>The builder for chaining.</returns>
    public HttpDictionarySourceBuilder Format(string format)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(format);
        _config.Format = format;
        return this;
    }

    /// <summary>
    /// Configures HTTP Basic Authentication credentials.
    /// </summary>
    /// <param name="userEnv">Environment variable name for username.</param>
    /// <param name="passwordEnv">Environment variable name for password.</param>
    /// <returns>The builder for chaining.</returns>
    public HttpDictionarySourceBuilder Credentials(string userEnv, string passwordEnv)
    {
        _config.UserEnv = userEnv;
        _config.PasswordEnv = passwordEnv;
        return this;
    }

    /// <summary>
    /// Configures HTTP Basic Authentication credentials with literal values.
    /// Not recommended for production - use environment variables instead.
    /// </summary>
    /// <param name="user">Username.</param>
    /// <param name="password">Password.</param>
    /// <returns>The builder for chaining.</returns>
    public HttpDictionarySourceBuilder CredentialsLiteral(string user, string password)
    {
        _config.UserValue = user;
        _config.PasswordValue = password;
        return this;
    }

    /// <summary>
    /// Adds a custom HTTP header to requests.
    /// </summary>
    /// <param name="headerName">The header name.</param>
    /// <param name="value">The header value (literal).</param>
    /// <returns>The builder for chaining.</returns>
    public HttpDictionarySourceBuilder Header(string headerName, string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(headerName);
        _config.Headers[headerName] = value;
        return this;
    }

    /// <summary>
    /// Adds a custom HTTP header with value from an environment variable.
    /// </summary>
    /// <param name="headerName">The header name.</param>
    /// <param name="env">Environment variable name containing the header value.</param>
    /// <returns>The builder for chaining.</returns>
    public HttpDictionarySourceBuilder HeaderFromEnv(string headerName, string env)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(headerName);
        ArgumentException.ThrowIfNullOrWhiteSpace(env);
        _config.HeadersEnv[headerName] = env;
        return this;
    }

    /// <summary>
    /// Uses a pre-configured connection profile from appsettings.json.
    /// Looks up IConfiguration["ExternalConnections:{profileName}:*"].
    /// </summary>
    /// <param name="profileName">The profile name from configuration.</param>
    /// <returns>The builder for chaining.</returns>
    public HttpDictionarySourceBuilder UseProfile(string profileName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profileName);
        _config.ProfileName = profileName;
        return this;
    }

    /// <summary>
    /// Gets the built configuration.
    /// </summary>
    internal HttpDictionarySourceConfig Build() => _config;
}

/// <summary>
/// Internal storage for HTTP dictionary source configuration.
/// </summary>
internal class HttpDictionarySourceConfig
{
    public string? UrlEnv { get; set; }
    public string? UrlValue { get; set; }
    public string Format { get; set; } = "JSONEachRow";

    // Credentials
    public string? UserEnv { get; set; }
    public string? UserValue { get; set; }
    public string? PasswordEnv { get; set; }
    public string? PasswordValue { get; set; }

    // Headers (literal values)
    public Dictionary<string, string> Headers { get; } = new();

    // Headers from environment variables
    public Dictionary<string, string> HeadersEnv { get; } = new();

    // Profile-based configuration
    public string? ProfileName { get; set; }
}
