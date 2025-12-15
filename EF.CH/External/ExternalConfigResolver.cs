using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.Configuration;

namespace EF.CH.External;

/// <summary>
/// Resolves external table function configuration at runtime.
/// Credentials are resolved in order: environment variables, IConfiguration, literal values.
/// </summary>
public class ExternalConfigResolver : IExternalConfigResolver
{
    private readonly IConfiguration? _configuration;

    /// <summary>
    /// Creates a new resolver with optional IConfiguration for profile support.
    /// </summary>
    /// <param name="configuration">Optional configuration for profile lookups and fallback values.</param>
    public ExternalConfigResolver(IConfiguration? configuration = null)
    {
        _configuration = configuration;
    }

    /// <inheritdoc />
    public bool IsExternalTableFunction(IEntityType entityType)
    {
        return entityType.FindAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction)
            ?.Value is true;
    }

    /// <inheritdoc />
    public bool AreInsertsEnabled(IEntityType entityType)
    {
        var readOnly = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalReadOnly)
            ?.Value as bool? ?? true;
        return !readOnly;
    }

    /// <inheritdoc />
    public string ResolvePostgresTableFunction(IEntityType entityType)
    {
        var provider = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalProvider)
            ?.Value?.ToString();

        if (provider != "postgresql")
        {
            throw new InvalidOperationException(
                $"Entity '{entityType.ClrType.Name}' has unsupported external provider '{provider}'. " +
                "Only 'postgresql' is supported.");
        }

        // Check for connection profile first
        var profileName = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalConnectionProfile)
            ?.Value?.ToString();

        string hostPort, database, user, password, schema, table;

        if (!string.IsNullOrEmpty(profileName))
        {
            // Load from configuration profile
            (hostPort, database, user, password, schema) = ResolveFromProfile(profileName, entityType.ClrType.Name);
        }
        else
        {
            // Load from entity annotations
            hostPort = ResolveAnnotationValue(
                entityType,
                ClickHouseAnnotationNames.ExternalHostPortEnv,
                ClickHouseAnnotationNames.ExternalHostPortValue,
                "host:port");

            database = ResolveAnnotationValue(
                entityType,
                ClickHouseAnnotationNames.ExternalDatabaseEnv,
                ClickHouseAnnotationNames.ExternalDatabaseValue,
                "database");

            user = ResolveAnnotationValue(
                entityType,
                ClickHouseAnnotationNames.ExternalUserEnv,
                ClickHouseAnnotationNames.ExternalUserValue,
                "user");

            password = ResolveAnnotationValue(
                entityType,
                ClickHouseAnnotationNames.ExternalPasswordEnv,
                ClickHouseAnnotationNames.ExternalPasswordValue,
                "password");

            schema = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalSchema)
                ?.Value?.ToString() ?? "public";
        }

        table = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalTable)
            ?.Value?.ToString()
            ?? ToSnakeCase(entityType.ClrType.Name);

        // Build function call with escaped values
        // postgresql('host:port', 'database', 'table', 'user', 'password', 'schema')
        return $"postgresql('{Escape(hostPort)}', '{Escape(database)}', '{Escape(table)}', " +
               $"'{Escape(user)}', '{Escape(password)}', '{Escape(schema)}')";
    }

    private (string hostPort, string database, string user, string password, string schema) ResolveFromProfile(
        string profileName,
        string entityName)
    {
        if (_configuration == null)
        {
            throw new InvalidOperationException(
                $"External entity '{entityName}' uses connection profile '{profileName}' " +
                "but no IConfiguration was provided to ExternalConfigResolver.");
        }

        var profile = _configuration.GetSection($"ExternalConnections:{profileName}");
        if (!profile.Exists())
        {
            throw new InvalidOperationException(
                $"Connection profile '{profileName}' not found in configuration. " +
                $"Expected configuration section 'ExternalConnections:{profileName}'.");
        }

        var hostPort = ResolveProfileValue(profile, "HostPort", "HostPortEnv", profileName);
        var database = ResolveProfileValue(profile, "Database", "DatabaseEnv", profileName);
        var user = ResolveProfileValue(profile, "User", "UserEnv", profileName);
        var password = ResolveProfileValue(profile, "Password", "PasswordEnv", profileName);
        var schema = profile["Schema"] ?? "public";

        return (hostPort, database, user, password, schema);
    }

    private string ResolveProfileValue(
        IConfigurationSection profile,
        string valueKey,
        string envKey,
        string profileName)
    {
        // Try direct value first
        var value = profile[valueKey];
        if (!string.IsNullOrEmpty(value))
            return value;

        // Try environment variable reference
        var envVarName = profile[envKey];
        if (!string.IsNullOrEmpty(envVarName))
            return ResolveFromEnvironment(envVarName, $"profile '{profileName}'");

        throw new InvalidOperationException(
            $"Connection profile '{profileName}' is missing '{valueKey}' or '{envKey}'.");
    }

    private string ResolveAnnotationValue(
        IEntityType entityType,
        string envAnnotation,
        string valueAnnotation,
        string settingName)
    {
        var entityName = entityType.ClrType.Name;

        // Try environment variable first
        var envVarName = entityType.FindAnnotation(envAnnotation)?.Value?.ToString();
        if (!string.IsNullOrEmpty(envVarName))
            return ResolveFromEnvironment(envVarName, $"entity '{entityName}'");

        // Fall back to literal value
        var value = entityType.FindAnnotation(valueAnnotation)?.Value?.ToString();
        if (!string.IsNullOrEmpty(value))
            return value;

        throw new InvalidOperationException(
            $"External entity '{entityName}' is missing {settingName} configuration. " +
            $"Use Connection(c => c.{char.ToUpper(settingName[0])}{settingName[1..]}(...)) " +
            "or Connection(c => c.UseProfile(...)).");
    }

    private string ResolveFromEnvironment(string envVarName, string context)
    {
        // Try environment variable
        var value = Environment.GetEnvironmentVariable(envVarName);
        if (!string.IsNullOrEmpty(value))
            return value;

        // Fall back to IConfiguration (useful for testing)
        if (_configuration != null)
        {
            value = _configuration[envVarName];
            if (!string.IsNullOrEmpty(value))
                return value;
        }

        throw new InvalidOperationException(
            $"Environment variable '{envVarName}' is not set (required by {context}).");
    }

    /// <summary>
    /// Escapes single quotes in values for SQL safety.
    /// </summary>
    private static string Escape(string value) => value.Replace("'", "\\'");

    private static string ToSnakeCase(string str)
    {
        return string.Concat(str.Select((c, i) =>
            i > 0 && char.IsUpper(c) ? "_" + char.ToLower(c) : char.ToLower(c).ToString()));
    }
}
