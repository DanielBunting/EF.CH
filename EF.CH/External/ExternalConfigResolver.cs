using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
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
    public string ResolveTableFunction(IEntityType entityType)
    {
        var provider = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalProvider)?.Value?.ToString();

        return provider switch
        {
            "postgresql" => ResolvePostgresTableFunction(entityType),
            "mysql" => ResolveMySqlTableFunction(entityType),
            "odbc" => ResolveOdbcTableFunction(entityType),
            "redis" => ResolveRedisTableFunction(entityType),
            _ => throw new NotSupportedException($"External provider '{provider}' is not supported.")
        };
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

    /// <inheritdoc />
    public string ResolveMySqlTableFunction(IEntityType entityType)
    {
        var provider = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalProvider)
            ?.Value?.ToString();

        if (provider != "mysql")
        {
            throw new InvalidOperationException(
                $"Entity '{entityType.ClrType.Name}' has unsupported external provider '{provider}'. " +
                "Expected 'mysql'.");
        }

        // Check for connection profile first
        var profileName = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalConnectionProfile)
            ?.Value?.ToString();

        string hostPort, database, user, password;

        if (!string.IsNullOrEmpty(profileName))
        {
            // Load from configuration profile
            (hostPort, database, user, password, _) = ResolveFromProfile(profileName, entityType.ClrType.Name);
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
        }

        var table = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalTable)
            ?.Value?.ToString()
            ?? ToSnakeCase(entityType.ClrType.Name);

        // Build function call: mysql('host:port', 'database', 'table', 'user', 'password')
        return $"mysql('{Escape(hostPort)}', '{Escape(database)}', '{Escape(table)}', " +
               $"'{Escape(user)}', '{Escape(password)}')";
    }

    /// <inheritdoc />
    public string ResolveOdbcTableFunction(IEntityType entityType)
    {
        var provider = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalProvider)
            ?.Value?.ToString();

        if (provider != "odbc")
        {
            throw new InvalidOperationException(
                $"Entity '{entityType.ClrType.Name}' has unsupported external provider '{provider}'. " +
                "Expected 'odbc'.");
        }

        // Resolve DSN
        var dsn = ResolveDsn(entityType);

        var database = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalDatabaseValue)
            ?.Value?.ToString() ?? "";

        var table = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalTable)
            ?.Value?.ToString()
            ?? ToSnakeCase(entityType.ClrType.Name);

        // Build function call: odbc('DSN', 'database', 'table')
        return $"odbc('{Escape(dsn)}', '{Escape(database)}', '{Escape(table)}')";
    }

    /// <inheritdoc />
    public string ResolveRedisTableFunction(IEntityType entityType)
    {
        var provider = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalProvider)
            ?.Value?.ToString();

        if (provider != "redis")
        {
            throw new InvalidOperationException(
                $"Entity '{entityType.ClrType.Name}' has unsupported external provider '{provider}'. " +
                "Expected 'redis'.");
        }

        // Check for connection profile first
        var profileName = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalConnectionProfile)
            ?.Value?.ToString();

        string hostPort;
        string password = "";

        if (!string.IsNullOrEmpty(profileName))
        {
            (hostPort, _, _, password, _) = ResolveFromProfile(profileName, entityType.ClrType.Name);
        }
        else
        {
            hostPort = ResolveAnnotationValue(
                entityType,
                ClickHouseAnnotationNames.ExternalHostPortEnv,
                ClickHouseAnnotationNames.ExternalHostPortValue,
                "host:port");

            // Password is optional for Redis
            password = ResolveOptionalAnnotationValue(
                entityType,
                ClickHouseAnnotationNames.ExternalPasswordEnv,
                ClickHouseAnnotationNames.ExternalPasswordValue) ?? "";
        }

        var keyColumn = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalRedisKeyColumn)
            ?.Value?.ToString()
            ?? throw new InvalidOperationException(
                $"Redis external entity '{entityType.ClrType.Name}' requires a key column.");

        var structure = ResolveRedisStructure(entityType);

        var dbIndex = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalRedisDbIndex)
            ?.Value?.ToString() ?? "0";

        // Build function call: redis('host:port', 'key', 'structure', db_index, 'password')
        return $"redis('{Escape(hostPort)}', '{keyColumn}', '{structure}', {dbIndex}, '{Escape(password)}')";
    }

    private string ResolveRedisStructure(IEntityType entityType)
    {
        // Check for explicit structure first
        var explicitStructure = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalRedisStructure)
            ?.Value?.ToString();
        if (!string.IsNullOrEmpty(explicitStructure))
            return explicitStructure;

        // Auto-generate from entity properties discovered by EF Core
        var efProperties = entityType.GetProperties().ToArray();
        if (efProperties.Length > 0)
        {
            var columns = efProperties
                .Select(p => $"{p.GetColumnName()} {MapToClickHouseType(p.ClrType)}")
                .ToArray();
            return string.Join(", ", columns);
        }

        // Fallback: generate from CLR type properties via reflection
        // This handles cases where EF Core hasn't discovered properties yet
        var clrProperties = entityType.ClrType.GetProperties(
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
            .Where(p => p.CanRead && p.CanWrite)
            .Select(p => $"{p.Name} {MapToClickHouseType(p.PropertyType)}")
            .ToArray();
        return string.Join(", ", clrProperties);
    }

    private static string MapToClickHouseType(Type clrType)
    {
        // Handle nullable types
        var underlying = Nullable.GetUnderlyingType(clrType) ?? clrType;

        return underlying switch
        {
            var t when t == typeof(string) => "String",
            var t when t == typeof(int) => "Int32",
            var t when t == typeof(uint) => "UInt32",
            var t when t == typeof(long) => "Int64",
            var t when t == typeof(ulong) => "UInt64",
            var t when t == typeof(short) => "Int16",
            var t when t == typeof(ushort) => "UInt16",
            var t when t == typeof(sbyte) => "Int8",
            var t when t == typeof(byte) => "UInt8",
            var t when t == typeof(float) => "Float32",
            var t when t == typeof(double) => "Float64",
            var t when t == typeof(decimal) => "Decimal(18, 4)",
            var t when t == typeof(bool) => "Bool",
            var t when t == typeof(Guid) => "UUID",
            var t when t == typeof(DateTime) => "DateTime64(3)",
            var t when t == typeof(DateTimeOffset) => "DateTime64(3)",
            var t when t == typeof(DateOnly) => "Date",
            var t when t == typeof(TimeOnly) => "String", // ClickHouse doesn't have a Time type
            _ => "String" // Default fallback
        };
    }

    private string ResolveDsn(IEntityType entityType)
    {
        var entityName = entityType.ClrType.Name;

        // Try environment variable first
        var envVarName = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalOdbcDsnEnv)?.Value?.ToString();
        if (!string.IsNullOrEmpty(envVarName))
            return ResolveFromEnvironment(envVarName, $"entity '{entityName}'");

        // Fall back to literal value
        var value = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalOdbcDsnValue)?.Value?.ToString();
        if (!string.IsNullOrEmpty(value))
            return value;

        throw new InvalidOperationException(
            $"External ODBC entity '{entityName}' is missing DSN configuration. " +
            "Use Dsn(env: \"DSN_ENV_VAR\") or Dsn(value: \"MyDSN\").");
    }

    private string? ResolveOptionalAnnotationValue(
        IEntityType entityType,
        string envAnnotation,
        string valueAnnotation)
    {
        // Try environment variable first
        var envVarName = entityType.FindAnnotation(envAnnotation)?.Value?.ToString();
        if (!string.IsNullOrEmpty(envVarName))
        {
            var envValue = Environment.GetEnvironmentVariable(envVarName);
            if (!string.IsNullOrEmpty(envValue))
                return envValue;

            if (_configuration != null)
            {
                envValue = _configuration[envVarName];
                if (!string.IsNullOrEmpty(envValue))
                    return envValue;
            }
        }

        // Fall back to literal value
        return entityType.FindAnnotation(valueAnnotation)?.Value?.ToString();
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
