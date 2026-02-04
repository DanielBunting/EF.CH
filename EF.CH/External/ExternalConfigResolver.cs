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
            "s3" => ResolveS3TableFunction(entityType),
            "url" => ResolveUrlTableFunction(entityType),
            "remote" => ResolveRemoteTableFunction(entityType),
            "file" => ResolveFileTableFunction(entityType),
            "cluster" => ResolveClusterTableFunction(entityType),
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
    /// Resolves the s3() table function for S3-compatible storage.
    /// </summary>
    public string ResolveS3TableFunction(IEntityType entityType)
    {
        var path = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalS3Path)
            ?.Value?.ToString()
            ?? throw new InvalidOperationException(
                $"External S3 entity '{entityType.ClrType.Name}' requires a path. Use FromPath().");

        var format = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalS3Format)
            ?.Value?.ToString();

        var structure = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalS3Structure)
            ?.Value?.ToString();

        var compression = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalS3Compression)
            ?.Value?.ToString();

        // Resolve access key
        var accessKey = ResolveOptionalAnnotationValue(
            entityType,
            ClickHouseAnnotationNames.ExternalS3AccessKeyEnv,
            ClickHouseAnnotationNames.ExternalS3AccessKeyValue);

        // Resolve secret key
        var secretKey = ResolveOptionalAnnotationValue(
            entityType,
            ClickHouseAnnotationNames.ExternalS3SecretKeyEnv,
            ClickHouseAnnotationNames.ExternalS3SecretKeyValue);

        // Build function call
        // s3(path, [aws_access_key_id, aws_secret_access_key,] [format, [structure, [compression]]])
        var parts = new List<string> { $"'{Escape(path)}'" };

        if (!string.IsNullOrEmpty(accessKey) && !string.IsNullOrEmpty(secretKey))
        {
            parts.Add($"'{Escape(accessKey)}'");
            parts.Add($"'{Escape(secretKey)}'");
        }

        if (!string.IsNullOrEmpty(format))
        {
            parts.Add($"'{Escape(format)}'");
        }

        if (!string.IsNullOrEmpty(structure))
        {
            parts.Add($"'{Escape(structure)}'");
        }

        if (!string.IsNullOrEmpty(compression))
        {
            parts.Add($"'{Escape(compression)}'");
        }

        return $"s3({string.Join(", ", parts)})";
    }

    /// <summary>
    /// Resolves the url() table function for HTTP/HTTPS URLs.
    /// </summary>
    public string ResolveUrlTableFunction(IEntityType entityType)
    {
        // Try environment variable first, then literal URL
        var url = ResolveOptionalAnnotationValue(
            entityType,
            ClickHouseAnnotationNames.ExternalUrlEnv,
            ClickHouseAnnotationNames.ExternalUrl);

        if (string.IsNullOrEmpty(url))
        {
            throw new InvalidOperationException(
                $"External URL entity '{entityType.ClrType.Name}' requires a URL. Use FromUrl() or FromUrlEnv().");
        }

        var format = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalUrlFormat)
            ?.Value?.ToString()
            ?? throw new InvalidOperationException(
                $"External URL entity '{entityType.ClrType.Name}' requires a format. Use WithFormat().");

        var structure = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalUrlStructure)
            ?.Value?.ToString();

        var compression = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalUrlCompression)
            ?.Value?.ToString();

        // Build function call
        // url(url, format, [structure, [compression]])
        var parts = new List<string>
        {
            $"'{Escape(url)}'",
            $"'{Escape(format)}'"
        };

        if (!string.IsNullOrEmpty(structure))
        {
            parts.Add($"'{Escape(structure)}'");
        }

        if (!string.IsNullOrEmpty(compression))
        {
            parts.Add($"'{Escape(compression)}'");
        }

        return $"url({string.Join(", ", parts)})";
    }

    /// <summary>
    /// Resolves the remote() table function for remote ClickHouse servers.
    /// </summary>
    public string ResolveRemoteTableFunction(IEntityType entityType)
    {
        // Resolve addresses
        var addresses = ResolveOptionalAnnotationValue(
            entityType,
            ClickHouseAnnotationNames.ExternalRemoteAddressesEnv,
            ClickHouseAnnotationNames.ExternalRemoteAddresses);

        if (string.IsNullOrEmpty(addresses))
        {
            throw new InvalidOperationException(
                $"External remote entity '{entityType.ClrType.Name}' requires server addresses. Use FromAddresses().");
        }

        // Resolve database
        var database = ResolveOptionalAnnotationValue(
            entityType,
            ClickHouseAnnotationNames.ExternalRemoteDatabaseEnv,
            ClickHouseAnnotationNames.ExternalRemoteDatabase);

        if (string.IsNullOrEmpty(database))
        {
            throw new InvalidOperationException(
                $"External remote entity '{entityType.ClrType.Name}' requires a database. Use FromTable().");
        }

        var table = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalRemoteTable)
            ?.Value?.ToString()
            ?? throw new InvalidOperationException(
                $"External remote entity '{entityType.ClrType.Name}' requires a table. Use FromTable().");

        // Resolve optional credentials
        var user = ResolveOptionalAnnotationValue(
            entityType,
            ClickHouseAnnotationNames.ExternalRemoteUserEnv,
            ClickHouseAnnotationNames.ExternalRemoteUserValue) ?? "default";

        var password = ResolveOptionalAnnotationValue(
            entityType,
            ClickHouseAnnotationNames.ExternalRemotePasswordEnv,
            ClickHouseAnnotationNames.ExternalRemotePasswordValue) ?? "";

        var shardingKey = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalRemoteShardingKey)
            ?.Value?.ToString();

        // Build function call
        // remote('addresses', 'database', 'table', 'user', 'password', [sharding_key])
        var parts = new List<string>
        {
            $"'{Escape(addresses)}'",
            $"'{Escape(database)}'",
            $"'{Escape(table)}'",
            $"'{Escape(user)}'",
            $"'{Escape(password)}'"
        };

        if (!string.IsNullOrEmpty(shardingKey))
        {
            parts.Add(shardingKey); // sharding_key is an expression, not a string
        }

        return $"remote({string.Join(", ", parts)})";
    }

    /// <summary>
    /// Resolves the file() table function for local files.
    /// </summary>
    public string ResolveFileTableFunction(IEntityType entityType)
    {
        var path = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalFilePath)
            ?.Value?.ToString()
            ?? throw new InvalidOperationException(
                $"External file entity '{entityType.ClrType.Name}' requires a path. Use FromPath().");

        var format = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalFileFormat)
            ?.Value?.ToString();

        var structure = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalFileStructure)
            ?.Value?.ToString();

        var compression = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalFileCompression)
            ?.Value?.ToString();

        // Build function call
        // file(path, [format, [structure, [compression]]])
        var parts = new List<string> { $"'{Escape(path)}'" };

        if (!string.IsNullOrEmpty(format))
        {
            parts.Add($"'{Escape(format)}'");
        }

        if (!string.IsNullOrEmpty(structure))
        {
            parts.Add($"'{Escape(structure)}'");
        }

        if (!string.IsNullOrEmpty(compression))
        {
            parts.Add($"'{Escape(compression)}'");
        }

        return $"file({string.Join(", ", parts)})";
    }

    /// <summary>
    /// Resolves the cluster() table function for cluster-wide queries.
    /// </summary>
    public string ResolveClusterTableFunction(IEntityType entityType)
    {
        var clusterName = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalClusterName)
            ?.Value?.ToString()
            ?? throw new InvalidOperationException(
                $"External cluster entity '{entityType.ClrType.Name}' requires a cluster name. Use FromCluster().");

        var database = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalClusterDatabase)
            ?.Value?.ToString()
            ?? throw new InvalidOperationException(
                $"External cluster entity '{entityType.ClrType.Name}' requires a database. Use FromTable().");

        var table = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalClusterTable)
            ?.Value?.ToString()
            ?? throw new InvalidOperationException(
                $"External cluster entity '{entityType.ClrType.Name}' requires a table. Use FromTable().");

        var shardingKey = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalClusterShardingKey)
            ?.Value?.ToString();

        // Build function call
        // cluster('cluster_name', 'database', 'table', [sharding_key])
        // Note: database can be currentDatabase() which should not be quoted
        var databaseArg = database == "currentDatabase()"
            ? "currentDatabase()"
            : $"'{Escape(database)}'";

        var parts = new List<string>
        {
            $"'{Escape(clusterName)}'",
            databaseArg,
            $"'{Escape(table)}'"
        };

        if (!string.IsNullOrEmpty(shardingKey))
        {
            parts.Add(shardingKey); // sharding_key is an expression, not a string
        }

        return $"cluster({string.Join(", ", parts)})";
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
