using System.Text;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.Configuration;

namespace EF.CH.Dictionaries;

/// <summary>
/// Resolves external dictionary configuration at runtime.
/// Credentials are resolved in order: environment variables, IConfiguration, literal values.
/// </summary>
public class DictionaryConfigResolver : IDictionaryConfigResolver
{
    private readonly IConfiguration? _configuration;

    /// <summary>
    /// Creates a new resolver with optional IConfiguration for profile support.
    /// </summary>
    /// <param name="configuration">Optional configuration for profile lookups and fallback values.</param>
    public DictionaryConfigResolver(IConfiguration? configuration = null)
    {
        _configuration = configuration;
    }

    /// <inheritdoc />
    public bool IsDictionary(IEntityType entityType)
    {
        return entityType.FindAnnotation(ClickHouseAnnotationNames.Dictionary)?.Value is true;
    }

    /// <inheritdoc />
    public bool IsExternalDictionary(IEntityType entityType)
    {
        if (!IsDictionary(entityType))
            return false;

        var provider = GetSourceProvider(entityType);
        return provider != "clickhouse";
    }

    /// <inheritdoc />
    public string GetSourceProvider(IEntityType entityType)
    {
        return entityType.FindAnnotation(ClickHouseAnnotationNames.DictionarySourceProvider)
            ?.Value?.ToString() ?? "clickhouse";
    }

    /// <inheritdoc />
    public string GenerateCreateDictionaryDdl(IEntityType entityType, bool ifNotExists = true)
    {
        if (!IsDictionary(entityType))
        {
            throw new InvalidOperationException(
                $"Entity '{entityType.ClrType.Name}' is not configured as a dictionary.");
        }

        var sb = new StringBuilder();
        var tableName = entityType.GetTableName() ?? ConvertToSnakeCase(entityType.ClrType.Name);

        // CREATE DICTIONARY
        sb.Append("CREATE DICTIONARY ");
        if (ifNotExists)
            sb.Append("IF NOT EXISTS ");
        sb.Append(QuoteIdentifier(tableName));
        sb.AppendLine();

        // Column definitions
        sb.AppendLine("(");
        GenerateColumnDefinitions(entityType, sb);
        sb.AppendLine(")");

        // PRIMARY KEY
        GeneratePrimaryKey(entityType, sb);

        // SOURCE
        GenerateSource(entityType, sb);

        // LAYOUT
        GenerateLayout(entityType, sb);

        // LIFETIME
        GenerateLifetime(entityType, sb);

        return sb.ToString();
    }

    /// <inheritdoc />
    public string GenerateDropDictionaryDdl(IEntityType entityType, bool ifExists = true)
    {
        var tableName = entityType.GetTableName() ?? ConvertToSnakeCase(entityType.ClrType.Name);
        return ifExists
            ? $"DROP DICTIONARY IF EXISTS {QuoteIdentifier(tableName)}"
            : $"DROP DICTIONARY {QuoteIdentifier(tableName)}";
    }

    /// <inheritdoc />
    public string GenerateReloadDictionaryDdl(IEntityType entityType)
    {
        var tableName = entityType.GetTableName() ?? ConvertToSnakeCase(entityType.ClrType.Name);
        return $"SYSTEM RELOAD DICTIONARY {QuoteIdentifier(tableName)}";
    }

    private void GenerateColumnDefinitions(IEntityType entityType, StringBuilder sb)
    {
        var defaults = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryDefaults)
            ?.Value as Dictionary<string, object>;

        var properties = entityType.GetProperties().ToList();
        for (var i = 0; i < properties.Count; i++)
        {
            var prop = properties[i];
            if (i > 0)
                sb.AppendLine(",");

            sb.Append("    ");
            sb.Append(QuoteIdentifier(prop.GetColumnName() ?? prop.Name));
            sb.Append(' ');
            sb.Append(GetClickHouseType(prop));

            // DEFAULT value
            if (defaults?.TryGetValue(prop.Name, out var defaultValue) == true)
            {
                sb.Append(" DEFAULT ");
                sb.Append(FormatDefaultValue(defaultValue));
            }
        }
        sb.AppendLine();
    }

    private void GeneratePrimaryKey(IEntityType entityType, StringBuilder sb)
    {
        var keyColumns = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns)
            ?.Value as string[];

        if (keyColumns == null || keyColumns.Length == 0)
        {
            throw new InvalidOperationException(
                $"Dictionary '{entityType.ClrType.Name}' must have key columns defined.");
        }

        sb.Append("PRIMARY KEY ");
        if (keyColumns.Length == 1)
        {
            sb.AppendLine(QuoteIdentifier(keyColumns[0]));
        }
        else
        {
            sb.Append('(');
            sb.Append(string.Join(", ", keyColumns.Select(QuoteIdentifier)));
            sb.AppendLine(")");
        }
    }

    private void GenerateSource(IEntityType entityType, StringBuilder sb)
    {
        var provider = GetSourceProvider(entityType);

        switch (provider)
        {
            case "clickhouse":
                GenerateClickHouseSource(entityType, sb);
                break;
            case "postgresql":
                GeneratePostgreSqlSource(entityType, sb);
                break;
            case "mysql":
                GenerateMySqlSource(entityType, sb);
                break;
            case "http":
                GenerateHttpSource(entityType, sb);
                break;
            default:
                throw new NotSupportedException($"Dictionary source provider '{provider}' is not supported.");
        }
    }

    private void GenerateClickHouseSource(IEntityType entityType, StringBuilder sb)
    {
        var sourceTable = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionarySource)?.Value?.ToString();
        var sourceQuery = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionarySourceQuery)?.Value?.ToString();

        sb.Append("SOURCE(CLICKHOUSE(");
        if (!string.IsNullOrEmpty(sourceQuery))
        {
            sb.Append("QUERY '");
            sb.Append(EscapeSql(sourceQuery));
            sb.Append('\'');
        }
        else
        {
            sb.Append("TABLE '");
            sb.Append(EscapeSql(sourceTable ?? entityType.GetTableName()!));
            sb.Append('\'');
        }
        sb.AppendLine("))");
    }

    private void GeneratePostgreSqlSource(IEntityType entityType, StringBuilder sb)
    {
        var (host, port) = ResolveHostPort(entityType);
        var database = ResolveDatabase(entityType);
        var (user, password) = ResolveCredentials(entityType);
        var table = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryExternalTable)?.Value?.ToString()
            ?? throw new InvalidOperationException($"PostgreSQL dictionary '{entityType.ClrType.Name}' must have a table specified.");
        var schema = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryExternalSchema)?.Value?.ToString() ?? "public";
        var where = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryExternalWhere)?.Value?.ToString();
        var invalidateQuery = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryInvalidateQuery)?.Value?.ToString();

        sb.AppendLine("SOURCE(POSTGRESQL(");
        sb.AppendLine($"    host '{EscapeSql(host)}'");
        sb.AppendLine($"    port {port}");
        sb.AppendLine($"    user '{EscapeSql(user)}'");
        sb.AppendLine($"    password '{EscapeSql(password)}'");
        sb.AppendLine($"    db '{EscapeSql(database)}'");
        sb.AppendLine($"    table '{EscapeSql(table)}'");
        sb.AppendLine($"    schema '{EscapeSql(schema)}'");

        if (!string.IsNullOrEmpty(where))
            sb.AppendLine($"    where '{EscapeSql(where)}'");
        if (!string.IsNullOrEmpty(invalidateQuery))
            sb.AppendLine($"    invalidate_query '{EscapeSql(invalidateQuery)}'");

        sb.AppendLine("))");
    }

    private void GenerateMySqlSource(IEntityType entityType, StringBuilder sb)
    {
        var (host, port) = ResolveHostPort(entityType);
        var database = ResolveDatabase(entityType);
        var (user, password) = ResolveCredentials(entityType);
        var table = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryExternalTable)?.Value?.ToString()
            ?? throw new InvalidOperationException($"MySQL dictionary '{entityType.ClrType.Name}' must have a table specified.");
        var where = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryExternalWhere)?.Value?.ToString();
        var invalidateQuery = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryInvalidateQuery)?.Value?.ToString();
        var failOnConnectionLoss = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryMySqlFailOnConnectionLoss)?.Value;

        sb.AppendLine("SOURCE(MYSQL(");
        sb.AppendLine($"    host '{EscapeSql(host)}'");
        sb.AppendLine($"    port {port}");
        sb.AppendLine($"    user '{EscapeSql(user)}'");
        sb.AppendLine($"    password '{EscapeSql(password)}'");
        sb.AppendLine($"    db '{EscapeSql(database)}'");
        sb.AppendLine($"    table '{EscapeSql(table)}'");

        if (!string.IsNullOrEmpty(where))
            sb.AppendLine($"    where '{EscapeSql(where)}'");
        if (!string.IsNullOrEmpty(invalidateQuery))
            sb.AppendLine($"    invalidate_query '{EscapeSql(invalidateQuery)}'");
        if (failOnConnectionLoss is bool fail)
            sb.AppendLine($"    fail_on_connection_loss '{(fail ? "true" : "false")}'");

        sb.AppendLine("))");
    }

    private void GenerateHttpSource(IEntityType entityType, StringBuilder sb)
    {
        var url = ResolveHttpUrl(entityType);
        var format = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryHttpFormat)?.Value?.ToString() ?? "JSONEachRow";

        sb.AppendLine("SOURCE(HTTP(");
        sb.AppendLine($"    url '{EscapeSql(url)}'");
        sb.AppendLine($"    format '{format}'");

        // Credentials (optional for HTTP)
        var userEnv = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalUserEnv)?.Value?.ToString();
        var userValue = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalUserValue)?.Value?.ToString();
        var passwordEnv = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalPasswordEnv)?.Value?.ToString();
        var passwordValue = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalPasswordValue)?.Value?.ToString();

        var user = ResolveValue(userEnv, userValue, entityType.ClrType.Name, "user", required: false);
        var password = ResolveValue(passwordEnv, passwordValue, entityType.ClrType.Name, "password", required: false);

        if (!string.IsNullOrEmpty(user) && !string.IsNullOrEmpty(password))
        {
            sb.AppendLine($"    credentials(user '{EscapeSql(user)}' password '{EscapeSql(password)}')");
        }

        // Headers
        var headers = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryHttpHeaders)?.Value as Dictionary<string, string>;
        var headersEnv = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryHttpHeaders + "Env")?.Value as Dictionary<string, string>;

        if (headers?.Count > 0 || headersEnv?.Count > 0)
        {
            sb.Append("    headers(");
            var headerList = new List<string>();

            if (headers != null)
            {
                foreach (var (key, value) in headers)
                    headerList.Add($"'{EscapeSql(key)}' '{EscapeSql(value)}'");
            }

            if (headersEnv != null)
            {
                foreach (var (key, envVar) in headersEnv)
                {
                    var value = ResolveFromEnvironment(envVar, entityType.ClrType.Name);
                    headerList.Add($"'{EscapeSql(key)}' '{EscapeSql(value)}'");
                }
            }

            sb.Append(string.Join(" ", headerList));
            sb.AppendLine(")");
        }

        sb.AppendLine("))");
    }

    private void GenerateLayout(IEntityType entityType, StringBuilder sb)
    {
        var layout = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayout)?.Value as DictionaryLayout?
            ?? DictionaryLayout.Hashed;
        var layoutOptions = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayoutOptions)?.Value as Dictionary<string, object>;

        sb.Append("LAYOUT(");
        sb.Append(GetLayoutSql(layout, layoutOptions));
        sb.AppendLine(")");
    }

    private void GenerateLifetime(IEntityType entityType, StringBuilder sb)
    {
        var lifetimeMin = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMin)?.Value as int? ?? 0;
        var lifetimeMax = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMax)?.Value as int? ?? 300;

        if (lifetimeMin == 0 && lifetimeMax == 0)
        {
            sb.AppendLine("LIFETIME(0)");
        }
        else if (lifetimeMin == lifetimeMax || lifetimeMin == 0)
        {
            sb.AppendLine($"LIFETIME({lifetimeMax})");
        }
        else
        {
            sb.AppendLine($"LIFETIME(MIN {lifetimeMin} MAX {lifetimeMax})");
        }
    }

    #region Credential Resolution

    private (string host, int port) ResolveHostPort(IEntityType entityType)
    {
        var entityName = entityType.ClrType.Name;

        // Check for profile first
        var profileName = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalConnectionProfile)?.Value?.ToString();
        if (!string.IsNullOrEmpty(profileName))
        {
            var (hostPort, _, _, _, _) = ResolveFromProfile(profileName, entityName);
            return ParseHostPort(hostPort);
        }

        // Try combined host:port
        var hostPortEnv = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalHostPortEnv)?.Value?.ToString();
        var hostPortValue = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalHostPortValue)?.Value?.ToString();
        var hostPort2 = ResolveValue(hostPortEnv, hostPortValue, entityName, "host:port", required: false);

        if (!string.IsNullOrEmpty(hostPort2))
        {
            return ParseHostPort(hostPort2);
        }

        // Try separate host and port
        var hostEnv = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalHostPortEnv + ":Host")?.Value?.ToString();
        var hostValue = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalHostPortValue + ":Host")?.Value?.ToString();
        var host = ResolveValue(hostEnv, hostValue, entityName, "host");

        var portEnv = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalHostPortEnv + ":Port")?.Value?.ToString();
        var portValue = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalHostPortValue + ":Port")?.Value;
        int port;

        if (portValue is int portInt)
        {
            port = portInt;
        }
        else if (!string.IsNullOrEmpty(portEnv))
        {
            var portStr = ResolveFromEnvironment(portEnv, entityName);
            port = int.Parse(portStr);
        }
        else
        {
            // Default ports by provider
            var provider = GetSourceProvider(entityType);
            port = provider switch
            {
                "postgresql" => 5432,
                "mysql" => 3306,
                _ => throw new InvalidOperationException($"Dictionary '{entityName}' is missing port configuration.")
            };
        }

        return (host, port);
    }

    private static (string host, int port) ParseHostPort(string hostPort)
    {
        var parts = hostPort.Split(':');
        if (parts.Length == 2 && int.TryParse(parts[1], out var port))
        {
            return (parts[0], port);
        }
        throw new InvalidOperationException($"Invalid host:port format: '{hostPort}'. Expected 'hostname:port'.");
    }

    private string ResolveDatabase(IEntityType entityType)
    {
        var entityName = entityType.ClrType.Name;

        // Check for profile first
        var profileName = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalConnectionProfile)?.Value?.ToString();
        if (!string.IsNullOrEmpty(profileName))
        {
            var (_, database, _, _, _) = ResolveFromProfile(profileName, entityName);
            return database;
        }

        var dbEnv = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalDatabaseEnv)?.Value?.ToString();
        var dbValue = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalDatabaseValue)?.Value?.ToString();
        return ResolveValue(dbEnv, dbValue, entityName, "database");
    }

    private (string user, string password) ResolveCredentials(IEntityType entityType)
    {
        var entityName = entityType.ClrType.Name;

        // Check for profile first
        var profileName = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalConnectionProfile)?.Value?.ToString();
        if (!string.IsNullOrEmpty(profileName))
        {
            var (_, _, user, password, _) = ResolveFromProfile(profileName, entityName);
            return (user, password);
        }

        var userEnv = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalUserEnv)?.Value?.ToString();
        var userValue = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalUserValue)?.Value?.ToString();
        var user2 = ResolveValue(userEnv, userValue, entityName, "user");

        var passwordEnv = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalPasswordEnv)?.Value?.ToString();
        var passwordValue = entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalPasswordValue)?.Value?.ToString();
        var password2 = ResolveValue(passwordEnv, passwordValue, entityName, "password");

        return (user2, password2);
    }

    private string ResolveHttpUrl(IEntityType entityType)
    {
        var entityName = entityType.ClrType.Name;

        var urlEnv = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryHttpUrl + "Env")?.Value?.ToString();
        var urlValue = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryHttpUrl)?.Value?.ToString();

        return ResolveValue(urlEnv, urlValue, entityName, "url");
    }

    private (string hostPort, string database, string user, string password, string schema) ResolveFromProfile(
        string profileName,
        string entityName)
    {
        if (_configuration == null)
        {
            throw new InvalidOperationException(
                $"Dictionary '{entityName}' uses connection profile '{profileName}' " +
                "but no IConfiguration was provided to DictionaryConfigResolver.");
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
        var value = profile[valueKey];
        if (!string.IsNullOrEmpty(value))
            return value;

        var envVarName = profile[envKey];
        if (!string.IsNullOrEmpty(envVarName))
            return ResolveFromEnvironment(envVarName, $"profile '{profileName}'");

        throw new InvalidOperationException(
            $"Connection profile '{profileName}' is missing '{valueKey}' or '{envKey}'.");
    }

    private string ResolveValue(string? envVarName, string? literalValue, string entityName, string settingName, bool required = true)
    {
        // Try environment variable first
        if (!string.IsNullOrEmpty(envVarName))
        {
            var value = Environment.GetEnvironmentVariable(envVarName);
            if (!string.IsNullOrEmpty(value))
                return value;

            // Fall back to IConfiguration
            if (_configuration != null)
            {
                value = _configuration[envVarName];
                if (!string.IsNullOrEmpty(value))
                    return value;
            }

            if (required)
            {
                throw new InvalidOperationException(
                    $"Environment variable '{envVarName}' is not set (required by dictionary '{entityName}').");
            }
        }

        // Fall back to literal value
        if (!string.IsNullOrEmpty(literalValue))
            return literalValue;

        if (required)
        {
            throw new InvalidOperationException(
                $"Dictionary '{entityName}' is missing {settingName} configuration.");
        }

        return string.Empty;
    }

    private string ResolveFromEnvironment(string envVarName, string context)
    {
        var value = Environment.GetEnvironmentVariable(envVarName);
        if (!string.IsNullOrEmpty(value))
            return value;

        if (_configuration != null)
        {
            value = _configuration[envVarName];
            if (!string.IsNullOrEmpty(value))
                return value;
        }

        throw new InvalidOperationException(
            $"Environment variable '{envVarName}' is not set (required by {context}).");
    }

    #endregion

    #region Helpers

    private static string GetLayoutSql(DictionaryLayout layout, Dictionary<string, object>? options)
    {
        var layoutName = layout switch
        {
            DictionaryLayout.Flat => "FLAT",
            DictionaryLayout.Hashed => "HASHED",
            DictionaryLayout.HashedArray => "HASHED_ARRAY",
            DictionaryLayout.ComplexKeyHashed => "COMPLEX_KEY_HASHED",
            DictionaryLayout.ComplexKeyHashedArray => "COMPLEX_KEY_HASHED_ARRAY",
            DictionaryLayout.RangeHashed => "RANGE_HASHED",
            DictionaryLayout.Cache => "CACHE",
            DictionaryLayout.Direct => "DIRECT",
            _ => "HASHED"
        };

        if (options == null || options.Count == 0)
            return $"{layoutName}()";

        var optionParts = options.Select(kvp =>
        {
            var value = kvp.Value switch
            {
                bool b => b ? "1" : "0",
                string s => $"'{s}'",
                _ => kvp.Value.ToString()
            };
            return $"{kvp.Key.ToUpperInvariant()} {value}";
        });

        return $"{layoutName}({string.Join(" ", optionParts)})";
    }

    private static string GetClickHouseType(IProperty property)
    {
        // Check for explicit column type first
        // Note: GetColumnType() may fail if the model was built without a database provider
        try
        {
            var columnType = property.GetColumnType();
            if (!string.IsNullOrEmpty(columnType))
                return columnType;
        }
        catch (InvalidOperationException)
        {
            // Model wasn't initialized with a provider - fall back to CLR type mapping
        }

        var clrType = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;

        return clrType switch
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
            var t when t == typeof(TimeOnly) => "String",
            _ => "String"
        };
    }

    private static string FormatDefaultValue(object value)
    {
        return value switch
        {
            string s => $"'{EscapeSql(s)}'",
            bool b => b ? "1" : "0",
            DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss}'",
            _ => value.ToString() ?? ""
        };
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\\\"")}\"";
    }

    private static string EscapeSql(string value)
    {
        return value.Replace("'", "\\'");
    }

    private static string ConvertToSnakeCase(string name)
    {
        if (string.IsNullOrEmpty(name))
            return name;

        var result = new StringBuilder();
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                if (i > 0)
                    result.Append('_');
                result.Append(char.ToLowerInvariant(c));
            }
            else
            {
                result.Append(c);
            }
        }
        return result.ToString();
    }

    #endregion
}
