using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using ClickHouse.Client.ADO;
using EF.CH.Metadata;
using EF.CH.Storage.Internal;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;

namespace EF.CH.Scaffolding.Internal;

/// <summary>
/// Database model factory for ClickHouse. Queries system tables to build
/// a DatabaseModel for reverse engineering/scaffolding.
/// </summary>
public partial class ClickHouseDatabaseModelFactory : IDatabaseModelFactory
{
    private readonly ILogger<ClickHouseDatabaseModelFactory> _logger;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly ClickHouseEngineParser _engineParser;

    // Track discovered enum definitions to avoid duplicates
    private readonly Dictionary<string, string> _enumDefinitions = new();

    public ClickHouseDatabaseModelFactory(
        ILogger<ClickHouseDatabaseModelFactory> logger,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _logger = logger;
        _typeMappingSource = typeMappingSource;
        _engineParser = new ClickHouseEngineParser();
    }

    /// <inheritdoc />
    public DatabaseModel Create(string connectionString, DatabaseModelFactoryOptions options)
    {
        using var connection = new ClickHouseConnection(connectionString);
        connection.Open();
        return Create(connection, options);
    }

    /// <inheritdoc />
    public DatabaseModel Create(DbConnection connection, DatabaseModelFactoryOptions options)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(options);

        var databaseModel = new DatabaseModel();

        // Get database name from connection
        var database = GetDatabaseName(connection);
        databaseModel.DatabaseName = database;

        _logger.LogDebug("Scaffolding database: {Database}", database);

        // Query tables (filtered by options if specified)
        var tables = GetTables(connection, database, options);
        _logger.LogDebug("Found {Count} tables", tables.Count);

        if (tables.Count == 0)
        {
            return databaseModel;
        }

        // Query columns for all tables
        var columnsByTable = GetColumns(connection, database, tables.Select(t => t.Name).ToList());

        // Build database model
        foreach (var table in tables)
        {
            var dbTable = CreateDatabaseTable(databaseModel, table, columnsByTable);
            databaseModel.Tables.Add(dbTable);
        }

        return databaseModel;
    }

    /// <summary>
    /// Gets the database name from the connection.
    /// </summary>
    private static string GetDatabaseName(DbConnection connection)
    {
        // ClickHouse connection string has Database parameter
        if (!string.IsNullOrEmpty(connection.Database))
        {
            return connection.Database;
        }

        // Default database
        return "default";
    }

    /// <summary>
    /// Queries system.tables to get table metadata.
    /// </summary>
    private List<TableInfo> GetTables(DbConnection connection, string database, DatabaseModelFactoryOptions options)
    {
        var tables = new List<TableInfo>();

        // ClickHouse.Client uses inline parameters with {name:Type} syntax
        // For safety, we escape single quotes in the database name
        var escapedDatabase = database.Replace("'", "\\'");

        using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT
                database,
                name,
                engine,
                engine_full,
                sorting_key,
                partition_key,
                primary_key,
                sampling_key,
                comment
            FROM system.tables
            WHERE database = '{escapedDatabase}'
              AND engine NOT IN ('View', 'MaterializedView', 'LiveView', 'Dictionary')
              AND name NOT LIKE '.%'
              AND is_temporary = 0
            ORDER BY name
            """;

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var tableName = reader.GetString("name");

            // Apply table filter if specified
            if (options.Tables.Any() && !options.Tables.Contains(tableName))
            {
                continue;
            }

            tables.Add(new TableInfo
            {
                Database = reader.GetString("database"),
                Name = tableName,
                Engine = reader.GetString("engine"),
                EngineFull = reader.GetString("engine_full"),
                SortingKey = reader.IsDBNull("sorting_key") ? null : reader.GetString("sorting_key"),
                PartitionKey = reader.IsDBNull("partition_key") ? null : reader.GetString("partition_key"),
                PrimaryKey = reader.IsDBNull("primary_key") ? null : reader.GetString("primary_key"),
                SamplingKey = reader.IsDBNull("sampling_key") ? null : reader.GetString("sampling_key"),
                Comment = reader.IsDBNull("comment") ? null : reader.GetString("comment")
            });
        }

        return tables;
    }

    /// <summary>
    /// Queries system.columns to get column metadata for the specified tables.
    /// </summary>
    private ILookup<string, ColumnInfo> GetColumns(DbConnection connection, string database, List<string> tableNames)
    {
        var columns = new List<ColumnInfo>();

        if (tableNames.Count == 0)
        {
            return columns.ToLookup(c => c.Table);
        }

        // ClickHouse.Client uses inline parameters - escape values
        var escapedDatabase = database.Replace("'", "\\'");
        var escapedTables = tableNames.Select(t => $"'{t.Replace("'", "\\'")}'");

        using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT
                database,
                table,
                name,
                type,
                position,
                default_kind,
                default_expression,
                comment,
                is_in_partition_key,
                is_in_sorting_key,
                is_in_primary_key,
                is_in_sampling_key
            FROM system.columns
            WHERE database = '{escapedDatabase}'
              AND table IN ({string.Join(", ", escapedTables)})
            ORDER BY database, table, position
            """;

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            columns.Add(new ColumnInfo
            {
                Database = reader.GetString("database"),
                Table = reader.GetString("table"),
                Name = reader.GetString("name"),
                Type = reader.GetString("type"),
                Position = Convert.ToInt32(reader.GetValue(reader.GetOrdinal("position"))),
                DefaultKind = reader.IsDBNull("default_kind") ? null : reader.GetString("default_kind"),
                DefaultExpression = reader.IsDBNull("default_expression") ? null : reader.GetString("default_expression"),
                Comment = reader.IsDBNull("comment") ? null : reader.GetString("comment"),
                IsInPartitionKey = Convert.ToBoolean(reader.GetValue(reader.GetOrdinal("is_in_partition_key"))),
                IsInSortingKey = Convert.ToBoolean(reader.GetValue(reader.GetOrdinal("is_in_sorting_key"))),
                IsInPrimaryKey = Convert.ToBoolean(reader.GetValue(reader.GetOrdinal("is_in_primary_key"))),
                IsInSamplingKey = Convert.ToBoolean(reader.GetValue(reader.GetOrdinal("is_in_sampling_key")))
            });
        }

        return columns.ToLookup(c => c.Table);
    }

    /// <summary>
    /// Creates a DatabaseTable from table and column metadata.
    /// </summary>
    private DatabaseTable CreateDatabaseTable(
        DatabaseModel databaseModel,
        TableInfo table,
        ILookup<string, ColumnInfo> columnsByTable)
    {
        var dbTable = new DatabaseTable
        {
            Database = databaseModel,
            Name = table.Name,
            Schema = null, // ClickHouse uses databases, not schemas
            Comment = table.Comment
        };

        // Parse engine metadata
        var engineMetadata = _engineParser.Parse(
            table.Engine,
            table.EngineFull,
            table.SortingKey,
            table.PartitionKey,
            table.PrimaryKey,
            table.SamplingKey);

        // Apply engine annotations
        ApplyEngineAnnotations(dbTable, engineMetadata);

        // Add columns
        foreach (var column in columnsByTable[table.Name].OrderBy(c => c.Position))
        {
            var dbColumn = CreateDatabaseColumn(dbTable, column);
            dbTable.Columns.Add(dbColumn);
        }

        // Set up primary key from columns marked as primary key
        var primaryKeyColumns = dbTable.Columns
            .Where(c => columnsByTable[table.Name].First(col => col.Name == c.Name).IsInPrimaryKey)
            .ToList();

        if (primaryKeyColumns.Count > 0)
        {
            dbTable.PrimaryKey = new DatabasePrimaryKey
            {
                Table = dbTable,
                Name = $"PK_{table.Name}"
            };
            foreach (var col in primaryKeyColumns)
            {
                dbTable.PrimaryKey.Columns.Add(col);
            }
        }

        return dbTable;
    }

    /// <summary>
    /// Creates a DatabaseColumn from column metadata.
    /// </summary>
    private DatabaseColumn CreateDatabaseColumn(DatabaseTable dbTable, ColumnInfo column)
    {
        var storeType = column.Type;

        // Check for Nullable wrapper
        var isNullable = storeType.StartsWith("Nullable(", StringComparison.OrdinalIgnoreCase);
        var innerType = isNullable
            ? storeType.Substring(9, storeType.Length - 10)
            : storeType;

        // Handle LowCardinality - strip it for type mapping
        var lowCardinalityMatch = LowCardinalityRegex().Match(innerType);
        if (lowCardinalityMatch.Success)
        {
            innerType = lowCardinalityMatch.Groups[1].Value;
        }

        // Get CLR type from type mapping source
        Type clrType;
        try
        {
            // Cast to ClickHouseTypeMappingSource to use internal method
            var clickHouseTypeMappingSource = _typeMappingSource as ClickHouseTypeMappingSource;
            var mapping = clickHouseTypeMappingSource?.FindMappingByStoreType(innerType);

            clrType = mapping?.ClrType ?? typeof(string);
        }
        catch
        {
            // Fallback for unknown types
            clrType = typeof(string);
            _logger.LogWarning("Unknown ClickHouse type '{Type}' for column '{Column}', mapping to string",
                innerType, column.Name);
        }

        // Handle enum types - check if it's an enum definition
        var enumInfo = ParseEnumDefinition(innerType);
        if (enumInfo is not null)
        {
            // Store enum definition for code generation
            var enumTypeName = GenerateEnumTypeName(column.Table, column.Name);
            _enumDefinitions[enumTypeName] = innerType;
        }

        var dbColumn = new DatabaseColumn
        {
            Table = dbTable,
            Name = column.Name,
            StoreType = storeType,
            IsNullable = isNullable,
            DefaultValueSql = column.DefaultExpression,
            Comment = column.Comment,
            ValueGenerated = column.DefaultKind switch
            {
                "DEFAULT" => Microsoft.EntityFrameworkCore.Metadata.ValueGenerated.OnAdd,
                "MATERIALIZED" => Microsoft.EntityFrameworkCore.Metadata.ValueGenerated.OnAddOrUpdate,
                _ => null
            }
        };

        return dbColumn;
    }

    /// <summary>
    /// Applies engine metadata as annotations on the DatabaseTable.
    /// </summary>
    private static void ApplyEngineAnnotations(DatabaseTable table, EngineMetadata metadata)
    {
        // Engine name
        table[ClickHouseAnnotationNames.Engine] = metadata.EngineName;

        // ORDER BY
        if (metadata.OrderBy.Length > 0)
        {
            table[ClickHouseAnnotationNames.OrderBy] = metadata.OrderBy;
        }

        // PARTITION BY
        if (!string.IsNullOrEmpty(metadata.PartitionBy))
        {
            table[ClickHouseAnnotationNames.PartitionBy] = metadata.PartitionBy;
        }

        // PRIMARY KEY (only if different from ORDER BY)
        if (metadata.PrimaryKey is { Length: > 0 } &&
            !metadata.PrimaryKey.SequenceEqual(metadata.OrderBy))
        {
            table[ClickHouseAnnotationNames.PrimaryKey] = metadata.PrimaryKey;
        }

        // SAMPLE BY
        if (!string.IsNullOrEmpty(metadata.SampleBy))
        {
            table[ClickHouseAnnotationNames.SampleBy] = metadata.SampleBy;
        }

        // Version column (ReplacingMergeTree, VersionedCollapsingMergeTree)
        if (!string.IsNullOrEmpty(metadata.VersionColumn))
        {
            table[ClickHouseAnnotationNames.VersionColumn] = metadata.VersionColumn;
        }

        // Sign column (CollapsingMergeTree, VersionedCollapsingMergeTree)
        if (!string.IsNullOrEmpty(metadata.SignColumn))
        {
            table[ClickHouseAnnotationNames.SignColumn] = metadata.SignColumn;
        }
    }

    /// <summary>
    /// Parses an enum definition from a ClickHouse Enum8/Enum16 type string.
    /// </summary>
    /// <returns>List of (name, value) pairs, or null if not an enum type.</returns>
    private static List<(string Name, int Value)>? ParseEnumDefinition(string typeString)
    {
        var match = EnumDefinitionRegex().Match(typeString);
        if (!match.Success)
        {
            return null;
        }

        var content = match.Groups[1].Value;
        var members = new List<(string, int)>();

        // Parse 'name' = value pairs
        var memberMatches = EnumMemberRegex().Matches(content);
        foreach (Match memberMatch in memberMatches)
        {
            var name = memberMatch.Groups[1].Value;
            var value = int.Parse(memberMatch.Groups[2].Value);
            members.Add((name, value));
        }

        return members.Count > 0 ? members : null;
    }

    /// <summary>
    /// Generates a C# enum type name from table and column names.
    /// </summary>
    private static string GenerateEnumTypeName(string tableName, string columnName)
    {
        // Convert to PascalCase and create a reasonable enum name
        var tableNamePascal = ToPascalCase(tableName);
        var columnNamePascal = ToPascalCase(columnName);

        // If column already ends with common suffixes, use as-is
        if (columnNamePascal.EndsWith("Status") ||
            columnNamePascal.EndsWith("Type") ||
            columnNamePascal.EndsWith("Kind") ||
            columnNamePascal.EndsWith("State") ||
            columnNamePascal.EndsWith("Mode"))
        {
            return columnNamePascal;
        }

        return $"{tableNamePascal}{columnNamePascal}";
    }

    /// <summary>
    /// Converts a snake_case or kebab-case string to PascalCase.
    /// </summary>
    private static string ToPascalCase(string input)
    {
        if (string.IsNullOrEmpty(input))
        {
            return input;
        }

        var words = input.Split(['_', '-', ' '], StringSplitOptions.RemoveEmptyEntries);
        return string.Concat(words.Select(w =>
            char.ToUpperInvariant(w[0]) + (w.Length > 1 ? w.Substring(1).ToLowerInvariant() : "")));
    }

    [GeneratedRegex(@"^LowCardinality\((.+)\)$", RegexOptions.IgnoreCase)]
    private static partial Regex LowCardinalityRegex();

    [GeneratedRegex(@"^Enum(?:8|16)\((.+)\)$", RegexOptions.IgnoreCase)]
    private static partial Regex EnumDefinitionRegex();

    [GeneratedRegex(@"'([^']+)'\s*=\s*(-?\d+)")]
    private static partial Regex EnumMemberRegex();

    /// <summary>
    /// Internal table metadata from system.tables.
    /// </summary>
    private class TableInfo
    {
        public required string Database { get; init; }
        public required string Name { get; init; }
        public required string Engine { get; init; }
        public required string EngineFull { get; init; }
        public string? SortingKey { get; init; }
        public string? PartitionKey { get; init; }
        public string? PrimaryKey { get; init; }
        public string? SamplingKey { get; init; }
        public string? Comment { get; init; }
    }

    /// <summary>
    /// Internal column metadata from system.columns.
    /// </summary>
    private class ColumnInfo
    {
        public required string Database { get; init; }
        public required string Table { get; init; }
        public required string Name { get; init; }
        public required string Type { get; init; }
        public int Position { get; init; }
        public string? DefaultKind { get; init; }
        public string? DefaultExpression { get; init; }
        public string? Comment { get; init; }
        public bool IsInPartitionKey { get; init; }
        public bool IsInSortingKey { get; init; }
        public bool IsInPrimaryKey { get; init; }
        public bool IsInSamplingKey { get; init; }
    }
}
