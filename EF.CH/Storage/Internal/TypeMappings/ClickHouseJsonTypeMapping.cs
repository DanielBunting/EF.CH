using System.Data.Common;
using System.Text;
using System.Text.Json;
using ClickHouse.Driver.ADO.Parameters;
using EF.CH.Storage.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Type mapping for ClickHouse native JSON type (requires ClickHouse 24.8+).
/// Supports JsonElement, JsonDocument, and typed POCOs.
/// </summary>
/// <remarks>
/// ClickHouse JSON type parameters:
/// - max_dynamic_paths: Maximum number of paths stored as subcolumns (default 1024)
/// - max_dynamic_types: Maximum number of types per path (default 32)
///
/// Example DDL: JSON(max_dynamic_paths=1024, max_dynamic_types=32)
/// Query syntax: "column"."path"."subpath" for subcolumn access
/// </remarks>
public class ClickHouseJsonTypeMapping : RelationalTypeMapping
{
    /// <summary>
    /// Maximum number of dynamic paths for the JSON column.
    /// When null, ClickHouse uses its default (1024).
    /// </summary>
    public int? MaxDynamicPaths { get; }

    /// <summary>
    /// Maximum number of dynamic types per path.
    /// When null, ClickHouse uses its default (32).
    /// </summary>
    public int? MaxDynamicTypes { get; }

    /// <summary>
    /// Creates a JSON type mapping for JsonElement or JsonDocument.
    /// </summary>
    public ClickHouseJsonTypeMapping(
        Type clrType,
        int? maxDynamicPaths = null,
        int? maxDynamicTypes = null)
        : base(new RelationalTypeMappingParameters(
            BuildCoreParameters(clrType, converter: null),
            BuildStoreType(maxDynamicPaths, maxDynamicTypes),
            StoreTypePostfix.None,
            System.Data.DbType.String))
    {
        MaxDynamicPaths = maxDynamicPaths;
        MaxDynamicTypes = maxDynamicTypes;
    }

    /// <summary>
    /// Creates a JSON type mapping for a typed POCO with value converter.
    /// </summary>
    public ClickHouseJsonTypeMapping(
        Type clrType,
        ValueConverter converter,
        int? maxDynamicPaths = null,
        int? maxDynamicTypes = null)
        : base(new RelationalTypeMappingParameters(
            BuildCoreParameters(clrType, converter),
            BuildStoreType(maxDynamicPaths, maxDynamicTypes),
            StoreTypePostfix.None,
            System.Data.DbType.String))
    {
        MaxDynamicPaths = maxDynamicPaths;
        MaxDynamicTypes = maxDynamicTypes;
    }

    private static CoreTypeMappingParameters BuildCoreParameters(Type clrType, ValueConverter? converter)
    {
        if (converter is null && clrType == typeof(JsonDocument))
        {
            return new CoreTypeMappingParameters(clrType, new ClickHouseJsonDocumentValueConverter());
        }

        return converter is null
            ? new CoreTypeMappingParameters(clrType)
            : new CoreTypeMappingParameters(clrType, converter);
    }

    /// <summary>
    /// Creates a JSON type mapping from existing parameters.
    /// </summary>
    protected ClickHouseJsonTypeMapping(
        RelationalTypeMappingParameters parameters,
        int? maxDynamicPaths,
        int? maxDynamicTypes)
        : base(parameters)
    {
        MaxDynamicPaths = maxDynamicPaths;
        MaxDynamicTypes = maxDynamicTypes;
    }

    /// <summary>
    /// Builds the ClickHouse JSON store type string with optional parameters.
    /// </summary>
    private static string BuildStoreType(int? maxDynamicPaths, int? maxDynamicTypes)
    {
        if (maxDynamicPaths is null && maxDynamicTypes is null)
        {
            return "JSON";
        }

        var parts = new List<string>();
        if (maxDynamicPaths.HasValue)
        {
            parts.Add($"max_dynamic_paths={maxDynamicPaths.Value}");
        }
        if (maxDynamicTypes.HasValue)
        {
            parts.Add($"max_dynamic_types={maxDynamicTypes.Value}");
        }

        return $"JSON({string.Join(", ", parts)})";
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseJsonTypeMapping(parameters, MaxDynamicPaths, MaxDynamicTypes);

    /// <summary>
    /// Creates a copy of this mapping with modified parameters.
    /// </summary>
    public ClickHouseJsonTypeMapping WithJsonParameters(
        int? maxDynamicPaths = null,
        int? maxDynamicTypes = null)
    {
        var newMaxPaths = maxDynamicPaths ?? MaxDynamicPaths;
        var newMaxTypes = maxDynamicTypes ?? MaxDynamicTypes;

        if (newMaxPaths == MaxDynamicPaths && newMaxTypes == MaxDynamicTypes)
        {
            return this;
        }

        return new ClickHouseJsonTypeMapping(ClrType, newMaxPaths, newMaxTypes);
    }

    /// <summary>
    /// Configures a DbParameter with ClickHouse-specific settings.
    /// </summary>
    protected override void ConfigureParameter(DbParameter parameter)
    {
        base.ConfigureParameter(parameter);

        if (parameter is ClickHouseDbParameter clickHouseParam)
        {
            // Use base JSON type for parameter substitution
            clickHouseParam.ClickHouseType = "JSON";
        }
    }

    /// <summary>
    /// Generates a SQL literal for a JSON value.
    /// </summary>
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var json = value switch
        {
            JsonElement element => element.GetRawText(),
            JsonDocument document => document.RootElement.GetRawText(),
            string s => s,
            _ => JsonSerializer.Serialize(value, JsonSerializerOptions)
        };

        return $"'{EscapeJsonString(json)}'";
    }

    /// <summary>
    /// Escapes a JSON string for embedding inside a ClickHouse single-quoted SQL
    /// literal. ClickHouse interprets the standard C-style escape characters
    /// (<c>\n</c>, <c>\r</c>, <c>\t</c>, <c>\b</c>, <c>\f</c>, <c>\0</c>) inside
    /// <c>'…'</c> literals, so a JSON value containing a literal control char would
    /// otherwise produce a malformed literal. Passes ASCII printables through
    /// unchanged; non-ASCII (UTF-8) bytes are also preserved verbatim because
    /// ClickHouse string literals are UTF-8 by default.
    /// </summary>
    private static string EscapeJsonString(string json)
    {
        var builder = new StringBuilder(json.Length + 10);
        foreach (var c in json)
        {
            switch (c)
            {
                case '\'': builder.Append("\\'"); break;
                case '\\': builder.Append("\\\\"); break;
                case '\n': builder.Append("\\n"); break;
                case '\r': builder.Append("\\r"); break;
                case '\t': builder.Append("\\t"); break;
                case '\b': builder.Append("\\b"); break;
                case '\f': builder.Append("\\f"); break;
                case '\0': builder.Append("\\0"); break;
                default: builder.Append(c); break;
            }
        }
        return builder.ToString();
    }

    /// <summary>
    /// JSON serializer options for POCO serialization.
    /// Uses snake_case naming for ClickHouse compatibility.
    /// </summary>
    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        WriteIndented = false,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };
}
