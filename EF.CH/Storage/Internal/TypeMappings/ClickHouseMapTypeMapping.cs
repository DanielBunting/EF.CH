using System.Collections;
using System.Text;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Type mapping for ClickHouse Map(K, V) type.
/// Maps to .NET Dictionary&lt;TKey, TValue&gt; or IDictionary&lt;TKey, TValue&gt;.
/// </summary>
/// <remarks>
/// ClickHouse Map syntax: Map(KeyType, ValueType)
/// Literal format: {'key1': value1, 'key2': value2}
/// Internally stored as Array(Tuple(K, V)).
///
/// Key type restrictions: Cannot be Nullable or LowCardinality(Nullable).
/// </remarks>
public class ClickHouseMapTypeMapping : RelationalTypeMapping
{
    /// <summary>
    /// The type mapping for the map key type.
    /// </summary>
    public RelationalTypeMapping KeyMapping { get; }

    /// <summary>
    /// The type mapping for the map value type.
    /// </summary>
    public RelationalTypeMapping ValueMapping { get; }

    public ClickHouseMapTypeMapping(
        Type clrType,
        RelationalTypeMapping keyMapping,
        RelationalTypeMapping valueMapping)
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(clrType),
            $"Map({keyMapping.StoreType}, {valueMapping.StoreType})",
            StoreTypePostfix.None,
            System.Data.DbType.Object))
    {
        KeyMapping = keyMapping;
        ValueMapping = valueMapping;
    }

    protected ClickHouseMapTypeMapping(
        RelationalTypeMappingParameters parameters,
        RelationalTypeMapping keyMapping,
        RelationalTypeMapping valueMapping)
        : base(parameters)
    {
        KeyMapping = keyMapping;
        ValueMapping = valueMapping;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseMapTypeMapping(parameters, KeyMapping, ValueMapping);

    /// <summary>
    /// Generates a SQL literal for a map/dictionary value.
    /// Format: {'key1': value1, 'key2': value2}
    /// </summary>
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var builder = new StringBuilder();
        builder.Append('{');

        var first = true;

        if (value is IDictionary dict)
        {
            foreach (DictionaryEntry entry in dict)
            {
                if (!first)
                {
                    builder.Append(", ");
                }
                first = false;

                // Generate key literal
                if (entry.Key is null)
                {
                    throw new InvalidOperationException("Map keys cannot be null in ClickHouse.");
                }
                builder.Append(KeyMapping.GenerateSqlLiteral(entry.Key));

                builder.Append(": ");

                // Generate value literal
                if (entry.Value is null)
                {
                    builder.Append("NULL");
                }
                else
                {
                    builder.Append(ValueMapping.GenerateSqlLiteral(entry.Value));
                }
            }
        }

        builder.Append('}');
        return builder.ToString();
    }
}
