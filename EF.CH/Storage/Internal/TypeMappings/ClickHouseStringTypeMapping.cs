using System.Text;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Type mapping for ClickHouse String type.
/// ClickHouse stores strings as UTF-8 encoded byte sequences.
/// </summary>
public class ClickHouseStringTypeMapping : RelationalTypeMapping
{
    /// <summary>
    /// Creates a new String type mapping.
    /// </summary>
    /// <param name="storeType">The store type name (default: "String").</param>
    public ClickHouseStringTypeMapping(string storeType = "String")
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(string)),
            storeType,
            StoreTypePostfix.None,
            System.Data.DbType.String,
            unicode: true,  // ClickHouse strings are UTF-8
            size: null,
            fixedLength: false,
            precision: null,
            scale: null))
    {
    }

    protected ClickHouseStringTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseStringTypeMapping(parameters);

    /// <summary>
    /// Generates a SQL literal for a string value.
    /// </summary>
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var stringValue = (string)value;
        var builder = new StringBuilder(stringValue.Length + 10);

        builder.Append('\'');
        EscapeStringLiteral(builder, stringValue);
        builder.Append('\'');

        return builder.ToString();
    }

    /// <summary>
    /// Escapes a string for safe use in ClickHouse SQL.
    /// </summary>
    private static void EscapeStringLiteral(StringBuilder builder, string value)
    {
        foreach (var c in value)
        {
            switch (c)
            {
                case '\'':
                    builder.Append("\\'");
                    break;
                case '\\':
                    builder.Append("\\\\");
                    break;
                case '\n':
                    builder.Append("\\n");
                    break;
                case '\r':
                    builder.Append("\\r");
                    break;
                case '\t':
                    builder.Append("\\t");
                    break;
                case '\0':
                    builder.Append("\\0");
                    break;
                default:
                    builder.Append(c);
                    break;
            }
        }
    }
}

/// <summary>
/// Type mapping for ClickHouse FixedString(N) type.
/// Fixed-length strings are padded with null bytes.
/// </summary>
public class ClickHouseFixedStringTypeMapping : ClickHouseStringTypeMapping
{
    public int Length { get; }

    public ClickHouseFixedStringTypeMapping(int length)
        : base($"FixedString({length})")
    {
        Length = length;
    }

    protected ClickHouseFixedStringTypeMapping(RelationalTypeMappingParameters parameters, int length)
        : base(parameters)
    {
        Length = length;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseFixedStringTypeMapping(parameters, Length);
}
