using System.Text;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

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
/// ClickHouse.Driver returns FixedString as byte[], so we need a value converter.
/// </summary>
public class ClickHouseFixedStringTypeMapping : RelationalTypeMapping
{
    private static readonly FixedStringByteArrayConverter ByteArrayConverter = new();

    public int Length { get; }

    public ClickHouseFixedStringTypeMapping(int length)
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(string), ByteArrayConverter),
            $"FixedString({length})",
            StoreTypePostfix.None,
            System.Data.DbType.String,
            unicode: true,
            size: length,
            fixedLength: true,
            precision: null,
            scale: null))
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

    /// <summary>
    /// Generates a SQL literal for a string value.
    /// Handles both string (model value) and byte[] (provider value after conversion).
    /// </summary>
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        // Handle both string (model value) and byte[] (provider value)
        var stringValue = value is byte[] bytes
            ? Encoding.UTF8.GetString(bytes).TrimEnd('\0')
            : (string)value;

        var builder = new StringBuilder(stringValue.Length + 10);

        builder.Append('\'');
        EscapeStringLiteral(builder, stringValue);
        builder.Append('\'');

        return builder.ToString();
    }

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
/// Converts between string (CLR type) and byte[] (ClickHouse.Driver returns FixedString as byte[]).
/// Handles null-byte padding that ClickHouse uses for FixedString.
/// </summary>
internal class FixedStringByteArrayConverter : ValueConverter<string, byte[]>
{
    public FixedStringByteArrayConverter()
        : base(
            // Convert string to byte[] when writing to database
            str => Encoding.UTF8.GetBytes(str),
            // Convert byte[] to string when reading from database, trimming null padding
            bytes => Encoding.UTF8.GetString(bytes).TrimEnd('\0'))
    {
    }
}
