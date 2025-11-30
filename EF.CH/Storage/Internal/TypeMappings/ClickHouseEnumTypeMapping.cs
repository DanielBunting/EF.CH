using System.Globalization;
using System.Text;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Type mapping for ClickHouse Enum8/Enum16 types.
/// Maps C# enums to ClickHouse enum types.
/// </summary>
/// <remarks>
/// ClickHouse enum syntax: Enum8('name1' = value1, 'name2' = value2, ...)
/// - Enum8: values in range [-128, 127], up to 256 values
/// - Enum16: values in range [-32768, 32767], up to 65536 values
///
/// The type is auto-selected based on the number of enum values and their range.
/// </remarks>
public class ClickHouseEnumTypeMapping : RelationalTypeMapping
{
    private readonly string _enumDefinition;

    public ClickHouseEnumTypeMapping(Type enumType)
        : this(enumType, BuildEnumDefinition(enumType))
    {
    }

    private ClickHouseEnumTypeMapping(Type enumType, string enumDefinition)
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(
                enumType,
                converter: CreateEnumToStringConverter(enumType)),
            enumDefinition,
            StoreTypePostfix.None,
            System.Data.DbType.String))
    {
        _enumDefinition = enumDefinition;
    }

    protected ClickHouseEnumTypeMapping(RelationalTypeMappingParameters parameters, string enumDefinition)
        : base(parameters)
    {
        _enumDefinition = enumDefinition;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseEnumTypeMapping(parameters, _enumDefinition);

    /// <summary>
    /// Generates a SQL literal for an enum value.
    /// ClickHouse accepts enum values as strings.
    /// </summary>
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        // If value is already a string (from converter), use it directly
        if (value is string stringValue)
        {
            return $"'{EscapeString(stringValue)}'";
        }

        // Otherwise get the enum name
        var enumValue = (Enum)value;
        var name = Enum.GetName(enumValue.GetType(), enumValue);
        return $"'{EscapeString(name ?? enumValue.ToString())}'";
    }

    /// <summary>
    /// Builds the ClickHouse enum type definition from a C# enum type.
    /// Auto-selects Enum8 or Enum16 based on the number and range of values.
    /// </summary>
    private static string BuildEnumDefinition(Type enumType)
    {
        if (!enumType.IsEnum)
        {
            throw new ArgumentException($"Type {enumType.Name} is not an enum type.", nameof(enumType));
        }

        var underlyingType = Enum.GetUnderlyingType(enumType);
        var values = Enum.GetValues(enumType);
        var names = Enum.GetNames(enumType);

        var builder = new StringBuilder();
        var minValue = long.MaxValue;
        var maxValue = long.MinValue;

        // Build the enum members and track min/max values
        var members = new List<string>();
        for (int i = 0; i < names.Length; i++)
        {
            var name = names[i];
            var value = Convert.ToInt64(values.GetValue(i), CultureInfo.InvariantCulture);

            minValue = Math.Min(minValue, value);
            maxValue = Math.Max(maxValue, value);

            members.Add($"'{EscapeString(name)}' = {value}");
        }

        // Determine if we can use Enum8 or need Enum16
        // Enum8: [-128, 127], Enum16: [-32768, 32767]
        var useEnum8 = minValue >= sbyte.MinValue && maxValue <= sbyte.MaxValue && names.Length <= 256;
        var enumTypeName = useEnum8 ? "Enum8" : "Enum16";

        builder.Append(enumTypeName);
        builder.Append('(');
        builder.Append(string.Join(", ", members));
        builder.Append(')');

        return builder.ToString();
    }

    /// <summary>
    /// Creates a value converter that converts enum values to/from strings.
    /// </summary>
    private static ValueConverter CreateEnumToStringConverter(Type enumType)
    {
        var converterType = typeof(EnumToStringConverter<>).MakeGenericType(enumType);
        return (ValueConverter)Activator.CreateInstance(converterType)!;
    }

    /// <summary>
    /// Escapes single quotes in string values for SQL literals.
    /// </summary>
    private static string EscapeString(string value)
    {
        return value.Replace("'", "\\'");
    }
}

/// <summary>
/// Value converter for enum to string conversion.
/// </summary>
internal class EnumToStringConverter<TEnum> : ValueConverter<TEnum, string>
    where TEnum : struct, Enum
{
    public EnumToStringConverter()
        : base(
            e => e.ToString(),
            s => Enum.Parse<TEnum>(s))
    {
    }
}
