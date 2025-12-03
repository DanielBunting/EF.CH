using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EF.CH.Storage.Internal;

/// <summary>
/// Value converter that converts C# null to a default value for ClickHouse storage,
/// and converts the default value back to null when reading.
/// </summary>
/// <remarks>
/// <para>
/// This converter enables storing nullable columns without using ClickHouse's Nullable(T) wrapper,
/// which has performance overhead due to the additional bitmask column.
/// </para>
/// <para>
/// When writing: null → default value, non-null → as-is
/// When reading: default value → null, other values → as-is
/// </para>
/// </remarks>
/// <typeparam name="T">The underlying value type (e.g., int, Guid, DateTime).</typeparam>
public class DefaultForNullValueConverter<T> : ValueConverter<T?, T>
    where T : struct
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DefaultForNullValueConverter{T}"/> class.
    /// </summary>
    /// <param name="defaultValue">The default value that represents null in the database.</param>
    public DefaultForNullValueConverter(T defaultValue)
        : base(
            v => v ?? defaultValue,
            v => EqualityComparer<T>.Default.Equals(v, defaultValue) ? null : v)
    {
        DefaultValue = defaultValue;
    }

    /// <summary>
    /// Gets the default value that represents null.
    /// </summary>
    public T DefaultValue { get; }
}

/// <summary>
/// Value converter for nullable strings that uses a default value instead of NULL.
/// </summary>
/// <remarks>
/// <para>
/// This is a separate class because string is a reference type and requires different
/// handling than nullable value types.
/// </para>
/// </remarks>
public class DefaultForNullStringValueConverter : ValueConverter<string?, string>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DefaultForNullStringValueConverter"/> class.
    /// </summary>
    /// <param name="defaultValue">The default value that represents null in the database (e.g., "").</param>
    public DefaultForNullStringValueConverter(string defaultValue)
        : base(
            v => v ?? defaultValue,
            v => string.Equals(v, defaultValue, StringComparison.Ordinal) ? null : v)
    {
        DefaultValue = defaultValue;
    }

    /// <summary>
    /// Gets the default value that represents null.
    /// </summary>
    public string DefaultValue { get; }
}

/// <summary>
/// Factory for creating default-for-null value converters based on type.
/// </summary>
public static class DefaultForNullValueConverterFactory
{
    /// <summary>
    /// Creates a default-for-null value converter for the specified type and default value.
    /// </summary>
    /// <param name="clrType">The CLR type of the property.</param>
    /// <param name="defaultValue">The default value to use.</param>
    /// <returns>A value converter, or null if the type is not supported.</returns>
    public static ValueConverter? Create(Type clrType, object defaultValue)
    {
        var underlyingType = Nullable.GetUnderlyingType(clrType);

        // Handle nullable value types (int?, Guid?, DateTime?, etc.)
        if (underlyingType != null)
        {
            var converterType = typeof(DefaultForNullValueConverter<>).MakeGenericType(underlyingType);
            return (ValueConverter)Activator.CreateInstance(converterType, defaultValue)!;
        }

        // Handle nullable reference types (string?)
        if (clrType == typeof(string))
        {
            return new DefaultForNullStringValueConverter((string)defaultValue);
        }

        return null;
    }
}
