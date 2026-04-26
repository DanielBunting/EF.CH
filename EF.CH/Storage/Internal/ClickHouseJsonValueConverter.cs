using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EF.CH.Storage.Internal;

/// <summary>
/// Value converter for typed JSON POCOs that serializes to and from JSON strings.
/// </summary>
/// <remarks>
/// <para>
/// This converter enables mapping C# POCOs to ClickHouse JSON columns.
/// The POCO is serialized to JSON on write and deserialized on read.
/// </para>
/// <para>
/// Uses snake_case naming by default for ClickHouse compatibility.
/// </para>
/// </remarks>
/// <typeparam name="T">The POCO type to convert.</typeparam>
public class ClickHouseJsonValueConverter<T> : ValueConverter<T, string>
    where T : class
{
    private static readonly JsonSerializerOptions DefaultOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false
    };

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseJsonValueConverter{T}"/> class
    /// with default JSON serialization options.
    /// </summary>
    public ClickHouseJsonValueConverter()
        : this(DefaultOptions)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseJsonValueConverter{T}"/> class
    /// with custom JSON serialization options.
    /// </summary>
    /// <param name="options">The JSON serialization options to use.</param>
    public ClickHouseJsonValueConverter(JsonSerializerOptions options)
        : base(
            v => JsonSerializer.Serialize(v, options),
            v => JsonSerializer.Deserialize<T>(v, options)!)
    {
    }
}

/// <summary>
/// Bridges a property declared as <see cref="JsonDocument"/> to a raw JSON string on the wire.
/// </summary>
/// <remarks>
/// The driver returns <c>JsonObject</c> (not <c>JsonElement</c>) for native ClickHouse JSON
/// columns, so the converter speaks <c>string</c> — which the driver always supports for JSON
/// columns and which round-trips losslessly through the existing parameter and literal paths.
/// On read the raw text is re-parsed into a fresh <see cref="JsonDocument"/> owned by EF's
/// change tracker.
/// </remarks>
public sealed class ClickHouseJsonDocumentValueConverter : ValueConverter<JsonDocument, string>
{
    public ClickHouseJsonDocumentValueConverter()
        : base(
            doc => doc.RootElement.GetRawText(),
            s => JsonDocument.Parse(s))
    {
    }
}

/// <summary>
/// Factory for creating ClickHouse JSON value converters.
/// </summary>
public static class ClickHouseJsonValueConverterFactory
{
    private static readonly JsonSerializerOptions DefaultOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false
    };

    /// <summary>
    /// Creates a JSON value converter for the specified type.
    /// </summary>
    /// <param name="clrType">The CLR type to convert.</param>
    /// <returns>A value converter for the type.</returns>
    public static ValueConverter? Create(Type clrType)
    {
        return Create(clrType, DefaultOptions);
    }

    /// <summary>
    /// Creates a JSON value converter for the specified type with custom options.
    /// </summary>
    /// <param name="clrType">The CLR type to convert.</param>
    /// <param name="options">The JSON serialization options.</param>
    /// <returns>A value converter for the type.</returns>
    public static ValueConverter? Create(Type clrType, JsonSerializerOptions options)
    {
        if (!clrType.IsClass || clrType == typeof(string))
            return null;

        var converterType = typeof(ClickHouseJsonValueConverter<>).MakeGenericType(clrType);
        return (ValueConverter)Activator.CreateInstance(converterType, options)!;
    }
}
