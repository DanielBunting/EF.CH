using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Type mapping for ClickHouse Nested type.
/// Maps .NET List&lt;T&gt; or T[] where T is a record/class to ClickHouse Nested(field1 Type1, field2 Type2, ...).
/// </summary>
/// <remarks>
/// ClickHouse Nested types are syntactic sugar for parallel arrays.
/// Nested(ID UInt32, Name String) is stored as:
/// - ID Array(UInt32)
/// - Name Array(String)
///
/// All parallel arrays must have the same length.
/// Insert format: ([id1, id2], ['name1', 'name2'])
/// </remarks>
public class ClickHouseNestedTypeMapping : RelationalTypeMapping
{
    /// <summary>
    /// The element type (the record/class type, e.g., Goal).
    /// </summary>
    public Type ElementType { get; }

    /// <summary>
    /// The field mappings: (PropertyName, TypeMapping) pairs.
    /// </summary>
    public IReadOnlyList<(string Name, PropertyInfo Property, RelationalTypeMapping Mapping)> FieldMappings { get; }

    public ClickHouseNestedTypeMapping(
        Type clrType,
        Type elementType,
        IReadOnlyList<(string Name, PropertyInfo Property, RelationalTypeMapping Mapping)> fieldMappings)
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(
                clrType,
                CreateValueConverter(clrType, elementType, fieldMappings)),
            BuildStoreType(fieldMappings),
            StoreTypePostfix.None,
            System.Data.DbType.Object))
    {
        ElementType = elementType;
        FieldMappings = fieldMappings;
    }

    protected ClickHouseNestedTypeMapping(
        RelationalTypeMappingParameters parameters,
        Type elementType,
        IReadOnlyList<(string Name, PropertyInfo Property, RelationalTypeMapping Mapping)> fieldMappings)
        : base(parameters)
    {
        ElementType = elementType;
        FieldMappings = fieldMappings;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseNestedTypeMapping(parameters, ElementType, FieldMappings);

    /// <summary>
    /// Creates a value converter that converts from ClickHouse's tuple array format
    /// (Tuple&lt;T1, T2, ...&gt;[]) back to List&lt;TElement&gt;.
    /// </summary>
    private static ValueConverter? CreateValueConverter(
        Type clrType,
        Type elementType,
        IReadOnlyList<(string Name, PropertyInfo Property, RelationalTypeMapping Mapping)> fieldMappings)
    {
        // Build the tuple type that ClickHouse.Driver returns
        var tupleFieldTypes = fieldMappings.Select(f => f.Property.PropertyType).ToArray();
        var tupleType = tupleFieldTypes.Length switch
        {
            1 => typeof(Tuple<>).MakeGenericType(tupleFieldTypes),
            2 => typeof(Tuple<,>).MakeGenericType(tupleFieldTypes),
            3 => typeof(Tuple<,,>).MakeGenericType(tupleFieldTypes),
            4 => typeof(Tuple<,,,>).MakeGenericType(tupleFieldTypes),
            5 => typeof(Tuple<,,,,>).MakeGenericType(tupleFieldTypes),
            6 => typeof(Tuple<,,,,,>).MakeGenericType(tupleFieldTypes),
            7 => typeof(Tuple<,,,,,,>).MakeGenericType(tupleFieldTypes),
            _ => throw new NotSupportedException($"Nested types with {tupleFieldTypes.Length} fields are not supported (max 7)")
        };
        var tupleArrayType = tupleType.MakeArrayType();

        // Create the converter using a helper method
        var createMethod = typeof(ClickHouseNestedTypeMapping)
            .GetMethod(nameof(CreateTypedValueConverter), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(clrType, elementType, tupleType);

        return (ValueConverter?)createMethod.Invoke(null, [fieldMappings]);
    }

    private static ValueConverter CreateTypedValueConverter<TList, TElement, TTuple>(
        IReadOnlyList<(string Name, PropertyInfo Property, RelationalTypeMapping Mapping)> fieldMappings)
        where TList : class
        where TElement : new()
    {
        var properties = fieldMappings.Select(f => f.Property).ToArray();
        var tupleProperties = typeof(TTuple).GetProperties();

        return new ValueConverter<TList, TTuple[]>(
            // CLR to provider (List<T> -> Tuple<...>[])
            list => ConvertToTupleArray<TList, TElement, TTuple>(list, properties, tupleProperties),
            // Provider to CLR (Tuple<...>[] -> List<T>)
            tuples => ConvertFromTupleArray<TList, TElement, TTuple>(tuples, properties, tupleProperties));
    }

    private static TTuple[] ConvertToTupleArray<TList, TElement, TTuple>(
        TList list,
        PropertyInfo[] elementProperties,
        PropertyInfo[] tupleProperties)
    {
        if (list is not IList<TElement> typedList)
            return Array.Empty<TTuple>();

        var result = new TTuple[typedList.Count];
        var tupleCtorParams = elementProperties.Select(p => p.PropertyType).ToArray();
        var tupleCtor = typeof(TTuple).GetConstructor(tupleCtorParams);

        for (var i = 0; i < typedList.Count; i++)
        {
            var element = typedList[i];
            var values = elementProperties.Select(p => p.GetValue(element)).ToArray();
            result[i] = (TTuple)tupleCtor!.Invoke(values);
        }

        return result;
    }

    private static TList ConvertFromTupleArray<TList, TElement, TTuple>(
        TTuple[] tuples,
        PropertyInfo[] elementProperties,
        PropertyInfo[] tupleProperties)
        where TElement : new()
    {
        var list = new List<TElement>(tuples.Length);

        foreach (var tuple in tuples)
        {
            var element = new TElement();
            for (var i = 0; i < elementProperties.Length; i++)
            {
                var value = tupleProperties[i].GetValue(tuple);
                elementProperties[i].SetValue(element, value);
            }
            list.Add(element);
        }

        // Handle both List<T> and T[] return types
        if (typeof(TList) == typeof(List<TElement>))
        {
            return (TList)(object)list;
        }
        if (typeof(TList) == typeof(TElement[]))
        {
            return (TList)(object)list.ToArray();
        }

        // Fallback - try to cast
        return (TList)(object)list;
    }

    /// <summary>
    /// Builds the ClickHouse store type string for the nested type.
    /// Format: Nested("Field1" Type1, "Field2" Type2, ...)
    /// </summary>
    private static string BuildStoreType(
        IReadOnlyList<(string Name, PropertyInfo Property, RelationalTypeMapping Mapping)> fieldMappings)
    {
        var sb = new StringBuilder("Nested(");

        for (var i = 0; i < fieldMappings.Count; i++)
        {
            if (i > 0)
            {
                sb.Append(", ");
            }

            var (name, _, mapping) = fieldMappings[i];
            sb.Append('"');
            sb.Append(name);
            sb.Append("\" ");
            sb.Append(mapping.StoreType);
        }

        sb.Append(')');
        return sb.ToString();
    }

    /// <summary>
    /// Generates a SQL literal for a Nested value.
    /// ClickHouse expects parallel arrays for Nested inserts.
    /// Format: ([field1_val1, field1_val2], [field2_val1, field2_val2], ...)
    /// </summary>
    /// <remarks>
    /// This method handles two input formats:
    /// 1. Original CLR format: List&lt;TElement&gt; (e.g., List&lt;Goal&gt;)
    /// 2. Converted provider format: Tuple&lt;...&gt;[] (e.g., Tuple&lt;uint, DateTime&gt;[])
    /// </remarks>
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        // Collect all items from the collection
        var items = new List<object>();
        foreach (var item in (IEnumerable)value)
        {
            if (item is not null)
            {
                items.Add(item);
            }
        }

        if (items.Count == 0)
        {
            // Generate empty parallel arrays
            var emptyArrays = string.Join(", ", FieldMappings.Select(_ => "[]"));
            return $"({emptyArrays})";
        }

        // Determine if we're dealing with the original element type or converted tuple type
        var firstItem = items[0];
        var itemType = firstItem.GetType();
        var isTupleFormat = itemType.FullName?.StartsWith("System.Tuple`") ?? false;

        // Build parallel arrays for each field
        var sb = new StringBuilder("(");

        if (isTupleFormat)
        {
            // Handle Tuple[] format (provider type)
            var tupleProperties = itemType.GetProperties();

            for (var fieldIndex = 0; fieldIndex < FieldMappings.Count; fieldIndex++)
            {
                if (fieldIndex > 0) sb.Append(", ");

                var (_, _, mapping) = FieldMappings[fieldIndex];
                sb.Append('[');

                for (var itemIndex = 0; itemIndex < items.Count; itemIndex++)
                {
                    if (itemIndex > 0) sb.Append(", ");

                    var fieldValue = tupleProperties[fieldIndex].GetValue(items[itemIndex]);
                    if (fieldValue is null)
                    {
                        sb.Append("NULL");
                    }
                    else
                    {
                        sb.Append(mapping.GenerateSqlLiteral(fieldValue));
                    }
                }

                sb.Append(']');
            }
        }
        else
        {
            // Handle original element type format (List<TElement>)
            for (var fieldIndex = 0; fieldIndex < FieldMappings.Count; fieldIndex++)
            {
                if (fieldIndex > 0) sb.Append(", ");

                var (_, property, mapping) = FieldMappings[fieldIndex];
                sb.Append('[');

                for (var itemIndex = 0; itemIndex < items.Count; itemIndex++)
                {
                    if (itemIndex > 0) sb.Append(", ");

                    var fieldValue = property.GetValue(items[itemIndex]);
                    if (fieldValue is null)
                    {
                        sb.Append("NULL");
                    }
                    else
                    {
                        sb.Append(mapping.GenerateSqlLiteral(fieldValue));
                    }
                }

                sb.Append(']');
            }
        }

        sb.Append(')');
        return sb.ToString();
    }

    /// <summary>
    /// Checks if a type is suitable for Nested mapping.
    /// A type is suitable if it:
    /// - Is a class or record (not a primitive)
    /// - Has only value-type or string properties (no navigation properties)
    /// - Is not an entity type
    /// </summary>
    public static bool IsSuitableForNested(Type elementType)
    {
        // Must be a class or struct (not a primitive)
        if (elementType.IsPrimitive || elementType == typeof(string) || elementType == typeof(decimal))
        {
            return false;
        }

        // Must not be a collection itself
        if (typeof(IEnumerable).IsAssignableFrom(elementType) && elementType != typeof(string))
        {
            return false;
        }

        // Must not be a known special type
        if (elementType == typeof(Guid) ||
            elementType == typeof(DateTime) ||
            elementType == typeof(DateTimeOffset) ||
            elementType == typeof(DateOnly) ||
            elementType == typeof(TimeOnly) ||
            elementType == typeof(TimeSpan) ||
            elementType.IsEnum)
        {
            return false;
        }

        // Must have properties
        var properties = GetMappableProperties(elementType);
        if (properties.Count == 0)
        {
            return false;
        }

        // All properties must be simple types (mappable by ClickHouse)
        foreach (var prop in properties)
        {
            var propType = prop.PropertyType;
            var underlyingType = Nullable.GetUnderlyingType(propType) ?? propType;

            // Check if it's a mappable type
            if (!IsMappableSimpleType(underlyingType))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Gets the properties that can be mapped to Nested fields.
    /// </summary>
    public static IReadOnlyList<PropertyInfo> GetMappableProperties(Type elementType)
    {
        return elementType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.GetIndexParameters().Length == 0)
            .ToList();
    }

    /// <summary>
    /// Checks if a type is a simple type that can be mapped to a ClickHouse column.
    /// </summary>
    private static bool IsMappableSimpleType(Type type)
    {
        // Primitive types
        if (type.IsPrimitive)
        {
            return true;
        }

        // Common value types and strings
        if (type == typeof(string) ||
            type == typeof(decimal) ||
            type == typeof(Guid) ||
            type == typeof(DateTime) ||
            type == typeof(DateTimeOffset) ||
            type == typeof(DateOnly) ||
            type == typeof(TimeOnly) ||
            type == typeof(TimeSpan) ||
            type.IsEnum)
        {
            return true;
        }

        // Large integers
        if (type == typeof(Int128) ||
            type == typeof(UInt128) ||
            type == typeof(System.Numerics.BigInteger))
        {
            return true;
        }

        return false;
    }
}
