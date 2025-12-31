using System.Collections.Concurrent;
using System.Net;
using System.Numerics;
using System.Text.Json;
using System.Text.RegularExpressions;
using EF.CH.Metadata;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Storage.Internal;

/// <summary>
/// Type mapping source for ClickHouse.
/// Maps CLR types to ClickHouse store types and vice versa.
/// </summary>
public partial class ClickHouseTypeMappingSource : RelationalTypeMappingSource
{
    /// <summary>
    /// Direct mappings from CLR types to ClickHouse type mappings.
    /// </summary>
    private static readonly Dictionary<Type, RelationalTypeMapping> ClrTypeMappings = new()
    {
        // Signed integers
        [typeof(sbyte)] = new ClickHouseInt8TypeMapping(),
        [typeof(short)] = new ClickHouseInt16TypeMapping(),
        [typeof(int)] = new ClickHouseInt32TypeMapping(),
        [typeof(long)] = new ClickHouseInt64TypeMapping(),
        [typeof(Int128)] = new ClickHouseInt128TypeMapping(),

        // Unsigned integers
        [typeof(byte)] = new ClickHouseUInt8TypeMapping(),
        [typeof(ushort)] = new ClickHouseUInt16TypeMapping(),
        [typeof(uint)] = new ClickHouseUInt32TypeMapping(),
        [typeof(ulong)] = new ClickHouseUInt64TypeMapping(),
        [typeof(UInt128)] = new ClickHouseUInt128TypeMapping(),

        // Large integers (BigInteger maps to Int256 by default)
        [typeof(BigInteger)] = new ClickHouseInt256TypeMapping(),

        // Floating point
        [typeof(float)] = new ClickHouseFloat32TypeMapping(),
        [typeof(double)] = new ClickHouseFloat64TypeMapping(),

        // Decimal (default precision)
        [typeof(decimal)] = new ClickHouseDecimalTypeMapping(18, 4),

        // String
        [typeof(string)] = new ClickHouseStringTypeMapping(),

        // Boolean
        [typeof(bool)] = new ClickHouseBoolTypeMapping(),

        // GUID
        [typeof(Guid)] = new ClickHouseGuidTypeMapping(),

        // DateTime types
        [typeof(DateTime)] = new ClickHouseDateTimeTypeMapping(3),
        [typeof(DateTimeOffset)] = new ClickHouseDateTimeOffsetTypeMapping(3),
        [typeof(DateOnly)] = new ClickHouseDateTypeMapping(),
        [typeof(TimeOnly)] = new ClickHouseTimeTypeMapping(),
        [typeof(TimeSpan)] = new ClickHouseTimeSpanTypeMapping(),

        // JSON (native JSON type, requires ClickHouse 24.8+)
        [typeof(JsonElement)] = new ClickHouseJsonTypeMapping(typeof(JsonElement)),
        [typeof(JsonDocument)] = new ClickHouseJsonTypeMapping(typeof(JsonDocument)),

        // IP Address types
        [typeof(ClickHouseIPv4)] = new ClickHouseIPv4TypeMapping(),
        [typeof(ClickHouseIPv6)] = new ClickHouseIPv6TypeMapping(),
        [typeof(IPAddress)] = new ClickHouseIPAddressTypeMapping(),
    };

    /// <summary>
    /// Mappings from ClickHouse store type names (case-insensitive) to type mappings.
    /// Includes aliases for MySQL compatibility.
    /// </summary>
    private static readonly Dictionary<string, RelationalTypeMapping> StoreTypeMappings =
        new(StringComparer.OrdinalIgnoreCase)
        {
            // Signed integers
            ["Int8"] = new ClickHouseInt8TypeMapping(),
            ["Int16"] = new ClickHouseInt16TypeMapping(),
            ["Int32"] = new ClickHouseInt32TypeMapping(),
            ["Int64"] = new ClickHouseInt64TypeMapping(),
            ["Int128"] = new ClickHouseInt128TypeMapping(),
            ["Int256"] = new ClickHouseInt256TypeMapping(),

            // Unsigned integers
            ["UInt8"] = new ClickHouseUInt8TypeMapping(),
            ["UInt16"] = new ClickHouseUInt16TypeMapping(),
            ["UInt32"] = new ClickHouseUInt32TypeMapping(),
            ["UInt64"] = new ClickHouseUInt64TypeMapping(),
            ["UInt128"] = new ClickHouseUInt128TypeMapping(),
            ["UInt256"] = new ClickHouseUInt256TypeMapping(),

            // MySQL-style aliases
            ["TINYINT"] = new ClickHouseInt8TypeMapping(),
            ["SMALLINT"] = new ClickHouseInt16TypeMapping(),
            ["INT"] = new ClickHouseInt32TypeMapping(),
            ["INTEGER"] = new ClickHouseInt32TypeMapping(),
            ["BIGINT"] = new ClickHouseInt64TypeMapping(),

            // Floating point
            ["Float32"] = new ClickHouseFloat32TypeMapping(),
            ["Float64"] = new ClickHouseFloat64TypeMapping(),
            ["FLOAT"] = new ClickHouseFloat32TypeMapping(),
            ["DOUBLE"] = new ClickHouseFloat64TypeMapping(),

            // String types
            ["String"] = new ClickHouseStringTypeMapping(),
            ["VARCHAR"] = new ClickHouseStringTypeMapping(),
            ["TEXT"] = new ClickHouseStringTypeMapping(),
            ["CHAR"] = new ClickHouseStringTypeMapping(),

            // Boolean
            ["Bool"] = new ClickHouseBoolTypeMapping(),
            ["Boolean"] = new ClickHouseBoolTypeMapping(),

            // UUID
            ["UUID"] = new ClickHouseGuidTypeMapping(),

            // Date/Time
            ["Date"] = new ClickHouseDateTypeMapping(),
            ["Date32"] = new ClickHouseDate32TypeMapping(),
            ["Time"] = new ClickHouseTimeTypeMapping(),

            // JSON (native JSON type, requires ClickHouse 24.8+)
            ["JSON"] = new ClickHouseJsonTypeMapping(typeof(JsonElement)),

            // IP Address types
            ["IPv4"] = new ClickHouseIPv4TypeMapping(),
            ["IPv6"] = new ClickHouseIPv6TypeMapping(),
        };

    /// <summary>
    /// Cache for dynamically created type mappings.
    /// </summary>
    private readonly ConcurrentDictionary<string, RelationalTypeMapping?> _storeTypeMappingCache = new();

    public ClickHouseTypeMappingSource(TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    /// <summary>
    /// Finds a type mapping for the given CLR type.
    /// </summary>
    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var clrType = mappingInfo.ClrType;
        var storeTypeName = mappingInfo.StoreTypeName;

        // If we have a store type name, try to parse and match it
        if (storeTypeName is not null)
        {
            var mapping = FindMappingByStoreType(storeTypeName, clrType);
            if (mapping is not null)
            {
                return mapping;
            }
        }

        // If we have a CLR type, use the direct mapping
        if (clrType is not null)
        {
            // Handle nullable types by unwrapping
            var underlyingType = Nullable.GetUnderlyingType(clrType) ?? clrType;

            if (ClrTypeMappings.TryGetValue(underlyingType, out var mapping))
            {
                // For nullable types, wrap in Nullable() in ClickHouse
                // Note: Properties with sentinel defaults will have a value converter that
                // converts null to the sentinel value. When a value converter is present,
                // EF Core uses the provider type (non-nullable) for the store type.
                if (Nullable.GetUnderlyingType(clrType) is not null)
                {
                    return mapping.WithStoreTypeAndSize($"Nullable({mapping.StoreType})", null);
                }
                return mapping;
            }

            // Handle enums - map to ClickHouse Enum8/Enum16
            if (underlyingType.IsEnum)
            {
                return new ClickHouseEnumTypeMapping(underlyingType);
            }

            // Handle nested types (List<T> where T is a complex type) - check before Array
            var nestedMapping = FindNestedMapping(underlyingType);
            if (nestedMapping is not null)
            {
                return nestedMapping;
            }

            // Handle array types (T[] or List<T> where T is a primitive)
            var arrayMapping = FindArrayMapping(underlyingType);
            if (arrayMapping is not null)
            {
                return arrayMapping;
            }

            // Handle dictionary types (Dictionary<K,V>)
            var mapMapping = FindMapMapping(underlyingType);
            if (mapMapping is not null)
            {
                return mapMapping;
            }

            // Handle tuple types (ValueTuple and Tuple)
            var tupleMapping = FindTupleMapping(underlyingType);
            if (tupleMapping is not null)
            {
                return tupleMapping;
            }
        }

        return base.FindMapping(mappingInfo);
    }

    /// <summary>
    /// Finds a type mapping by parsing the ClickHouse store type name.
    /// </summary>
    /// <param name="storeTypeName">The ClickHouse store type name (e.g., "Nullable(String)", "Array(Int32)")</param>
    /// <param name="clrType">Optional CLR type hint</param>
    /// <returns>The type mapping, or null if not found</returns>
    internal RelationalTypeMapping? FindMappingByStoreType(string storeTypeName, Type? clrType = null)
    {
        // Check cache first
        if (_storeTypeMappingCache.TryGetValue(storeTypeName, out var cached))
        {
            return cached;
        }

        var mapping = ParseStoreType(storeTypeName, clrType);
        _storeTypeMappingCache.TryAdd(storeTypeName, mapping);
        return mapping;
    }

    /// <summary>
    /// Parses a ClickHouse store type name and returns the appropriate mapping.
    /// </summary>
    private RelationalTypeMapping? ParseStoreType(string storeTypeName, Type? clrType)
    {
        // Trim whitespace
        storeTypeName = storeTypeName.Trim();

        // Check simple types first
        if (StoreTypeMappings.TryGetValue(storeTypeName, out var simpleMapping))
        {
            return simpleMapping;
        }

        // Handle Nullable(T)
        var nullableMatch = NullableRegex().Match(storeTypeName);
        if (nullableMatch.Success)
        {
            var innerType = nullableMatch.Groups[1].Value;
            var innerMapping = ParseStoreType(innerType, clrType);
            if (innerMapping is not null)
            {
                return innerMapping.WithStoreTypeAndSize($"Nullable({innerMapping.StoreType})", null);
            }
        }

        // Handle DateTime64(precision) or DateTime64(precision, timezone)
        var dateTime64Match = DateTime64Regex().Match(storeTypeName);
        if (dateTime64Match.Success)
        {
            var precision = int.Parse(dateTime64Match.Groups[1].Value);
            var timezone = dateTime64Match.Groups.Count > 2 && dateTime64Match.Groups[2].Success
                ? dateTime64Match.Groups[2].Value.Trim('\'', '"')
                : null;

            // Return appropriate mapping based on CLR type
            if (clrType == typeof(DateTimeOffset) || clrType == typeof(DateTimeOffset?))
            {
                return new ClickHouseDateTimeOffsetTypeMapping(precision, timezone);
            }

            return new ClickHouseDateTimeTypeMapping(precision, timezone);
        }

        // Handle Decimal(P, S)
        var decimalMatch = DecimalRegex().Match(storeTypeName);
        if (decimalMatch.Success)
        {
            var precision = int.Parse(decimalMatch.Groups[1].Value);
            var scale = int.Parse(decimalMatch.Groups[2].Value);
            return new ClickHouseDecimalTypeMapping(precision, scale);
        }

        // Handle Decimal32(S), Decimal64(S), Decimal128(S)
        var decimal32Match = Decimal32Regex().Match(storeTypeName);
        if (decimal32Match.Success)
        {
            var scale = int.Parse(decimal32Match.Groups[1].Value);
            return new ClickHouseDecimal32TypeMapping(scale);
        }

        var decimal64Match = Decimal64Regex().Match(storeTypeName);
        if (decimal64Match.Success)
        {
            var scale = int.Parse(decimal64Match.Groups[1].Value);
            return new ClickHouseDecimal64TypeMapping(scale);
        }

        var decimal128Match = Decimal128Regex().Match(storeTypeName);
        if (decimal128Match.Success)
        {
            var scale = int.Parse(decimal128Match.Groups[1].Value);
            return new ClickHouseDecimal128TypeMapping(scale);
        }

        // Handle FixedString(N)
        var fixedStringMatch = FixedStringRegex().Match(storeTypeName);
        if (fixedStringMatch.Success)
        {
            var length = int.Parse(fixedStringMatch.Groups[1].Value);
            return new ClickHouseFixedStringTypeMapping(length);
        }

        // Handle LowCardinality(T)
        var lowCardinalityMatch = LowCardinalityRegex().Match(storeTypeName);
        if (lowCardinalityMatch.Success)
        {
            var innerType = lowCardinalityMatch.Groups[1].Value;
            var innerMapping = ParseStoreType(innerType, clrType);
            if (innerMapping is not null)
            {
                // LowCardinality doesn't change the CLR type, just the storage
                return innerMapping.WithStoreTypeAndSize($"LowCardinality({innerMapping.StoreType})", null);
            }
        }

        // Handle JSON or JSON(max_dynamic_paths=X, max_dynamic_types=Y)
        var jsonMatch = JsonRegex().Match(storeTypeName);
        if (jsonMatch.Success)
        {
            int? maxDynamicPaths = null;
            int? maxDynamicTypes = null;

            // Parse parameters if present
            if (jsonMatch.Groups[1].Success && !string.IsNullOrWhiteSpace(jsonMatch.Groups[1].Value))
            {
                var paramsString = jsonMatch.Groups[1].Value;
                var (paths, types) = ParseJsonParameters(paramsString);
                maxDynamicPaths = paths;
                maxDynamicTypes = types;
            }

            var jsonClrType = clrType ?? typeof(JsonElement);
            return new ClickHouseJsonTypeMapping(jsonClrType, maxDynamicPaths, maxDynamicTypes);
        }

        // Handle Array(T)
        var arrayMatch = ArrayRegex().Match(storeTypeName);
        if (arrayMatch.Success)
        {
            var elementType = arrayMatch.Groups[1].Value;
            var elementMapping = ParseStoreType(elementType, null);
            if (elementMapping is not null)
            {
                // Default to T[] for array CLR type
                var arrayClrType = clrType ?? elementMapping.ClrType.MakeArrayType();
                return new ClickHouseArrayTypeMapping(arrayClrType, elementMapping);
            }
        }

        // Handle Map(K, V)
        var mapMatch = MapRegex().Match(storeTypeName);
        if (mapMatch.Success)
        {
            var keyType = mapMatch.Groups[1].Value.Trim();
            var valueType = mapMatch.Groups[2].Value.Trim();
            var keyMapping = ParseStoreType(keyType, null);
            var valueMapping = ParseStoreType(valueType, null);
            if (keyMapping is not null && valueMapping is not null)
            {
                // Default to Dictionary<K, V> for map CLR type
                var mapClrType = clrType ?? typeof(Dictionary<,>).MakeGenericType(keyMapping.ClrType, valueMapping.ClrType);
                return new ClickHouseMapTypeMapping(mapClrType, keyMapping, valueMapping);
            }
        }

        // Handle Tuple(T1, T2, ...)
        var tupleMatch = TupleRegex().Match(storeTypeName);
        if (tupleMatch.Success)
        {
            var innerContent = tupleMatch.Groups[1].Value;
            var elementTypes = ParseTupleElements(innerContent);
            var elementMappings = new List<RelationalTypeMapping>();

            foreach (var elementType in elementTypes)
            {
                var elementMapping = ParseStoreType(elementType.Trim(), null);
                if (elementMapping is null)
                {
                    return null;
                }
                elementMappings.Add(elementMapping);
            }

            if (elementMappings.Count > 0)
            {
                // Build a ValueTuple CLR type for the mapping
                var tupleClrType = clrType ?? MakeValueTupleType(elementMappings.Select(m => m.ClrType).ToArray());
                if (tupleClrType is not null)
                {
                    return new ClickHouseTupleTypeMapping(tupleClrType, elementMappings);
                }
            }
        }

        // Handle AggregateFunction(func, T) - binary state columns
        var aggMatch = AggregateFunctionRegex().Match(storeTypeName);
        if (aggMatch.Success)
        {
            var functionName = aggMatch.Groups[1].Value;
            var argumentType = aggMatch.Groups[2].Value.Trim();
            var argumentMapping = ParseStoreType(argumentType, null);
            if (argumentMapping is not null)
            {
                return new ClickHouseAggregateFunctionTypeMapping(functionName, argumentMapping);
            }
        }

        // Handle SimpleAggregateFunction(func, T)
        var simpleAggMatch = SimpleAggregateFunctionRegex().Match(storeTypeName);
        if (simpleAggMatch.Success)
        {
            var functionName = simpleAggMatch.Groups[1].Value;
            var argumentType = simpleAggMatch.Groups[2].Value.Trim();
            var argumentMapping = ParseStoreType(argumentType, null);
            if (argumentMapping is not null)
            {
                return new ClickHouseSimpleAggregateFunctionTypeMapping(functionName, argumentMapping);
            }
        }

        // Handle Nested(field1 Type1, field2 Type2, ...)
        var nestedMatch = NestedRegex().Match(storeTypeName);
        if (nestedMatch.Success)
        {
            var innerContent = nestedMatch.Groups[1].Value;
            var fieldMappings = ParseNestedFields(innerContent);
            if (fieldMappings is not null && fieldMappings.Count > 0)
            {
                // For scaffolding, we use List<object> as a placeholder CLR type.
                // The scaffolding factory will store the field definitions as an annotation
                // so code generation can produce XML docs with the TODO and record class definition.
                return new ClickHouseTypeMapping(storeTypeName, typeof(List<object>));
            }
        }

        return null;
    }

    /// <summary>
    /// Parses Nested field definitions from a comma-separated string.
    /// Format: "field1 Type1, field2 Type2, ..."
    /// </summary>
    internal static List<(string Name, string TypeName)>? ParseNestedFields(string content)
    {
        var fields = new List<(string Name, string TypeName)>();
        var elements = ParseTupleElements(content);

        foreach (var element in elements)
        {
            var trimmed = element.Trim();
            // Find the first space to separate name from type
            var spaceIndex = trimmed.IndexOf(' ');
            if (spaceIndex <= 0)
            {
                return null;
            }

            var name = trimmed.Substring(0, spaceIndex).Trim().Trim('"');
            var typeName = trimmed.Substring(spaceIndex + 1).Trim();
            fields.Add((name, typeName));
        }

        return fields;
    }

    /// <summary>
    /// Parses JSON type parameters from a comma-separated string.
    /// Format: "max_dynamic_paths=1024, max_dynamic_types=32"
    /// </summary>
    private static (int? MaxDynamicPaths, int? MaxDynamicTypes) ParseJsonParameters(string paramsString)
    {
        int? maxDynamicPaths = null;
        int? maxDynamicTypes = null;

        var parts = paramsString.Split(',', StringSplitOptions.TrimEntries);
        foreach (var part in parts)
        {
            var keyValue = part.Split('=', 2, StringSplitOptions.TrimEntries);
            if (keyValue.Length != 2) continue;

            var key = keyValue[0].ToLowerInvariant();
            if (int.TryParse(keyValue[1], out var value))
            {
                switch (key)
                {
                    case "max_dynamic_paths":
                        maxDynamicPaths = value;
                        break;
                    case "max_dynamic_types":
                        maxDynamicTypes = value;
                        break;
                }
            }
        }

        return (maxDynamicPaths, maxDynamicTypes);
    }

    /// <summary>
    /// Parses tuple element types from a comma-separated string, handling nested types.
    /// </summary>
    private static List<string> ParseTupleElements(string content)
    {
        var elements = new List<string>();
        var current = new System.Text.StringBuilder();
        var depth = 0;

        foreach (var ch in content)
        {
            if (ch == '(' || ch == '<')
            {
                depth++;
                current.Append(ch);
            }
            else if (ch == ')' || ch == '>')
            {
                depth--;
                current.Append(ch);
            }
            else if (ch == ',' && depth == 0)
            {
                elements.Add(current.ToString().Trim());
                current.Clear();
            }
            else
            {
                current.Append(ch);
            }
        }

        if (current.Length > 0)
        {
            elements.Add(current.ToString().Trim());
        }

        return elements;
    }

    /// <summary>
    /// Creates a ValueTuple type for the given element types.
    /// </summary>
    private static Type? MakeValueTupleType(Type[] elementTypes)
    {
        return elementTypes.Length switch
        {
            1 => typeof(ValueTuple<>).MakeGenericType(elementTypes),
            2 => typeof(ValueTuple<,>).MakeGenericType(elementTypes),
            3 => typeof(ValueTuple<,,>).MakeGenericType(elementTypes),
            4 => typeof(ValueTuple<,,,>).MakeGenericType(elementTypes),
            5 => typeof(ValueTuple<,,,,>).MakeGenericType(elementTypes),
            6 => typeof(ValueTuple<,,,,,>).MakeGenericType(elementTypes),
            7 => typeof(ValueTuple<,,,,,,>).MakeGenericType(elementTypes),
            _ when elementTypes.Length > 7 =>
                // For > 7 elements, nest remaining in a Rest tuple
                typeof(ValueTuple<,,,,,,,>).MakeGenericType(
                    elementTypes[0], elementTypes[1], elementTypes[2], elementTypes[3],
                    elementTypes[4], elementTypes[5], elementTypes[6],
                    MakeValueTupleType(elementTypes.Skip(7).ToArray())!),
            _ => null
        };
    }

    /// <summary>
    /// Gets the relational type mapping for a given CLR type.
    /// </summary>
    public RelationalTypeMapping? GetMapping(Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

        if (ClrTypeMappings.TryGetValue(underlyingType, out var mapping))
        {
            if (Nullable.GetUnderlyingType(type) is not null)
            {
                return mapping.WithStoreTypeAndSize($"Nullable({mapping.StoreType})", null);
            }
            return mapping;
        }

        return null;
    }

    /// <summary>
    /// Finds a type mapping for array types (T[] or List&lt;T&gt;).
    /// </summary>
    private RelationalTypeMapping? FindArrayMapping(Type clrType)
    {
        Type? elementType = null;

        if (clrType.IsArray)
        {
            elementType = clrType.GetElementType();
        }
        else if (clrType.IsGenericType)
        {
            var genericDef = clrType.GetGenericTypeDefinition();
            if (genericDef == typeof(List<>) ||
                genericDef == typeof(IList<>) ||
                genericDef == typeof(ICollection<>) ||
                genericDef == typeof(IEnumerable<>) ||
                genericDef == typeof(IReadOnlyList<>) ||
                genericDef == typeof(IReadOnlyCollection<>))
            {
                elementType = clrType.GetGenericArguments()[0];
            }
        }

        if (elementType is null)
        {
            return null;
        }

        // Get the element type mapping
        var elementMapping = FindMapping(new RelationalTypeMappingInfo(elementType));
        if (elementMapping is null)
        {
            return null;
        }

        return new ClickHouseArrayTypeMapping(clrType, elementMapping);
    }

    /// <summary>
    /// Finds a type mapping for dictionary types (Dictionary&lt;K,V&gt;).
    /// </summary>
    private RelationalTypeMapping? FindMapMapping(Type clrType)
    {
        if (!clrType.IsGenericType)
        {
            return null;
        }

        var genericDef = clrType.GetGenericTypeDefinition();
        if (genericDef != typeof(Dictionary<,>) &&
            genericDef != typeof(IDictionary<,>) &&
            genericDef != typeof(IReadOnlyDictionary<,>))
        {
            return null;
        }

        var typeArgs = clrType.GetGenericArguments();
        var keyType = typeArgs[0];
        var valueType = typeArgs[1];

        // Get key type mapping
        var keyMapping = FindMapping(new RelationalTypeMappingInfo(keyType));
        if (keyMapping is null)
        {
            return null;
        }

        // Get value type mapping
        var valueMapping = FindMapping(new RelationalTypeMappingInfo(valueType));
        if (valueMapping is null)
        {
            return null;
        }

        return new ClickHouseMapTypeMapping(clrType, keyMapping, valueMapping);
    }

    /// <summary>
    /// Finds a type mapping for tuple types (ValueTuple and Tuple).
    /// </summary>
    private RelationalTypeMapping? FindTupleMapping(Type clrType)
    {
        if (!clrType.IsGenericType)
        {
            return null;
        }

        var genericDef = clrType.GetGenericTypeDefinition();
        if (!IsValueTupleType(genericDef) && !IsTupleType(genericDef))
        {
            return null;
        }

        var typeArgs = clrType.GetGenericArguments();
        var elementMappings = new List<RelationalTypeMapping>();

        foreach (var typeArg in typeArgs)
        {
            // Handle nested tuples (for > 7 elements, the 8th is a "Rest" tuple)
            if (IsValueTupleType(typeArg.IsGenericType ? typeArg.GetGenericTypeDefinition() : null) ||
                IsTupleType(typeArg.IsGenericType ? typeArg.GetGenericTypeDefinition() : null))
            {
                var nestedMapping = FindTupleMapping(typeArg);
                if (nestedMapping is ClickHouseTupleTypeMapping nestedTuple)
                {
                    // Flatten nested tuple elements
                    elementMappings.AddRange(nestedTuple.ElementMappings);
                    continue;
                }
            }

            var elementMapping = FindMapping(new RelationalTypeMappingInfo(typeArg));
            if (elementMapping is null)
            {
                return null;
            }

            elementMappings.Add(elementMapping);
        }

        return new ClickHouseTupleTypeMapping(clrType, elementMappings);
    }

    private static bool IsValueTupleType(Type? genericDef)
    {
        return genericDef == typeof(ValueTuple<>) ||
               genericDef == typeof(ValueTuple<,>) ||
               genericDef == typeof(ValueTuple<,,>) ||
               genericDef == typeof(ValueTuple<,,,>) ||
               genericDef == typeof(ValueTuple<,,,,>) ||
               genericDef == typeof(ValueTuple<,,,,,>) ||
               genericDef == typeof(ValueTuple<,,,,,,>) ||
               genericDef == typeof(ValueTuple<,,,,,,,>);
    }

    private static bool IsTupleType(Type? genericDef)
    {
        return genericDef == typeof(Tuple<>) ||
               genericDef == typeof(Tuple<,>) ||
               genericDef == typeof(Tuple<,,>) ||
               genericDef == typeof(Tuple<,,,>) ||
               genericDef == typeof(Tuple<,,,,>) ||
               genericDef == typeof(Tuple<,,,,,>) ||
               genericDef == typeof(Tuple<,,,,,,>) ||
               genericDef == typeof(Tuple<,,,,,,,>);
    }

    /// <summary>
    /// Finds a type mapping for nested types (List&lt;T&gt; where T is a complex type).
    /// </summary>
    private RelationalTypeMapping? FindNestedMapping(Type clrType)
    {
        Type? elementType = null;

        // Check if it's a List<T> or T[]
        if (clrType.IsArray)
        {
            elementType = clrType.GetElementType();
        }
        else if (clrType.IsGenericType)
        {
            var genericDef = clrType.GetGenericTypeDefinition();
            if (genericDef == typeof(List<>) ||
                genericDef == typeof(IList<>) ||
                genericDef == typeof(ICollection<>) ||
                genericDef == typeof(IReadOnlyList<>) ||
                genericDef == typeof(IReadOnlyCollection<>))
            {
                elementType = clrType.GetGenericArguments()[0];
            }
        }

        if (elementType is null)
        {
            return null;
        }

        // Check if the element type is suitable for Nested
        if (!ClickHouseNestedTypeMapping.IsSuitableForNested(elementType))
        {
            return null;
        }

        // Build field mappings from the element type's properties
        var properties = ClickHouseNestedTypeMapping.GetMappableProperties(elementType);
        var fieldMappings = new List<(string Name, System.Reflection.PropertyInfo Property, RelationalTypeMapping Mapping)>();

        foreach (var prop in properties)
        {
            var propType = prop.PropertyType;
            var propMapping = FindMapping(new RelationalTypeMappingInfo(propType));
            if (propMapping is null)
            {
                return null;
            }

            fieldMappings.Add((prop.Name, prop, propMapping));
        }

        return new ClickHouseNestedTypeMapping(clrType, elementType, fieldMappings);
    }

    // Regex patterns for parsing ClickHouse type names
    [GeneratedRegex(@"^Nullable\((.+)\)$", RegexOptions.IgnoreCase)]
    private static partial Regex NullableRegex();

    [GeneratedRegex(@"^DateTime64\((\d+)(?:\s*,\s*'?([^']+)'?)?\)$", RegexOptions.IgnoreCase)]
    private static partial Regex DateTime64Regex();

    [GeneratedRegex(@"^Decimal\((\d+)\s*,\s*(\d+)\)$", RegexOptions.IgnoreCase)]
    private static partial Regex DecimalRegex();

    [GeneratedRegex(@"^Decimal32\((\d+)\)$", RegexOptions.IgnoreCase)]
    private static partial Regex Decimal32Regex();

    [GeneratedRegex(@"^Decimal64\((\d+)\)$", RegexOptions.IgnoreCase)]
    private static partial Regex Decimal64Regex();

    [GeneratedRegex(@"^Decimal128\((\d+)\)$", RegexOptions.IgnoreCase)]
    private static partial Regex Decimal128Regex();

    [GeneratedRegex(@"^FixedString\((\d+)\)$", RegexOptions.IgnoreCase)]
    private static partial Regex FixedStringRegex();

    [GeneratedRegex(@"^LowCardinality\((.+)\)$", RegexOptions.IgnoreCase)]
    private static partial Regex LowCardinalityRegex();

    [GeneratedRegex(@"^Array\((.+)\)$", RegexOptions.IgnoreCase)]
    private static partial Regex ArrayRegex();

    [GeneratedRegex(@"^Enum8\(.+\)$", RegexOptions.IgnoreCase)]
    private static partial Regex Enum8Regex();

    [GeneratedRegex(@"^Enum16\(.+\)$", RegexOptions.IgnoreCase)]
    private static partial Regex Enum16Regex();

    [GeneratedRegex(@"^Map\((.+),\s*(.+)\)$", RegexOptions.IgnoreCase)]
    private static partial Regex MapRegex();

    [GeneratedRegex(@"^Tuple\((.+)\)$", RegexOptions.IgnoreCase)]
    private static partial Regex TupleRegex();

    [GeneratedRegex(@"^SimpleAggregateFunction\((\w+),\s*(.+)\)$", RegexOptions.IgnoreCase)]
    private static partial Regex SimpleAggregateFunctionRegex();

    [GeneratedRegex(@"^AggregateFunction\((\w+),\s*(.+)\)$", RegexOptions.IgnoreCase)]
    private static partial Regex AggregateFunctionRegex();

    [GeneratedRegex(@"^Nested\((.+)\)$", RegexOptions.IgnoreCase)]
    private static partial Regex NestedRegex();

    // JSON type: JSON or JSON(max_dynamic_paths=X, max_dynamic_types=Y)
    [GeneratedRegex(@"^JSON(?:\((.+)\))?$", RegexOptions.IgnoreCase)]
    private static partial Regex JsonRegex();
}
