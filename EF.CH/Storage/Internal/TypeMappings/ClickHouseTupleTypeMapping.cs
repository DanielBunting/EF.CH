using System.Text;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Type mapping for ClickHouse Tuple types.
/// Maps .NET tuples (ValueTuple and Tuple) to ClickHouse Tuple(T1, T2, ...).
/// </summary>
/// <remarks>
/// ClickHouse tuples can be named or unnamed:
/// - Tuple(Int32, String) - unnamed
/// - Tuple(id Int32, name String) - named
///
/// This mapping supports:
/// - ValueTuple&lt;T1, T2, ...&gt; (C# value tuples)
/// - Tuple&lt;T1, T2, ...&gt; (C# reference tuples)
/// - Named tuples via element names
/// </remarks>
public class ClickHouseTupleTypeMapping : RelationalTypeMapping
{
    /// <summary>
    /// The type mappings for each element in the tuple.
    /// </summary>
    public IReadOnlyList<RelationalTypeMapping> ElementMappings { get; }

    /// <summary>
    /// Optional element names for named tuples.
    /// </summary>
    public IReadOnlyList<string>? ElementNames { get; }

    public ClickHouseTupleTypeMapping(
        Type clrType,
        IReadOnlyList<RelationalTypeMapping> elementMappings,
        IReadOnlyList<string>? elementNames = null)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(clrType),
                BuildStoreType(elementMappings, elementNames)))
    {
        ElementMappings = elementMappings;
        ElementNames = elementNames;
    }

    protected ClickHouseTupleTypeMapping(
        RelationalTypeMappingParameters parameters,
        IReadOnlyList<RelationalTypeMapping> elementMappings,
        IReadOnlyList<string>? elementNames)
        : base(parameters)
    {
        ElementMappings = elementMappings;
        ElementNames = elementNames;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseTupleTypeMapping(parameters, ElementMappings, ElementNames);

    /// <summary>
    /// Builds the ClickHouse store type string for the tuple.
    /// </summary>
    private static string BuildStoreType(
        IReadOnlyList<RelationalTypeMapping> elementMappings,
        IReadOnlyList<string>? elementNames)
    {
        var sb = new StringBuilder("Tuple(");

        for (var i = 0; i < elementMappings.Count; i++)
        {
            if (i > 0)
            {
                sb.Append(", ");
            }

            if (elementNames is not null && i < elementNames.Count && !string.IsNullOrEmpty(elementNames[i]))
            {
                // Named element: "name Type"
                sb.Append(elementNames[i]);
                sb.Append(' ');
            }

            sb.Append(elementMappings[i].StoreType);
        }

        sb.Append(')');
        return sb.ToString();
    }

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var sb = new StringBuilder("(");

        var values = GetTupleValues(value);

        for (var i = 0; i < values.Count && i < ElementMappings.Count; i++)
        {
            if (i > 0)
            {
                sb.Append(", ");
            }

            var elementValue = values[i];
            if (elementValue is null)
            {
                sb.Append("NULL");
            }
            else
            {
                sb.Append(ElementMappings[i].GenerateSqlLiteral(elementValue));
            }
        }

        sb.Append(')');
        return sb.ToString();
    }

    /// <summary>
    /// Extracts values from a tuple object.
    /// </summary>
    private static IReadOnlyList<object?> GetTupleValues(object tuple)
    {
        var type = tuple.GetType();
        var values = new List<object?>();

        if (IsValueTuple(type))
        {
            // ValueTuple uses Item1, Item2, etc. fields
            var fields = type.GetFields()
                .Where(f => f.Name.StartsWith("Item"))
                .OrderBy(f => f.Name)
                .ToList();

            foreach (var field in fields)
            {
                values.Add(field.GetValue(tuple));
            }

            // Handle nested ValueTuples (for tuples with > 7 elements)
            var restField = type.GetField("Rest");
            if (restField is not null)
            {
                var rest = restField.GetValue(tuple);
                if (rest is not null)
                {
                    values.AddRange(GetTupleValues(rest));
                }
            }
        }
        else if (IsTuple(type))
        {
            // Tuple<> uses Item1, Item2, etc. properties
            var props = type.GetProperties()
                .Where(p => p.Name.StartsWith("Item"))
                .OrderBy(p => p.Name)
                .ToList();

            foreach (var prop in props)
            {
                values.Add(prop.GetValue(tuple));
            }

            // Handle nested Tuples (for tuples with > 7 elements)
            var restProp = type.GetProperty("Rest");
            if (restProp is not null)
            {
                var rest = restProp.GetValue(tuple);
                if (rest is not null)
                {
                    values.AddRange(GetTupleValues(rest));
                }
            }
        }

        return values;
    }

    private static bool IsValueTuple(Type type)
    {
        if (!type.IsGenericType)
        {
            return false;
        }

        var genericDef = type.GetGenericTypeDefinition();
        return genericDef == typeof(ValueTuple<>) ||
               genericDef == typeof(ValueTuple<,>) ||
               genericDef == typeof(ValueTuple<,,>) ||
               genericDef == typeof(ValueTuple<,,,>) ||
               genericDef == typeof(ValueTuple<,,,,>) ||
               genericDef == typeof(ValueTuple<,,,,,>) ||
               genericDef == typeof(ValueTuple<,,,,,,>) ||
               genericDef == typeof(ValueTuple<,,,,,,,>);
    }

    private static bool IsTuple(Type type)
    {
        if (!type.IsGenericType)
        {
            return false;
        }

        var genericDef = type.GetGenericTypeDefinition();
        return genericDef == typeof(Tuple<>) ||
               genericDef == typeof(Tuple<,>) ||
               genericDef == typeof(Tuple<,,>) ||
               genericDef == typeof(Tuple<,,,>) ||
               genericDef == typeof(Tuple<,,,,>) ||
               genericDef == typeof(Tuple<,,,,,>) ||
               genericDef == typeof(Tuple<,,,,,,>) ||
               genericDef == typeof(Tuple<,,,,,,,>);
    }
}
