using System.Collections;
using System.Text;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Type mapping for ClickHouse Array(T) type.
/// Maps to .NET T[] or List&lt;T&gt; types.
/// </summary>
/// <remarks>
/// ClickHouse arrays are 1-indexed and use the syntax: Array(ElementType).
/// Literal format: [element1, element2, ...]
/// </remarks>
public class ClickHouseArrayTypeMapping : RelationalTypeMapping
{
    /// <summary>
    /// The type mapping for the array element type.
    /// </summary>
    public RelationalTypeMapping ElementMapping { get; }

    public ClickHouseArrayTypeMapping(
        Type clrType,
        RelationalTypeMapping elementMapping)
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(clrType),
            $"Array({elementMapping.StoreType})",
            StoreTypePostfix.None,
            System.Data.DbType.Object))
    {
        ElementMapping = elementMapping;
    }

    protected ClickHouseArrayTypeMapping(
        RelationalTypeMappingParameters parameters,
        RelationalTypeMapping elementMapping)
        : base(parameters)
    {
        ElementMapping = elementMapping;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseArrayTypeMapping(parameters, ElementMapping);

    /// <summary>
    /// Generates a SQL literal for an array value.
    /// Format: [element1, element2, ...]
    /// </summary>
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var builder = new StringBuilder();
        builder.Append('[');

        var first = true;
        foreach (var element in (IEnumerable)value)
        {
            if (!first)
            {
                builder.Append(", ");
            }
            first = false;

            if (element is null)
            {
                builder.Append("NULL");
            }
            else
            {
                builder.Append(ElementMapping.GenerateSqlLiteral(element));
            }
        }

        builder.Append(']');
        return builder.ToString();
    }
}
