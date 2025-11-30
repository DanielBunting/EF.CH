using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Type mapping for ClickHouse SimpleAggregateFunction types.
/// </summary>
/// <remarks>
/// SimpleAggregateFunction stores the actual result value (not an opaque state),
/// so the CLR type is the same as the underlying type. This is ideal for simple
/// aggregates like max, min, sum, any, anyLast where state equals final result.
///
/// Usage with AggregatingMergeTree:
/// - On INSERT: Values are stored directly (or via aggregate function)
/// - On MERGE: ClickHouse applies the aggregate function to combine values
/// - On SELECT: Values can be read directly (no -Merge combinator needed)
///
/// Supported functions:
/// - max, min, sum, any, anyLast
/// - groupBitAnd, groupBitOr, groupBitXor (integer types)
/// </remarks>
public class ClickHouseSimpleAggregateFunctionTypeMapping : RelationalTypeMapping
{
    /// <summary>
    /// The aggregate function name (e.g., "max", "min", "sum").
    /// </summary>
    public string FunctionName { get; }

    /// <summary>
    /// The type mapping for the underlying value type.
    /// </summary>
    public RelationalTypeMapping UnderlyingMapping { get; }

    public ClickHouseSimpleAggregateFunctionTypeMapping(
        string functionName,
        RelationalTypeMapping underlyingMapping)
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(underlyingMapping.ClrType),
            BuildStoreType(functionName, underlyingMapping.StoreType)))
    {
        FunctionName = functionName;
        UnderlyingMapping = underlyingMapping;
    }

    protected ClickHouseSimpleAggregateFunctionTypeMapping(
        RelationalTypeMappingParameters parameters,
        string functionName,
        RelationalTypeMapping underlyingMapping)
        : base(parameters)
    {
        FunctionName = functionName;
        UnderlyingMapping = underlyingMapping;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseSimpleAggregateFunctionTypeMapping(parameters, FunctionName, UnderlyingMapping);

    /// <summary>
    /// Builds the ClickHouse store type string.
    /// </summary>
    private static string BuildStoreType(string functionName, string underlyingStoreType)
        => $"SimpleAggregateFunction({functionName}, {underlyingStoreType})";

    /// <summary>
    /// Generates SQL literal using the underlying type's literal generation.
    /// </summary>
    protected override string GenerateNonNullSqlLiteral(object value)
        => UnderlyingMapping.GenerateSqlLiteral(value);
}
