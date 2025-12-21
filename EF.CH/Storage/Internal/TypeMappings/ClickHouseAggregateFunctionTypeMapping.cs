using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Type mapping for ClickHouse AggregateFunction types.
/// </summary>
/// <remarks>
/// AggregateFunction stores opaque intermediate aggregation states (binary data),
/// so the CLR type is always byte[]. This is used for complex aggregates that need
/// the -State/-Merge combinator pattern (avg, uniq, quantile, etc.).
///
/// Usage with AggregatingMergeTree:
/// - On INSERT: Values are stored via aggregate function with -State combinator
/// - On MERGE: ClickHouse merges states using internal merge logic
/// - On SELECT: Must use -Merge combinator to get final result
///
/// Supported functions include all ClickHouse aggregate functions:
/// - count, sum, avg, min, max
/// - uniq, uniqExact, uniqHLL12
/// - quantile, median
/// - argMax, argMin
/// - groupArray, groupUniqArray
/// </remarks>
public class ClickHouseAggregateFunctionTypeMapping : RelationalTypeMapping
{
    /// <summary>
    /// The aggregate function name (e.g., "sum", "avg", "uniq").
    /// </summary>
    public string FunctionName { get; }

    /// <summary>
    /// The type mapping for the underlying value type used by the aggregate function.
    /// </summary>
    public RelationalTypeMapping UnderlyingMapping { get; }

    public ClickHouseAggregateFunctionTypeMapping(
        string functionName,
        RelationalTypeMapping underlyingMapping)
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(byte[])),
            BuildStoreType(functionName, underlyingMapping.StoreType)))
    {
        FunctionName = functionName;
        UnderlyingMapping = underlyingMapping;
    }

    protected ClickHouseAggregateFunctionTypeMapping(
        RelationalTypeMappingParameters parameters,
        string functionName,
        RelationalTypeMapping underlyingMapping)
        : base(parameters)
    {
        FunctionName = functionName;
        UnderlyingMapping = underlyingMapping;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseAggregateFunctionTypeMapping(parameters, FunctionName, UnderlyingMapping);

    /// <summary>
    /// Builds the ClickHouse store type string.
    /// </summary>
    /// <example>
    /// AggregateFunction(sum, UInt64)
    /// AggregateFunction(uniq, String)
    /// AggregateFunction(quantile(0.95), Float64)
    /// </example>
    private static string BuildStoreType(string functionName, string underlyingStoreType)
        => $"AggregateFunction({functionName}, {underlyingStoreType})";

    /// <summary>
    /// Generates SQL literal for binary aggregate state data.
    /// AggregateFunction states are opaque binary and cannot be represented as literals.
    /// </summary>
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        // AggregateFunction states are opaque binary data that cannot be inserted as literals.
        // They must be created via the aggregate function itself (e.g., sumState(value)).
        throw new InvalidOperationException(
            $"AggregateFunction({FunctionName}) states cannot be represented as SQL literals. " +
            "Use the corresponding -State aggregate function to insert values.");
    }
}
