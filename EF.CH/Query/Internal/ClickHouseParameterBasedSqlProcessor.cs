using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;

namespace EF.CH.Query.Internal;

/// <summary>
/// ClickHouse-specific parameter-based SQL processor that uses our custom SqlNullabilityProcessor.
/// </summary>
public class ClickHouseParameterBasedSqlProcessor : RelationalParameterBasedSqlProcessor
{
    public ClickHouseParameterBasedSqlProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters)
        : base(dependencies, parameters)
    {
    }

    /// <inheritdoc />
    protected override Expression ProcessSqlNullability(
        Expression queryExpression,
        IReadOnlyDictionary<string, object?> parametersValues,
        out bool canCache)
    {
        // Resolve any deferred parameter values in the SQL generator's thread-local state.
        // EF Core 9+ auto-parameterizes constants, so values that were originally constant
        // may be stored as DeferredParameter objects that need resolution here.
        // This is called from Optimize() which runs at execution time with actual parameter values.
        ClickHouseQuerySqlGenerator.ResolveDeferredParameters(parametersValues);

        return new ClickHouseSqlNullabilityProcessor(Dependencies, Parameters)
            .Process(queryExpression, parametersValues, out canCache);
    }

    /// <inheritdoc />
    public override Expression Optimize(
        Expression queryExpression,
        IReadOnlyDictionary<string, object> parametersValues,
        out bool canCache)
    {
        // Resolve deferred parameters before any SQL generation.
        // Wrap in a nullable-friendly view for the resolver.
        var nullableView = parametersValues.ToDictionary(
            kvp => kvp.Key, kvp => (object?)kvp.Value);
        ClickHouseQuerySqlGenerator.ResolveDeferredParameters(nullableView);
        return base.Optimize(queryExpression, parametersValues, out canCache);
    }
}

/// <summary>
/// Factory for creating ClickHouse parameter-based SQL processors.
/// </summary>
public class ClickHouseParameterBasedSqlProcessorFactory : IRelationalParameterBasedSqlProcessorFactory
{
    private readonly RelationalParameterBasedSqlProcessorDependencies _dependencies;

    public ClickHouseParameterBasedSqlProcessorFactory(
        RelationalParameterBasedSqlProcessorDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    public virtual RelationalParameterBasedSqlProcessor Create(RelationalParameterBasedSqlProcessorParameters parameters)
        => new ClickHouseParameterBasedSqlProcessor(_dependencies, parameters);
}
