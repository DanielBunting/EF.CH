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
        bool useRelationalNulls)
        : base(dependencies, useRelationalNulls)
    {
    }

    /// <inheritdoc />
    protected override Expression ProcessSqlNullability(
        Expression queryExpression,
        IReadOnlyDictionary<string, object?> parametersValues,
        out bool canCache)
    {
        return new ClickHouseSqlNullabilityProcessor(Dependencies, UseRelationalNulls)
            .Process(queryExpression, parametersValues, out canCache);
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

    public virtual RelationalParameterBasedSqlProcessor Create(bool useRelationalNulls)
        => new ClickHouseParameterBasedSqlProcessor(_dependencies, useRelationalNulls);
}
