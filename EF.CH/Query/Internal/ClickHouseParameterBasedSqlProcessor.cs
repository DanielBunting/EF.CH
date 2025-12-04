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
        ParametersCacheDecorator decorator)
    {
        return new ClickHouseSqlNullabilityProcessor(Dependencies, Parameters)
            .Process(queryExpression, decorator);
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
