using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal;

/// <summary>
/// Processes SQL nullability for ClickHouse, handling custom table expressions like ClickHouseTableModifierExpression.
/// </summary>
public class ClickHouseSqlNullabilityProcessor : SqlNullabilityProcessor
{
    public ClickHouseSqlNullabilityProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters)
        : base(dependencies, parameters)
    {
    }

    /// <summary>
    /// Visits table expressions, handling ClickHouse-specific expressions.
    /// </summary>
    protected override TableExpressionBase Visit(TableExpressionBase tableExpressionBase)
    {
        if (tableExpressionBase is ClickHouseTableModifierExpression modifierExpression)
        {
            // Visit the wrapped table expression
            var visitedTable = Visit(modifierExpression.Table);

            // Return a new modifier expression with the visited table
            return visitedTable != modifierExpression.Table
                ? new ClickHouseTableModifierExpression(
                    visitedTable,
                    modifierExpression.UseFinal,
                    modifierExpression.SampleFraction,
                    modifierExpression.SampleOffset)
                : modifierExpression;
        }

        return base.Visit(tableExpressionBase);
    }
}
