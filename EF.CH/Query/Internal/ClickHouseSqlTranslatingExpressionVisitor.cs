using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal;

/// <summary>
/// Translates LINQ expressions to ClickHouse SQL expressions.
/// Handles ClickHouse-specific translation rules for binary operations,
/// method calls, and member access.
/// </summary>
public class ClickHouseSqlTranslatingExpressionVisitor : RelationalSqlTranslatingExpressionVisitor
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    public ClickHouseSqlTranslatingExpressionVisitor(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        : base(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
    {
        _sqlExpressionFactory = (ClickHouseSqlExpressionFactory)dependencies.SqlExpressionFactory;
    }

    /// <summary>
    /// Visits binary expressions and handles ClickHouse-specific translations.
    /// </summary>
    protected override Expression VisitBinary(BinaryExpression binaryExpression)
    {
        // Handle string concatenation: "a" + "b" → concat(a, b)
        if (binaryExpression.NodeType == ExpressionType.Add &&
            binaryExpression.Left.Type == typeof(string))
        {
            var left = Visit(binaryExpression.Left);
            var right = Visit(binaryExpression.Right);

            if (left is SqlExpression sqlLeft && right is SqlExpression sqlRight)
            {
                return _sqlExpressionFactory.ConcatStrings(sqlLeft, sqlRight);
            }
        }

        // Handle DateTime subtraction for TimeSpan operations
        if (binaryExpression.NodeType == ExpressionType.Subtract &&
            binaryExpression.Left.Type == typeof(DateTime) &&
            binaryExpression.Right.Type == typeof(DateTime))
        {
            var left = Visit(binaryExpression.Left);
            var right = Visit(binaryExpression.Right);

            if (left is SqlExpression sqlLeft && right is SqlExpression sqlRight)
            {
                // Result is TimeSpan - we'll handle TotalDays etc. in member access
                // For now, return dateDiff in seconds
                return _sqlExpressionFactory.Function(
                    "dateDiff",
                    new SqlExpression[]
                    {
                        _sqlExpressionFactory.Constant("second"),
                        sqlRight,
                        sqlLeft
                    },
                    nullable: true,
                    argumentsPropagateNullability: new[] { false, true, true },
                    typeof(long));
            }
        }

        return base.VisitBinary(binaryExpression);
    }

    /// <summary>
    /// Visits unary expressions for ClickHouse-specific handling.
    /// </summary>
    protected override Expression VisitUnary(UnaryExpression unaryExpression)
    {
        // Handle NOT operations
        if (unaryExpression.NodeType == ExpressionType.Not &&
            unaryExpression.Operand.Type == typeof(bool))
        {
            var operand = Visit(unaryExpression.Operand);
            if (operand is SqlExpression sqlOperand)
            {
                return _sqlExpressionFactory.Not(sqlOperand);
            }
        }

        return base.VisitUnary(unaryExpression);
    }

    /// <summary>
    /// Visits new expressions, handling tuple creation for ClickHouse.
    /// </summary>
    protected override Expression VisitNew(NewExpression newExpression)
    {
        // Handle anonymous types and tuples for projections
        return base.VisitNew(newExpression);
    }
}

/// <summary>
/// Factory for creating ClickHouse SQL translating expression visitors.
/// </summary>
public class ClickHouseSqlTranslatingExpressionVisitorFactory : IRelationalSqlTranslatingExpressionVisitorFactory
{
    private readonly RelationalSqlTranslatingExpressionVisitorDependencies _dependencies;

    public ClickHouseSqlTranslatingExpressionVisitorFactory(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    public RelationalSqlTranslatingExpressionVisitor Create(
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
    {
        return new ClickHouseSqlTranslatingExpressionVisitor(
            _dependencies,
            queryCompilationContext,
            queryableMethodTranslatingExpressionVisitor);
    }
}
