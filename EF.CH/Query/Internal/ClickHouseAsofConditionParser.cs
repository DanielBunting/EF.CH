using System.Linq.Expressions;

namespace EF.CH.Query.Internal;

/// <summary>
/// Shared helper for parsing the inequality lambda passed to <c>AsofJoin</c> /
/// <c>AsofLeftJoin</c>. Used by both the runtime preprocessor (which lowers
/// ASOF calls to standard <c>Queryable.Join</c> + thread-local metadata) and
/// the design-time materialized-view translator (which emits
/// <c>ASOF [LEFT] JOIN ... AND outerCol op innerCol</c> directly into the
/// CREATE MATERIALIZED VIEW SQL).
///
/// Single source of truth for the operator validation: ClickHouse only
/// accepts <c>&gt;=</c>, <c>&gt;</c>, <c>&lt;=</c>, <c>&lt;</c> as the ASOF
/// inequality.
/// </summary>
internal static class ClickHouseAsofConditionParser
{
    /// <summary>
    /// Parses an ASOF condition lambda body of the form
    /// <c>(outer, inner) =&gt; outer.Prop op inner.Prop</c> and returns the
    /// outer property name, inner property name, and SQL operator string.
    /// </summary>
    public static (string LeftProp, string RightProp, string Op) Parse(LambdaExpression lambda)
    {
        if (lambda.Body is not BinaryExpression binary)
        {
            throw new InvalidOperationException("ASOF condition must be a comparison (>=, >, <=, <).");
        }

        var op = binary.NodeType switch
        {
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.GreaterThan => ">",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.LessThan => "<",
            _ => throw new InvalidOperationException("ASOF condition must use >=, >, <=, or < operator.")
        };

        var leftProp = ExtractPropertyName(binary.Left);
        var rightProp = ExtractPropertyName(binary.Right);
        return (leftProp, rightProp, op);
    }

    private static string ExtractPropertyName(Expression expression)
    {
        if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
            expression = unary.Operand;

        if (expression is MemberExpression member)
            return member.Member.Name;

        throw new InvalidOperationException(
            $"ASOF condition must reference entity properties directly. Got: {expression.GetType().Name}");
    }
}
