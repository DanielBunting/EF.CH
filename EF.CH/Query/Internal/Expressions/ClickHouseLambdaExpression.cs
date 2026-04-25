using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Expressions;

/// <summary>
/// Represents a ClickHouse-style higher-order-function lambda, e.g. <c>x -> hasToken(doc, x)</c>
/// passed as the first argument to <c>arrayExists</c>, <c>arrayAll</c>, <c>arrayMap</c>, etc.
/// </summary>
public sealed class ClickHouseLambdaExpression : SqlExpression
{
    public IReadOnlyList<string> ParameterNames { get; }
    public SqlExpression Body { get; }

    public ClickHouseLambdaExpression(IReadOnlyList<string> parameterNames, SqlExpression body)
        : base(body.Type, body.TypeMapping)
    {
        ParameterNames = parameterNames;
        Body = body;
    }

    public ClickHouseLambdaExpression(string parameterName, SqlExpression body)
        : this(new[] { parameterName }, body) { }

    public override Expression Quote()
        => throw new InvalidOperationException("ClickHouseLambdaExpression is a custom expression that cannot be quoted.");

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var newBody = (SqlExpression)visitor.Visit(Body);
        return newBody == Body ? this : new ClickHouseLambdaExpression(ParameterNames, newBody);
    }

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        if (ParameterNames.Count == 1)
        {
            expressionPrinter.Append(ParameterNames[0]);
        }
        else
        {
            expressionPrinter.Append("(").Append(string.Join(", ", ParameterNames)).Append(")");
        }
        expressionPrinter.Append(" -> ");
        expressionPrinter.Visit(Body);
    }

    public override bool Equals(object? obj)
        => obj is ClickHouseLambdaExpression other
           && base.Equals(other)
           && ParameterNames.SequenceEqual(other.ParameterNames)
           && Body.Equals(other.Body);

    public override int GetHashCode()
    {
        var hash = base.GetHashCode();
        foreach (var name in ParameterNames) hash = HashCode.Combine(hash, name);
        return HashCode.Combine(hash, Body);
    }
}
