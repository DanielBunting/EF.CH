using System.Linq.Expressions;

namespace EF.CH.Query.Internal;

/// <summary>
/// A custom expression node that wraps a constant value and is invisible to EF Core's
/// ExpressionTreeFuncletizer. Because it extends Expression with NodeType.Extension,
/// the funcletizer will not evaluate or parameterize it.
/// The ClickHouseEvaluatableExpressionFilterPlugin returns false for this type,
/// and TryGetConstantValue in the translator unwraps it.
/// </summary>
public sealed class ClickHouseConstantExpression : Expression
{
    /// <summary>
    /// Gets the constant value.
    /// </summary>
    public object? Value { get; }

    private readonly Type _type;

    public ClickHouseConstantExpression(object? value, Type type)
    {
        Value = value;
        _type = type ?? throw new ArgumentNullException(nameof(type));
    }

    public override ExpressionType NodeType => ExpressionType.Extension;
    public override Type Type => _type;

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override bool CanReduce => false;

    public override string ToString() => $"ClickHouseConstant({Value})";
}
