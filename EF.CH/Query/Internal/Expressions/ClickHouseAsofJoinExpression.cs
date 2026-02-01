using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Expressions;

/// <summary>
/// Represents a ClickHouse ASOF JOIN expression.
/// ASOF JOIN matches rows based on the closest ASOF key value rather than exact equality.
/// </summary>
public class ClickHouseAsofJoinExpression : TableExpressionBase
{
    /// <summary>
    /// Gets the left (outer) table expression.
    /// </summary>
    public TableExpressionBase Left { get; }

    /// <summary>
    /// Gets the right (inner) table expression.
    /// </summary>
    public TableExpressionBase Right { get; }

    /// <summary>
    /// Gets the equality predicate (ON clause columns that must match exactly).
    /// </summary>
    public SqlExpression EqualityPredicate { get; }

    /// <summary>
    /// Gets the ASOF predicate column from the left table.
    /// </summary>
    public SqlExpression LeftAsofColumn { get; }

    /// <summary>
    /// Gets the ASOF predicate column from the right table.
    /// </summary>
    public SqlExpression RightAsofColumn { get; }

    /// <summary>
    /// Gets the ASOF comparison type (>=, >, <=, <).
    /// </summary>
    public AsofJoinType AsofType { get; }

    /// <summary>
    /// Gets whether this is a LEFT JOIN (preserves unmatched left rows).
    /// </summary>
    public bool IsLeftJoin { get; }

    /// <summary>
    /// Creates a new ASOF JOIN expression.
    /// </summary>
    public ClickHouseAsofJoinExpression(
        TableExpressionBase left,
        TableExpressionBase right,
        SqlExpression equalityPredicate,
        SqlExpression leftAsofColumn,
        SqlExpression rightAsofColumn,
        AsofJoinType asofType,
        bool isLeftJoin,
        string? alias = null)
        : base(alias)
    {
        Left = left ?? throw new ArgumentNullException(nameof(left));
        Right = right ?? throw new ArgumentNullException(nameof(right));
        EqualityPredicate = equalityPredicate ?? throw new ArgumentNullException(nameof(equalityPredicate));
        LeftAsofColumn = leftAsofColumn ?? throw new ArgumentNullException(nameof(leftAsofColumn));
        RightAsofColumn = rightAsofColumn ?? throw new ArgumentNullException(nameof(rightAsofColumn));
        AsofType = asofType;
        IsLeftJoin = isLeftJoin;
    }

    private ClickHouseAsofJoinExpression(
        TableExpressionBase left,
        TableExpressionBase right,
        SqlExpression equalityPredicate,
        SqlExpression leftAsofColumn,
        SqlExpression rightAsofColumn,
        AsofJoinType asofType,
        bool isLeftJoin,
        string? alias,
        IEnumerable<IAnnotation>? annotations)
        : base(alias, annotations)
    {
        Left = left;
        Right = right;
        EqualityPredicate = equalityPredicate;
        LeftAsofColumn = leftAsofColumn;
        RightAsofColumn = rightAsofColumn;
        AsofType = asofType;
        IsLeftJoin = isLeftJoin;
    }

    /// <inheritdoc />
    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var newLeft = (TableExpressionBase)visitor.Visit(Left);
        var newRight = (TableExpressionBase)visitor.Visit(Right);
        var newEqualityPredicate = (SqlExpression)visitor.Visit(EqualityPredicate);
        var newLeftAsofColumn = (SqlExpression)visitor.Visit(LeftAsofColumn);
        var newRightAsofColumn = (SqlExpression)visitor.Visit(RightAsofColumn);

        if (newLeft != Left ||
            newRight != Right ||
            newEqualityPredicate != EqualityPredicate ||
            newLeftAsofColumn != LeftAsofColumn ||
            newRightAsofColumn != RightAsofColumn)
        {
            return new ClickHouseAsofJoinExpression(
                newLeft,
                newRight,
                newEqualityPredicate,
                newLeftAsofColumn,
                newRightAsofColumn,
                AsofType,
                IsLeftJoin,
                Alias);
        }

        return this;
    }

    /// <inheritdoc />
    protected override TableExpressionBase CreateWithAnnotations(IEnumerable<IAnnotation> annotations)
        => new ClickHouseAsofJoinExpression(
            Left, Right, EqualityPredicate, LeftAsofColumn, RightAsofColumn, AsofType, IsLeftJoin, Alias, annotations);

    /// <summary>
    /// Creates a copy with updated child expressions.
    /// </summary>
    public ClickHouseAsofJoinExpression Update(
        TableExpressionBase left,
        TableExpressionBase right,
        SqlExpression equalityPredicate,
        SqlExpression leftAsofColumn,
        SqlExpression rightAsofColumn)
    {
        if (ReferenceEquals(left, Left) &&
            ReferenceEquals(right, Right) &&
            ReferenceEquals(equalityPredicate, EqualityPredicate) &&
            ReferenceEquals(leftAsofColumn, LeftAsofColumn) &&
            ReferenceEquals(rightAsofColumn, RightAsofColumn))
        {
            return this;
        }

        return new ClickHouseAsofJoinExpression(
            left, right, equalityPredicate, leftAsofColumn, rightAsofColumn, AsofType, IsLeftJoin, Alias);
    }

    /// <inheritdoc />
    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Visit(Left);
        expressionPrinter.AppendLine();
        expressionPrinter.Append(IsLeftJoin ? "ASOF LEFT JOIN " : "ASOF JOIN ");
        expressionPrinter.Visit(Right);
        expressionPrinter.Append(" ON ");
        expressionPrinter.Visit(EqualityPredicate);
        expressionPrinter.Append(" AND ");
        expressionPrinter.Visit(LeftAsofColumn);
        expressionPrinter.Append(GetAsofOperator());
        expressionPrinter.Visit(RightAsofColumn);

        if (!string.IsNullOrEmpty(Alias))
        {
            expressionPrinter.Append(" AS ");
            expressionPrinter.Append(Alias);
        }
    }

    /// <summary>
    /// Gets the SQL operator string for the ASOF comparison type.
    /// </summary>
    public string GetAsofOperator() => AsofType switch
    {
        AsofJoinType.GreaterOrEqual => " >= ",
        AsofJoinType.Greater => " > ",
        AsofJoinType.LessOrEqual => " <= ",
        AsofJoinType.Less => " < ",
        _ => throw new InvalidOperationException($"Unknown ASOF join type: {AsofType}")
    };

    /// <inheritdoc />
    public override bool Equals(object? obj)
        => obj is ClickHouseAsofJoinExpression other && Equals(other);

    private bool Equals(ClickHouseAsofJoinExpression other)
        => Alias == other.Alias
           && Left.Equals(other.Left)
           && Right.Equals(other.Right)
           && EqualityPredicate.Equals(other.EqualityPredicate)
           && LeftAsofColumn.Equals(other.LeftAsofColumn)
           && RightAsofColumn.Equals(other.RightAsofColumn)
           && AsofType == other.AsofType
           && IsLeftJoin == other.IsLeftJoin;

    /// <inheritdoc />
    public override int GetHashCode()
        => HashCode.Combine(Alias, Left, Right, EqualityPredicate, LeftAsofColumn, RightAsofColumn, AsofType, IsLeftJoin);
}
