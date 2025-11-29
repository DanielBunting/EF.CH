using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal;

/// <summary>
/// Represents the FINAL modifier for ClickHouse ReplacingMergeTree tables.
/// Forces deduplication of rows at query time.
/// </summary>
public class ClickHouseFinalExpression : Expression
{
    /// <summary>
    /// Gets the table expression to apply FINAL to.
    /// </summary>
    public TableExpressionBase Table { get; }

    public ClickHouseFinalExpression(TableExpressionBase table)
    {
        Table = table ?? throw new ArgumentNullException(nameof(table));
    }

    public override Type Type => Table.Type;

    public override ExpressionType NodeType => ExpressionType.Extension;

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var newTable = (TableExpressionBase)visitor.Visit(Table);
        return newTable != Table ? new ClickHouseFinalExpression(newTable) : this;
    }

    public override bool Equals(object? obj)
        => obj is ClickHouseFinalExpression other && Table.Equals(other.Table);

    public override int GetHashCode()
        => HashCode.Combine(typeof(ClickHouseFinalExpression), Table);
}

/// <summary>
/// Represents the SAMPLE clause for probabilistic sampling in ClickHouse.
/// </summary>
public class ClickHouseSampleExpression : Expression
{
    /// <summary>
    /// Gets the sample fraction (0.0 to 1.0).
    /// </summary>
    public double Fraction { get; }

    /// <summary>
    /// Gets the optional sample offset for reproducible sampling.
    /// </summary>
    public double? Offset { get; }

    public ClickHouseSampleExpression(double fraction, double? offset = null)
    {
        if (fraction <= 0 || fraction > 1)
        {
            throw new ArgumentOutOfRangeException(nameof(fraction),
                "Sample fraction must be between 0 (exclusive) and 1 (inclusive).");
        }

        Fraction = fraction;
        Offset = offset;
    }

    public override Type Type => typeof(void);

    public override ExpressionType NodeType => ExpressionType.Extension;

    protected override Expression VisitChildren(ExpressionVisitor visitor)
        => this; // No children to visit

    public override bool Equals(object? obj)
        => obj is ClickHouseSampleExpression other
           && Fraction.Equals(other.Fraction)
           && Offset.Equals(other.Offset);

    public override int GetHashCode()
        => HashCode.Combine(typeof(ClickHouseSampleExpression), Fraction, Offset);
}
