using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Expressions;

/// <summary>
/// Represents a ClickHouse ARRAY JOIN expression.
/// ARRAY JOIN "explodes" array columns into separate rows (like UNNEST in PostgreSQL).
/// </summary>
public class ClickHouseArrayJoinExpression : TableExpressionBase
{
    /// <summary>
    /// Gets the source table expression.
    /// </summary>
    public TableExpressionBase Source { get; }

    /// <summary>
    /// Gets the array column being exploded.
    /// </summary>
    public SqlExpression ArrayColumn { get; }

    /// <summary>
    /// Gets the alias for the exploded element.
    /// </summary>
    public string ElementAlias { get; }

    /// <summary>
    /// Gets the optional alias for the 1-based index (arrayEnumerate).
    /// </summary>
    public string? IndexAlias { get; }

    /// <summary>
    /// Gets whether this is a LEFT ARRAY JOIN (preserves rows with empty arrays).
    /// </summary>
    public bool IsLeftJoin { get; }

    /// <summary>
    /// Creates a new ARRAY JOIN expression.
    /// </summary>
    public ClickHouseArrayJoinExpression(
        TableExpressionBase source,
        SqlExpression arrayColumn,
        string elementAlias,
        bool isLeftJoin,
        string? indexAlias = null,
        string? tableAlias = null)
        : base(tableAlias)
    {
        Source = source ?? throw new ArgumentNullException(nameof(source));
        ArrayColumn = arrayColumn ?? throw new ArgumentNullException(nameof(arrayColumn));
        ElementAlias = elementAlias ?? throw new ArgumentNullException(nameof(elementAlias));
        IsLeftJoin = isLeftJoin;
        IndexAlias = indexAlias;
    }

    private ClickHouseArrayJoinExpression(
        TableExpressionBase source,
        SqlExpression arrayColumn,
        string elementAlias,
        bool isLeftJoin,
        string? indexAlias,
        string? tableAlias,
        IEnumerable<IAnnotation>? annotations)
        : base(tableAlias, annotations)
    {
        Source = source;
        ArrayColumn = arrayColumn;
        ElementAlias = elementAlias;
        IsLeftJoin = isLeftJoin;
        IndexAlias = indexAlias;
    }

    /// <inheritdoc />
    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var newSource = (TableExpressionBase)visitor.Visit(Source);
        var newArrayColumn = (SqlExpression)visitor.Visit(ArrayColumn);

        if (newSource != Source || newArrayColumn != ArrayColumn)
        {
            return new ClickHouseArrayJoinExpression(
                newSource,
                newArrayColumn,
                ElementAlias,
                IsLeftJoin,
                IndexAlias,
                Alias);
        }

        return this;
    }

    /// <inheritdoc />
    protected override TableExpressionBase CreateWithAnnotations(IEnumerable<IAnnotation> annotations)
        => new ClickHouseArrayJoinExpression(
            Source, ArrayColumn, ElementAlias, IsLeftJoin, IndexAlias, Alias, annotations);

    /// <summary>
    /// Creates a copy with updated child expressions.
    /// </summary>
    public ClickHouseArrayJoinExpression Update(
        TableExpressionBase source,
        SqlExpression arrayColumn)
    {
        if (ReferenceEquals(source, Source) && ReferenceEquals(arrayColumn, ArrayColumn))
        {
            return this;
        }

        return new ClickHouseArrayJoinExpression(
            source, arrayColumn, ElementAlias, IsLeftJoin, IndexAlias, Alias);
    }

    /// <inheritdoc />
    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Visit(Source);
        expressionPrinter.AppendLine();
        expressionPrinter.Append(IsLeftJoin ? "LEFT ARRAY JOIN " : "ARRAY JOIN ");
        expressionPrinter.Visit(ArrayColumn);
        expressionPrinter.Append(" AS ");
        expressionPrinter.Append(ElementAlias);

        if (!string.IsNullOrEmpty(IndexAlias))
        {
            expressionPrinter.Append(", arrayEnumerate(");
            expressionPrinter.Visit(ArrayColumn);
            expressionPrinter.Append(") AS ");
            expressionPrinter.Append(IndexAlias);
        }

        if (!string.IsNullOrEmpty(Alias))
        {
            expressionPrinter.Append(" /* table alias: ");
            expressionPrinter.Append(Alias);
            expressionPrinter.Append(" */");
        }
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
        => obj is ClickHouseArrayJoinExpression other && Equals(other);

    private bool Equals(ClickHouseArrayJoinExpression other)
        => Alias == other.Alias
           && Source.Equals(other.Source)
           && ArrayColumn.Equals(other.ArrayColumn)
           && ElementAlias == other.ElementAlias
           && IndexAlias == other.IndexAlias
           && IsLeftJoin == other.IsLeftJoin;

    /// <inheritdoc />
    public override int GetHashCode()
        => HashCode.Combine(Alias, Source, ArrayColumn, ElementAlias, IndexAlias, IsLeftJoin);
}
