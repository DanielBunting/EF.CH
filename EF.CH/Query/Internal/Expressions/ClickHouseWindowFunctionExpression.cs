using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Query.Internal.Expressions;

/// <summary>
/// Represents a window function call with an OVER clause in ClickHouse.
/// Examples: row_number() OVER (PARTITION BY x ORDER BY y)
/// </summary>
public sealed class ClickHouseWindowFunctionExpression : SqlExpression
{
    /// <summary>
    /// Gets the name of the window function (e.g., "row_number", "lagInFrame").
    /// </summary>
    public string FunctionName { get; }

    /// <summary>
    /// Gets the arguments to the window function (e.g., value and offset for lagInFrame).
    /// </summary>
    public IReadOnlyList<SqlExpression> Arguments { get; }

    /// <summary>
    /// Gets the PARTITION BY expressions.
    /// </summary>
    public IReadOnlyList<SqlExpression> PartitionBy { get; }

    /// <summary>
    /// Gets the ORDER BY expressions with their sort directions.
    /// </summary>
    public IReadOnlyList<OrderingExpression> OrderBy { get; }

    /// <summary>
    /// Gets the optional frame clause specification.
    /// </summary>
    public WindowFrame? Frame { get; }

    /// <summary>
    /// Gets the SQL result type (e.g., decimal for WindowSum of decimals).
    /// This is the actual data type returned by the SQL function.
    /// </summary>
    public Type ResultType { get; }

    /// <summary>
    /// Creates a new window function expression.
    /// </summary>
    public ClickHouseWindowFunctionExpression(
        string functionName,
        IReadOnlyList<SqlExpression> arguments,
        IReadOnlyList<SqlExpression> partitionBy,
        IReadOnlyList<OrderingExpression> orderBy,
        WindowFrame? frame,
        Type resultType,
        Type expressionType,
        RelationalTypeMapping? typeMapping)
        : base(expressionType, typeMapping)
    {
        FunctionName = functionName ?? throw new ArgumentNullException(nameof(functionName));
        Arguments = arguments ?? throw new ArgumentNullException(nameof(arguments));
        PartitionBy = partitionBy ?? throw new ArgumentNullException(nameof(partitionBy));
        OrderBy = orderBy ?? throw new ArgumentNullException(nameof(orderBy));
        Frame = frame;
        ResultType = resultType;
    }

    /// <summary>
    /// Creates a new window function expression (legacy constructor for compatibility).
    /// </summary>
    public ClickHouseWindowFunctionExpression(
        string functionName,
        IReadOnlyList<SqlExpression> arguments,
        IReadOnlyList<SqlExpression> partitionBy,
        IReadOnlyList<OrderingExpression> orderBy,
        WindowFrame? frame,
        Type type,
        RelationalTypeMapping? typeMapping)
        : this(functionName, arguments, partitionBy, orderBy, frame, type, type, typeMapping)
    {
    }

    /// <summary>
    /// Applies the specified type mapping to this expression.
    /// </summary>
    public SqlExpression ApplyTypeMapping(RelationalTypeMapping? typeMapping)
        => new ClickHouseWindowFunctionExpression(
            FunctionName,
            Arguments,
            PartitionBy,
            OrderBy,
            Frame,
            ResultType,
            Type,
            typeMapping ?? TypeMapping);

    /// <inheritdoc />
    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var changed = false;

        var arguments = new List<SqlExpression>(Arguments.Count);
        foreach (var arg in Arguments)
        {
            var newArg = (SqlExpression)visitor.Visit(arg);
            changed |= newArg != arg;
            arguments.Add(newArg);
        }

        var partitionBy = new List<SqlExpression>(PartitionBy.Count);
        foreach (var expr in PartitionBy)
        {
            var newExpr = (SqlExpression)visitor.Visit(expr);
            changed |= newExpr != expr;
            partitionBy.Add(newExpr);
        }

        var orderBy = new List<OrderingExpression>(OrderBy.Count);
        foreach (var ordering in OrderBy)
        {
            var newOrdering = (OrderingExpression)visitor.Visit(ordering);
            changed |= newOrdering != ordering;
            orderBy.Add(newOrdering);
        }

        return changed
            ? new ClickHouseWindowFunctionExpression(FunctionName, arguments, partitionBy, orderBy, Frame, ResultType, Type, TypeMapping)
            : this;
    }

    /// <summary>
    /// Creates an updated expression with new child expressions.
    /// </summary>
    public ClickHouseWindowFunctionExpression Update(
        IReadOnlyList<SqlExpression> arguments,
        IReadOnlyList<SqlExpression> partitionBy,
        IReadOnlyList<OrderingExpression> orderBy,
        WindowFrame? frame)
    {
        if (ReferenceEquals(arguments, Arguments) &&
            ReferenceEquals(partitionBy, PartitionBy) &&
            ReferenceEquals(orderBy, OrderBy) &&
            frame == Frame)
        {
            return this;
        }

        return new ClickHouseWindowFunctionExpression(
            FunctionName,
            arguments,
            partitionBy,
            orderBy,
            frame,
            ResultType,
            Type,
            TypeMapping);
    }

    /// <inheritdoc />
    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Append(FunctionName);
        expressionPrinter.Append("(");

        for (var i = 0; i < Arguments.Count; i++)
        {
            if (i > 0) expressionPrinter.Append(", ");
            expressionPrinter.Visit(Arguments[i]);
        }

        expressionPrinter.Append(") OVER (");

        if (PartitionBy.Count > 0)
        {
            expressionPrinter.Append("PARTITION BY ");
            for (var i = 0; i < PartitionBy.Count; i++)
            {
                if (i > 0) expressionPrinter.Append(", ");
                expressionPrinter.Visit(PartitionBy[i]);
            }
        }

        if (OrderBy.Count > 0)
        {
            if (PartitionBy.Count > 0) expressionPrinter.Append(" ");
            expressionPrinter.Append("ORDER BY ");
            for (var i = 0; i < OrderBy.Count; i++)
            {
                if (i > 0) expressionPrinter.Append(", ");
                expressionPrinter.Visit(OrderBy[i]);
            }
        }

        if (Frame != null)
        {
            expressionPrinter.Append(" ");
            PrintFrame(expressionPrinter, Frame);
        }

        expressionPrinter.Append(")");
    }

    private static void PrintFrame(ExpressionPrinter printer, WindowFrame frame)
    {
        printer.Append(frame.Type == WindowFrameType.Rows ? "ROWS" : "RANGE");
        printer.Append(" BETWEEN ");
        PrintFrameBound(printer, frame.StartBound, frame.StartOffset);
        printer.Append(" AND ");
        PrintFrameBound(printer, frame.EndBound, frame.EndOffset);
    }

    private static void PrintFrameBound(ExpressionPrinter printer, WindowFrameBound bound, int? offset)
    {
        var text = bound switch
        {
            WindowFrameBound.UnboundedPreceding => "UNBOUNDED PRECEDING",
            WindowFrameBound.Preceding => $"{offset} PRECEDING",
            WindowFrameBound.CurrentRow => "CURRENT ROW",
            WindowFrameBound.Following => $"{offset} FOLLOWING",
            WindowFrameBound.UnboundedFollowing => "UNBOUNDED FOLLOWING",
            _ => throw new InvalidOperationException($"Unknown frame bound: {bound}")
        };
        printer.Append(text);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
        => obj is ClickHouseWindowFunctionExpression other && Equals(other);

    private bool Equals(ClickHouseWindowFunctionExpression other)
        => base.Equals(other)
           && FunctionName == other.FunctionName
           && ResultType == other.ResultType
           && Arguments.SequenceEqual(other.Arguments)
           && PartitionBy.SequenceEqual(other.PartitionBy)
           && OrderBy.SequenceEqual(other.OrderBy)
           && Equals(Frame, other.Frame);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(base.GetHashCode());
        hash.Add(FunctionName);
        hash.Add(ResultType);
        foreach (var arg in Arguments) hash.Add(arg);
        foreach (var expr in PartitionBy) hash.Add(expr);
        foreach (var ordering in OrderBy) hash.Add(ordering);
        hash.Add(Frame);
        return hash.ToHashCode();
    }
}
