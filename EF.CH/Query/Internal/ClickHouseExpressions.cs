using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
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

/// <summary>
/// Wraps a TableExpressionBase with ClickHouse-specific modifiers (FINAL, SAMPLE).
/// Used by the query translation postprocessor to inject modifiers into the expression tree.
/// </summary>
public class ClickHouseTableModifierExpression : TableExpressionBase
{
    /// <summary>
    /// Gets the wrapped table expression.
    /// </summary>
    public TableExpressionBase Table { get; }

    /// <summary>
    /// Gets whether to apply the FINAL modifier.
    /// </summary>
    public bool UseFinal { get; }

    /// <summary>
    /// Gets the sample fraction (0.0 to 1.0), or null if no sampling.
    /// </summary>
    public double? SampleFraction { get; }

    /// <summary>
    /// Gets the sample offset for reproducible sampling.
    /// </summary>
    public double? SampleOffset { get; }

    public ClickHouseTableModifierExpression(
        TableExpressionBase table,
        bool useFinal,
        double? sampleFraction = null,
        double? sampleOffset = null)
        : base(table.Alias)
    {
        Table = table ?? throw new ArgumentNullException(nameof(table));
        UseFinal = useFinal;
        SampleFraction = sampleFraction;
        SampleOffset = sampleOffset;
    }

    private ClickHouseTableModifierExpression(
        TableExpressionBase table,
        bool useFinal,
        double? sampleFraction,
        double? sampleOffset,
        string? alias,
        IReadOnlyDictionary<string, IAnnotation>? annotations)
        : base(alias, annotations)
    {
        Table = table ?? throw new ArgumentNullException(nameof(table));
        UseFinal = useFinal;
        SampleFraction = sampleFraction;
        SampleOffset = sampleOffset;
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var newTable = (TableExpressionBase)visitor.Visit(Table);
        return newTable != Table
            ? new ClickHouseTableModifierExpression(newTable, UseFinal, SampleFraction, SampleOffset)
            : this;
    }

    public override TableExpressionBase Clone(string? alias, ExpressionVisitor cloningExpressionVisitor)
    {
        var newTable = (TableExpressionBase)cloningExpressionVisitor.Visit(Table);
        return new ClickHouseTableModifierExpression(newTable, UseFinal, SampleFraction, SampleOffset, alias, null);
    }

    public override ClickHouseTableModifierExpression WithAlias(string newAlias)
        => new(Table, UseFinal, SampleFraction, SampleOffset, newAlias, Annotations);

    protected override TableExpressionBase WithAnnotations(IReadOnlyDictionary<string, IAnnotation> annotations)
        => new ClickHouseTableModifierExpression(Table, UseFinal, SampleFraction, SampleOffset, Alias, annotations);

    public override Expression Quote()
        => New(
            typeof(ClickHouseTableModifierExpression).GetConstructors()
                .First(c => c.GetParameters().Length == 4),
            Table.Quote(),
            Constant(UseFinal),
            Constant(SampleFraction, typeof(double?)),
            Constant(SampleOffset, typeof(double?)));

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Visit(Table);
        if (UseFinal)
            expressionPrinter.Append(" FINAL");
        if (SampleFraction.HasValue)
        {
            expressionPrinter.Append($" SAMPLE {SampleFraction.Value}");
            if (SampleOffset.HasValue)
                expressionPrinter.Append($" OFFSET {SampleOffset.Value}");
        }
    }

    public override bool Equals(object? obj)
        => obj is ClickHouseTableModifierExpression other
           && Table.Equals(other.Table)
           && UseFinal == other.UseFinal
           && SampleFraction == other.SampleFraction
           && SampleOffset == other.SampleOffset;

    public override int GetHashCode()
        => HashCode.Combine(Table, UseFinal, SampleFraction, SampleOffset);
}
