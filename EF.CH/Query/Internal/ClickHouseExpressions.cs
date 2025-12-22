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

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var newTable = (TableExpressionBase)visitor.Visit(Table);
        return newTable != Table
            ? new ClickHouseTableModifierExpression(newTable, UseFinal, SampleFraction, SampleOffset)
            : this;
    }

    /// <inheritdoc />
    protected override TableExpressionBase CreateWithAnnotations(IEnumerable<IAnnotation> annotations)
        => new ClickHouseTableModifierExpression(Table, UseFinal, SampleFraction, SampleOffset);

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

/// <summary>
/// Represents an external table function call (e.g., postgresql(...)).
/// Replaces TableExpression for entities configured as external table functions.
/// </summary>
public class ClickHouseExternalTableFunctionExpression : TableExpressionBase
{
    /// <summary>
    /// Gets the function name (e.g., "postgresql").
    /// </summary>
    public string FunctionName { get; }

    /// <summary>
    /// Gets the complete function call with resolved parameters.
    /// E.g., "postgresql('localhost:5432', 'mydb', 'customers', 'user', 'pass', 'public')"
    /// </summary>
    public string FunctionCall { get; }

    /// <summary>
    /// Gets the CLR type of the entity this expression represents.
    /// </summary>
    public Type EntityClrType { get; }

    public ClickHouseExternalTableFunctionExpression(
        string alias,
        string functionName,
        string functionCall,
        Type entityClrType)
        : base(alias)
    {
        FunctionName = functionName ?? throw new ArgumentNullException(nameof(functionName));
        FunctionCall = functionCall ?? throw new ArgumentNullException(nameof(functionCall));
        EntityClrType = entityClrType ?? throw new ArgumentNullException(nameof(entityClrType));
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor)
        => this; // No children to visit - this is a leaf node

    /// <inheritdoc />
    protected override TableExpressionBase CreateWithAnnotations(IEnumerable<IAnnotation> annotations)
        => new ClickHouseExternalTableFunctionExpression(Alias!, FunctionName, FunctionCall, EntityClrType);

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Append(FunctionCall);
        if (!string.IsNullOrEmpty(Alias))
        {
            expressionPrinter.Append(" AS ");
            expressionPrinter.Append(Alias);
        }
    }

    public override bool Equals(object? obj)
        => obj is ClickHouseExternalTableFunctionExpression other
           && FunctionName == other.FunctionName
           && FunctionCall == other.FunctionCall
           && EntityClrType == other.EntityClrType
           && Alias == other.Alias;

    public override int GetHashCode()
        => HashCode.Combine(FunctionName, FunctionCall, EntityClrType, Alias);
}

/// <summary>
/// Represents a dictionary table function call (dictionary('name')).
/// Replaces TableExpression for entities configured as dictionaries when used in JOINs.
/// </summary>
public class ClickHouseDictionaryTableExpression : TableExpressionBase
{
    /// <summary>
    /// Gets the dictionary name.
    /// </summary>
    public string DictionaryName { get; }

    /// <summary>
    /// Gets the complete function call: dictionary('name')
    /// </summary>
    public string FunctionCall { get; }

    /// <summary>
    /// Gets the CLR type of the dictionary entity.
    /// </summary>
    public Type EntityClrType { get; }

    public ClickHouseDictionaryTableExpression(
        string alias,
        string dictionaryName,
        Type entityClrType)
        : base(alias)
    {
        DictionaryName = dictionaryName ?? throw new ArgumentNullException(nameof(dictionaryName));
        EntityClrType = entityClrType ?? throw new ArgumentNullException(nameof(entityClrType));
        FunctionCall = $"dictionary('{dictionaryName}')";
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor)
        => this; // No children to visit - this is a leaf node

    /// <inheritdoc />
    protected override TableExpressionBase CreateWithAnnotations(IEnumerable<IAnnotation> annotations)
        => new ClickHouseDictionaryTableExpression(Alias!, DictionaryName, EntityClrType);

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Append(FunctionCall);
        if (!string.IsNullOrEmpty(Alias))
        {
            expressionPrinter.Append(" AS ");
            expressionPrinter.Append(Alias);
        }
    }

    public override bool Equals(object? obj)
        => obj is ClickHouseDictionaryTableExpression other
           && DictionaryName == other.DictionaryName
           && EntityClrType == other.EntityClrType
           && Alias == other.Alias;

    public override int GetHashCode()
        => HashCode.Combine(DictionaryName, EntityClrType, Alias);
}
