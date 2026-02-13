using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Query.Internal.Expressions;

/// <summary>
/// Represents a raw SQL fragment with a specific CLR type and type mapping.
/// Unlike <see cref="SqlFragmentExpression"/>, this carries the correct return type
/// so that EF Core's projection binding accepts it.
/// </summary>
public sealed class ClickHouseRawSqlExpression : SqlExpression
{
    /// <summary>
    /// Gets the raw SQL string to emit verbatim.
    /// </summary>
    public string Sql { get; }

    public ClickHouseRawSqlExpression(string sql, Type type, RelationalTypeMapping? typeMapping)
        : base(type, typeMapping)
    {
        Sql = sql ?? throw new ArgumentNullException(nameof(sql));
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Append(Sql);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
        => obj is ClickHouseRawSqlExpression other
           && base.Equals(other)
           && Sql == other.Sql;

    /// <inheritdoc />
    public override int GetHashCode()
        => HashCode.Combine(base.GetHashCode(), Sql);
}
