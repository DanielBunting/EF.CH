using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Query.Internal;

/// <summary>
/// Factory for creating ClickHouse SQL expressions.
/// </summary>
public class ClickHouseSqlExpressionFactory : SqlExpressionFactory
{
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public ClickHouseSqlExpressionFactory(SqlExpressionFactoryDependencies dependencies)
        : base(dependencies)
    {
        _typeMappingSource = dependencies.TypeMappingSource;
    }

    /// <summary>
    /// Gets the type mapping source for ClickHouse types.
    /// </summary>
    public IRelationalTypeMappingSource TypeMappingSource => _typeMappingSource;

    /// <summary>
    /// Creates a SQL function call expression for ClickHouse-specific functions.
    /// </summary>
    public SqlExpression ClickHouseFunction(
        string name,
        IEnumerable<SqlExpression> arguments,
        bool nullable,
        IEnumerable<bool> argumentsPropagateNullability,
        Type returnType,
        RelationalTypeMapping? typeMapping = null)
    {
        return Function(
            name,
            arguments,
            nullable,
            argumentsPropagateNullability,
            returnType,
            typeMapping);
    }

    /// <summary>
    /// Creates a concat function expression (ClickHouse uses concat() for string concatenation).
    /// </summary>
    public SqlExpression ConcatStrings(params SqlExpression[] arguments)
    {
        return Function(
            "concat",
            arguments,
            nullable: true,
            argumentsPropagateNullability: arguments.Select(_ => true),
            typeof(string),
            _typeMappingSource.FindMapping(typeof(string)));
    }

    /// <summary>
    /// Creates an if() expression (ClickHouse's ternary operator).
    /// </summary>
    public SqlExpression If(
        SqlExpression condition,
        SqlExpression ifTrue,
        SqlExpression ifFalse,
        Type returnType)
    {
        return Function(
            "if",
            new[] { condition, ifTrue, ifFalse },
            nullable: true,
            argumentsPropagateNullability: new[] { false, true, true },
            returnType,
            _typeMappingSource.FindMapping(returnType));
    }

    /// <summary>
    /// Creates a multiIf() expression for multiple conditions.
    /// </summary>
    public SqlExpression MultiIf(
        IEnumerable<(SqlExpression Condition, SqlExpression Result)> cases,
        SqlExpression @else,
        Type returnType)
    {
        var arguments = new List<SqlExpression>();
        var nullability = new List<bool>();

        foreach (var (condition, result) in cases)
        {
            arguments.Add(condition);
            arguments.Add(result);
            nullability.Add(false);
            nullability.Add(true);
        }

        arguments.Add(@else);
        nullability.Add(true);

        return Function(
            "multiIf",
            arguments,
            nullable: true,
            argumentsPropagateNullability: nullability,
            returnType,
            _typeMappingSource.FindMapping(returnType));
    }

    /// <summary>
    /// Creates a coalesce() expression.
    /// </summary>
    public SqlExpression ClickHouseCoalesce(params SqlExpression[] arguments)
    {
        if (arguments.Length == 0)
        {
            throw new ArgumentException("At least one argument is required.", nameof(arguments));
        }

        var returnType = arguments[0].Type;
        var typeMapping = arguments[0].TypeMapping ?? _typeMappingSource.FindMapping(returnType);

        return Function(
            "coalesce",
            arguments,
            nullable: true,
            argumentsPropagateNullability: arguments.Select(_ => true),
            returnType,
            typeMapping);
    }

    /// <summary>
    /// Creates a LIKE expression.
    /// </summary>
    public SqlExpression ClickHouseLike(SqlExpression match, SqlExpression pattern, SqlExpression? escapeChar = null)
    {
        return Like(match, pattern, escapeChar);
    }

    /// <summary>
    /// Creates an aggregate function with OrNull suffix for null-safe behavior.
    /// </summary>
    public SqlExpression AggregateOrNull(
        string functionName,
        SqlExpression argument,
        Type returnType)
    {
        // Use *OrNull variants for null-safe aggregates
        var nullSafeName = functionName.EndsWith("OrNull", StringComparison.OrdinalIgnoreCase)
            ? functionName
            : $"{functionName}OrNull";

        return Function(
            nullSafeName,
            new[] { argument },
            nullable: true,
            argumentsPropagateNullability: new[] { false },
            returnType,
            _typeMappingSource.FindMapping(returnType));
    }

    /// <summary>
    /// Creates a raw SQL fragment expression.
    /// </summary>
    /// <param name="sql">The raw SQL to embed.</param>
    /// <param name="typeMapping">Optional type mapping for the result.</param>
    /// <returns>A SQL fragment expression.</returns>
    public SqlFragmentExpression Fragment(string sql, RelationalTypeMapping? typeMapping = null)
    {
        return new SqlFragmentExpression(sql);
    }
}
