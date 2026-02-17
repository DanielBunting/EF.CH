using System.Linq.Expressions;

namespace EF.CH.ParameterizedViews;

/// <summary>
/// Metadata for a ClickHouse parameterized view.
/// </summary>
/// <typeparam name="TResult">The view result entity type.</typeparam>
public sealed class ParameterizedViewMetadata<TResult>
    where TResult : class
{
    /// <summary>
    /// Creates a new parameterized view metadata instance.
    /// </summary>
    /// <param name="viewName">The view name in ClickHouse.</param>
    /// <param name="resultType">The CLR type of the result entity.</param>
    public ParameterizedViewMetadata(string viewName, Type resultType)
    {
        ViewName = viewName;
        ResultType = resultType;
    }

    /// <summary>
    /// The view name in ClickHouse.
    /// </summary>
    public string ViewName { get; }

    /// <summary>
    /// The CLR type of the result entity.
    /// </summary>
    public Type ResultType { get; }
}

/// <summary>
/// Non-generic base class for parameterized view metadata storage.
/// Used for annotation storage, fluent configuration, and SQL generation.
/// </summary>
public sealed class ParameterizedViewMetadataBase
{
    /// <summary>
    /// The view name in ClickHouse.
    /// </summary>
    public required string ViewName { get; init; }

    /// <summary>
    /// The CLR type of the view result entity.
    /// </summary>
    public required Type ResultType { get; init; }

    /// <summary>
    /// The CLR type of the source entity (for fluent configuration).
    /// </summary>
    public Type? SourceType { get; init; }

    /// <summary>
    /// The source table name (explicit or derived from source entity).
    /// </summary>
    public string? SourceTable { get; init; }

    /// <summary>
    /// The projection expression for SELECT clause generation.
    /// </summary>
    public LambdaExpression? ProjectionExpression { get; init; }

    /// <summary>
    /// The parameter definitions for the view.
    /// Key: parameter name, Value: (CLR type, optional ClickHouse type override)
    /// </summary>
    public Dictionary<string, ParameterDefinition>? Parameters { get; init; }

    /// <summary>
    /// The WHERE clause expressions with parameter accessors.
    /// </summary>
    public List<LambdaExpression>? WhereExpressions { get; init; }
}

/// <summary>
/// Definition of a parameterized view parameter.
/// </summary>
public sealed class ParameterDefinition
{
    /// <summary>
    /// Creates a new parameter definition.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="clrType">The CLR type of the parameter.</param>
    /// <param name="clickHouseType">Optional explicit ClickHouse type name.</param>
    public ParameterDefinition(string name, Type clrType, string? clickHouseType = null)
    {
        Name = name;
        ClrType = clrType;
        ClickHouseType = clickHouseType;
    }

    /// <summary>
    /// The parameter name (used in the view definition).
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// The CLR type of the parameter.
    /// </summary>
    public Type ClrType { get; }

    /// <summary>
    /// Optional explicit ClickHouse type name.
    /// If null, the type is inferred from ClrType.
    /// </summary>
    public string? ClickHouseType { get; }
}
