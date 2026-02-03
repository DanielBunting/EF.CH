using System.Linq.Expressions;

namespace EF.CH.ParameterizedViews;

/// <summary>
/// Fluent configuration builder for parameterized views.
/// </summary>
/// <typeparam name="TView">The view result entity type.</typeparam>
/// <typeparam name="TSource">The source entity type.</typeparam>
/// <remarks>
/// <para>
/// This builder provides a type-safe way to configure parameterized views
/// instead of writing raw SQL. The configuration is used to generate
/// CREATE VIEW DDL statements.
/// </para>
/// <para>
/// Example usage:
/// <code>
/// modelBuilder.Entity&lt;UserEventView&gt;(entity =>
/// {
///     entity.AsParameterizedView&lt;UserEventView, Event&gt;(cfg => cfg
///         .FromTable()
///         .Select(e => new UserEventView
///         {
///             EventId = e.EventId,
///             EventType = e.EventType,
///             UserId = e.UserId,
///             Timestamp = e.Timestamp
///         })
///         .Parameter&lt;ulong&gt;("user_id")
///         .Parameter&lt;DateTime&gt;("start_date")
///         .Where((e, p) => e.UserId == p.Get&lt;ulong&gt;("user_id"))
///         .Where((e, p) => e.Timestamp >= p.Get&lt;DateTime&gt;("start_date")));
/// });
/// </code>
/// </para>
/// </remarks>
public class ParameterizedViewConfiguration<TView, TSource>
    where TView : class
    where TSource : class
{
    private string? _viewName;
    private string? _sourceTable;
    private bool _useSourceEntity = true;
    private LambdaExpression? _projectionExpression;
    private readonly Dictionary<string, ParameterDefinition> _parameters = new();
    private readonly List<LambdaExpression> _whereExpressions = new();

    /// <summary>
    /// Creates a new configuration builder.
    /// </summary>
    public ParameterizedViewConfiguration()
    {
    }

    /// <summary>
    /// Configures the view to select from the source entity's table.
    /// </summary>
    /// <returns>This builder for chaining.</returns>
    /// <remarks>
    /// The table name will be resolved from the <typeparamref name="TSource"/> entity configuration.
    /// </remarks>
    public ParameterizedViewConfiguration<TView, TSource> FromTable()
    {
        _useSourceEntity = true;
        _sourceTable = null;
        return this;
    }

    /// <summary>
    /// Configures the view to select from an explicit table name.
    /// </summary>
    /// <param name="tableName">The source table name.</param>
    /// <returns>This builder for chaining.</returns>
    public ParameterizedViewConfiguration<TView, TSource> FromTable(string tableName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        _useSourceEntity = false;
        _sourceTable = tableName;
        return this;
    }

    /// <summary>
    /// Configures the SELECT clause using a projection expression.
    /// </summary>
    /// <param name="projection">
    /// Expression that maps source entity to view result.
    /// Property assignments become SELECT column AS alias clauses.
    /// </param>
    /// <returns>This builder for chaining.</returns>
    /// <example>
    /// <code>
    /// .Select(e => new UserEventView
    /// {
    ///     EventId = e.EventId,        // SELECT event_id AS "EventId"
    ///     EventType = e.EventType,    // SELECT event_type AS "EventType"
    ///     UserId = e.UserId,          // SELECT user_id AS "UserId"
    ///     Timestamp = e.Timestamp     // SELECT timestamp AS "Timestamp"
    /// })
    /// </code>
    /// </example>
    public ParameterizedViewConfiguration<TView, TSource> Select(Expression<Func<TSource, TView>> projection)
    {
        ArgumentNullException.ThrowIfNull(projection);
        _projectionExpression = projection;
        return this;
    }

    /// <summary>
    /// Declares a parameter for the view.
    /// </summary>
    /// <typeparam name="T">The CLR type of the parameter.</typeparam>
    /// <param name="name">The parameter name (used in the view definition).</param>
    /// <returns>This builder for chaining.</returns>
    /// <remarks>
    /// The ClickHouse type is automatically inferred from the CLR type:
    /// <list type="bullet">
    /// <item><c>ulong</c> → UInt64</item>
    /// <item><c>long</c> → Int64</item>
    /// <item><c>DateTime</c> → DateTime</item>
    /// <item><c>string</c> → String</item>
    /// <item>etc.</item>
    /// </list>
    /// </remarks>
    public ParameterizedViewConfiguration<TView, TSource> Parameter<T>(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        _parameters[name] = new ParameterDefinition(name, typeof(T), null);
        return this;
    }

    /// <summary>
    /// Declares a parameter for the view with an explicit ClickHouse type.
    /// </summary>
    /// <typeparam name="T">The CLR type of the parameter.</typeparam>
    /// <param name="name">The parameter name (used in the view definition).</param>
    /// <param name="clickHouseType">The explicit ClickHouse type name (e.g., "UInt64", "DateTime64(3)").</param>
    /// <returns>This builder for chaining.</returns>
    public ParameterizedViewConfiguration<TView, TSource> Parameter<T>(string name, string clickHouseType)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(clickHouseType);
        _parameters[name] = new ParameterDefinition(name, typeof(T), clickHouseType);
        return this;
    }

    /// <summary>
    /// Adds a WHERE clause condition using source entity and parameter accessor.
    /// </summary>
    /// <param name="predicate">
    /// A predicate expression that uses the source entity and parameter accessor.
    /// Use <c>p.Get&lt;T&gt;("name")</c> to reference parameters.
    /// </param>
    /// <returns>This builder for chaining.</returns>
    /// <example>
    /// <code>
    /// .Where((e, p) => e.UserId == p.Get&lt;ulong&gt;("user_id"))
    /// .Where((e, p) => e.Timestamp >= p.Get&lt;DateTime&gt;("start_date"))
    /// </code>
    /// </example>
    public ParameterizedViewConfiguration<TView, TSource> Where(
        Expression<Func<TSource, IParameterAccessor, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        _whereExpressions.Add(predicate);
        return this;
    }

    /// <summary>
    /// Sets an explicit view name.
    /// </summary>
    /// <param name="viewName">The view name in ClickHouse.</param>
    /// <returns>This builder for chaining.</returns>
    /// <remarks>
    /// If not specified, the view name from <c>HasParameterizedView()</c> is used.
    /// </remarks>
    public ParameterizedViewConfiguration<TView, TSource> HasName(string viewName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);
        _viewName = viewName;
        return this;
    }

    /// <summary>
    /// Gets the configured view name, or null if using the entity configuration.
    /// </summary>
    internal string? ViewName => _viewName;

    /// <summary>
    /// Gets the configured source table name, or null if using source entity.
    /// </summary>
    internal string? SourceTable => _sourceTable;

    /// <summary>
    /// Gets whether to use the source entity type for table resolution.
    /// </summary>
    internal bool UseSourceEntity => _useSourceEntity;

    /// <summary>
    /// Gets the source entity type.
    /// </summary>
    internal Type SourceType => typeof(TSource);

    /// <summary>
    /// Gets the view result type.
    /// </summary>
    internal Type ViewType => typeof(TView);

    /// <summary>
    /// Gets the projection expression.
    /// </summary>
    internal LambdaExpression? ProjectionExpression => _projectionExpression;

    /// <summary>
    /// Gets the parameter definitions.
    /// </summary>
    internal IReadOnlyDictionary<string, ParameterDefinition> Parameters => _parameters;

    /// <summary>
    /// Gets the WHERE clause expressions.
    /// </summary>
    internal IReadOnlyList<LambdaExpression> WhereExpressions => _whereExpressions;

    /// <summary>
    /// Validates the configuration and returns any errors.
    /// </summary>
    internal IEnumerable<string> Validate()
    {
        if (_projectionExpression == null)
        {
            yield return "Select() must be called to define the projection.";
        }

        if (_parameters.Count == 0)
        {
            yield return "At least one Parameter() must be defined.";
        }

        // Validate that all parameters referenced in Where clauses are defined
        foreach (var whereExpr in _whereExpressions)
        {
            var visitor = new ParameterReferenceVisitor();
            visitor.Visit(whereExpr);

            foreach (var paramName in visitor.ReferencedParameters)
            {
                if (!_parameters.ContainsKey(paramName))
                {
                    yield return $"Parameter '{paramName}' referenced in Where clause but not defined. Use Parameter<T>(\"{paramName}\") first.";
                }
            }
        }
    }

    /// <summary>
    /// Builds the metadata for this configuration.
    /// </summary>
    internal ParameterizedViewMetadataBase BuildMetadata(string fallbackViewName)
    {
        var errors = Validate().ToList();
        if (errors.Count > 0)
        {
            throw new InvalidOperationException(
                $"Invalid parameterized view configuration:\n- {string.Join("\n- ", errors)}");
        }

        return new ParameterizedViewMetadataBase
        {
            ViewName = _viewName ?? fallbackViewName,
            ResultType = typeof(TView),
            SourceType = typeof(TSource),
            SourceTable = _sourceTable,
            ProjectionExpression = _projectionExpression,
            Parameters = new Dictionary<string, ParameterDefinition>(_parameters),
            WhereExpressions = new List<LambdaExpression>(_whereExpressions)
        };
    }
}

/// <summary>
/// Visitor that collects parameter names referenced in Where clause expressions.
/// </summary>
internal sealed class ParameterReferenceVisitor : ExpressionVisitor
{
    private readonly HashSet<string> _referencedParameters = new();

    /// <summary>
    /// Gets the parameter names referenced in the visited expression.
    /// </summary>
    public IReadOnlySet<string> ReferencedParameters => _referencedParameters;

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Look for IParameterAccessor.Get<T>("paramName") calls
        if (node.Method.Name == nameof(IParameterAccessor.Get) &&
            node.Method.DeclaringType == typeof(IParameterAccessor) &&
            node.Arguments.Count == 1 &&
            node.Arguments[0] is ConstantExpression constantExpr &&
            constantExpr.Value is string paramName)
        {
            _referencedParameters.Add(paramName);
        }

        return base.VisitMethodCall(node);
    }
}
