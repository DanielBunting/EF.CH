using System.Linq.Expressions;

namespace EF.CH.Views;

/// <summary>
/// Fluent configuration builder for plain ClickHouse views.
/// </summary>
/// <typeparam name="TView">The view result entity type.</typeparam>
/// <typeparam name="TSource">The source entity type.</typeparam>
/// <remarks>
/// <para>
/// Provides a type-safe way to configure plain (non-parameterized, non-materialized)
/// views without writing raw SQL. The configuration is used to generate CREATE VIEW DDL.
/// </para>
/// <para>
/// Example usage:
/// <code>
/// modelBuilder.Entity&lt;ActiveUserView&gt;(entity =>
/// {
///     entity.AsView&lt;ActiveUserView, User&gt;(cfg => cfg
///         .FromTable()
///         .Select(u => new ActiveUserView
///         {
///             UserId = u.UserId,
///             Name = u.Name,
///             LastSeen = u.LastSeen
///         })
///         .Where(u => u.IsActive)
///         .OrReplace());
/// });
/// </code>
/// </para>
/// </remarks>
public class ViewConfiguration<TView, TSource>
    where TView : class
    where TSource : class
{
    private string? _viewName;
    private string? _sourceTable;
    private bool _useSourceEntity = true;
    private LambdaExpression? _projectionExpression;
    private readonly List<LambdaExpression> _whereExpressions = new();
    private bool _ifNotExists;
    private bool _orReplace;
    private string? _onCluster;
    private string? _schema;

    /// <summary>
    /// Creates a new configuration builder.
    /// </summary>
    public ViewConfiguration()
    {
    }

    /// <summary>
    /// Configures the view to select from the source entity's table.
    /// </summary>
    public ViewConfiguration<TView, TSource> FromTable()
    {
        _useSourceEntity = true;
        _sourceTable = null;
        return this;
    }

    /// <summary>
    /// Configures the view to select from an explicit table name.
    /// </summary>
    public ViewConfiguration<TView, TSource> FromTable(string tableName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        _useSourceEntity = false;
        _sourceTable = tableName;
        return this;
    }

    /// <summary>
    /// Configures the SELECT clause using a projection expression.
    /// </summary>
    public ViewConfiguration<TView, TSource> Select(Expression<Func<TSource, TView>> projection)
    {
        ArgumentNullException.ThrowIfNull(projection);
        _projectionExpression = projection;
        return this;
    }

    /// <summary>
    /// Adds a WHERE clause condition.
    /// </summary>
    public ViewConfiguration<TView, TSource> Where(Expression<Func<TSource, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        _whereExpressions.Add(predicate);
        return this;
    }

    /// <summary>
    /// Sets an explicit view name. If not specified, the entity's view name
    /// (or its snake-cased CLR type name) is used.
    /// </summary>
    public ViewConfiguration<TView, TSource> HasName(string viewName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);
        _viewName = viewName;
        return this;
    }

    /// <summary>
    /// Emit IF NOT EXISTS in the CREATE VIEW DDL. Mutually exclusive with <see cref="OrReplace"/>.
    /// </summary>
    public ViewConfiguration<TView, TSource> IfNotExists()
    {
        _ifNotExists = true;
        return this;
    }

    /// <summary>
    /// Emit OR REPLACE in the CREATE VIEW DDL. Mutually exclusive with <see cref="IfNotExists"/>.
    /// </summary>
    public ViewConfiguration<TView, TSource> OrReplace()
    {
        _orReplace = true;
        return this;
    }

    /// <summary>
    /// Adds an ON CLUSTER clause to the DDL.
    /// </summary>
    public ViewConfiguration<TView, TSource> OnCluster(string clusterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clusterName);
        _onCluster = clusterName;
        return this;
    }

    /// <summary>
    /// Sets a schema (database) qualifier for the view name.
    /// </summary>
    public ViewConfiguration<TView, TSource> InSchema(string schema)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        _schema = schema;
        return this;
    }

    internal string? ViewName => _viewName;
    internal string? SourceTable => _sourceTable;
    internal bool UseSourceEntity => _useSourceEntity;
    internal Type SourceType => typeof(TSource);
    internal Type ViewType => typeof(TView);
    internal LambdaExpression? ProjectionExpression => _projectionExpression;
    internal IReadOnlyList<LambdaExpression> WhereExpressions => _whereExpressions;
    internal bool IsIfNotExists => _ifNotExists;
    internal bool IsOrReplace => _orReplace;
    internal string? Cluster => _onCluster;
    internal string? Schema => _schema;

    internal IEnumerable<string> Validate()
    {
        if (_projectionExpression == null)
        {
            yield return "Select() must be called to define the projection.";
        }

        if (_ifNotExists && _orReplace)
        {
            yield return "IfNotExists() and OrReplace() are mutually exclusive in ClickHouse CREATE VIEW.";
        }
    }

    internal ViewMetadataBase BuildMetadata(string fallbackViewName)
    {
        var errors = Validate().ToList();
        if (errors.Count > 0)
        {
            throw new InvalidOperationException(
                $"Invalid view configuration:\n- {string.Join("\n- ", errors)}");
        }

        return new ViewMetadataBase
        {
            ViewName = _viewName ?? fallbackViewName,
            ResultType = typeof(TView),
            SourceType = typeof(TSource),
            SourceTable = _sourceTable,
            ProjectionExpression = _projectionExpression,
            WhereExpressions = new List<LambdaExpression>(_whereExpressions),
            IfNotExists = _ifNotExists,
            OrReplace = _orReplace,
            OnCluster = _onCluster,
            Schema = _schema
        };
    }
}
