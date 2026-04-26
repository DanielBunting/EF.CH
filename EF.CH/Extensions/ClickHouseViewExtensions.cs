using System.Text;
using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for querying plain (non-parameterized, non-materialized) ClickHouse views.
/// </summary>
/// <remarks>
/// <para>
/// For entities mapped to a view via <c>HasView</c> / <c>AsView</c>, you can query through the
/// usual <c>context.Set&lt;T&gt;()</c> path. <see cref="FromView{TResult}(DbContext, string, string?)"/>
/// is useful when you want to point an entity at a different physical view at query time, or
/// query an entity that is otherwise unmapped.
/// </para>
/// </remarks>
public static class ClickHouseViewExtensions
{
    /// <summary>
    /// Queries a ClickHouse view by name.
    /// </summary>
    /// <typeparam name="TResult">The result entity type (must be configured as keyless or be a query type).</typeparam>
    /// <param name="context">The DbContext.</param>
    /// <param name="viewName">The view name in ClickHouse.</param>
    /// <param name="schema">Optional schema (database) qualifier.</param>
    /// <returns>An IQueryable that can be further composed with LINQ.</returns>
    /// <example>
    /// <code>
    /// var rows = await context
    ///     .FromView&lt;ActiveUserView&gt;("active_users")
    ///     .Where(u => u.LastSeen >= cutoff)
    ///     .OrderByDescending(u => u.LastSeen)
    ///     .ToListAsync();
    /// </code>
    /// </example>
    public static IQueryable<TResult> FromView<TResult>(
        this DbContext context,
        string viewName,
        string? schema = null)
        where TResult : class
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);

        var sql = BuildSelectSql(viewName, schema);
        return context.Set<TResult>().FromSqlRaw(sql);
    }

    internal static string BuildSelectSql(string viewName, string? schema)
    {
        var sb = new StringBuilder();
        sb.Append("SELECT * FROM ");
        if (!string.IsNullOrEmpty(schema))
        {
            sb.Append('"');
            sb.Append(EscapeIdentifier(schema));
            sb.Append("\".\"");
            sb.Append(EscapeIdentifier(viewName));
            sb.Append('"');
        }
        else
        {
            sb.Append('"');
            sb.Append(EscapeIdentifier(viewName));
            sb.Append('"');
        }
        return sb.ToString();
    }

    private static string EscapeIdentifier(string identifier)
        => identifier.Replace("\"", "\"\"");
}
