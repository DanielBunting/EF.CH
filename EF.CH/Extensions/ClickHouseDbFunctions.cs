using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// ClickHouse-specific extension methods for <see cref="DbFunctions"/>.
/// </summary>
public static class ClickHouseDbFunctions
{
    /// <summary>
    /// Inserts a raw SQL expression into the query projection.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use this when you need to include ClickHouse-specific SQL expressions in your projections
    /// that cannot be expressed via LINQ, such as complex JSON operations, array functions,
    /// or specialized ClickHouse functions.
    /// </para>
    /// <para>
    /// <b>WARNING:</b> This method does not validate or escape the SQL. Ensure the SQL is safe
    /// and does not contain user-provided values without proper escaping.
    /// </para>
    /// </remarks>
    /// <typeparam name="TResult">The return type of the SQL expression.</typeparam>
    /// <param name="_">The DbFunctions instance (not used).</param>
    /// <param name="sql">The raw SQL expression.</param>
    /// <returns>This method throws at runtime - it's only meant to be translated to SQL.</returns>
    /// <exception cref="InvalidOperationException">Always thrown if called directly.</exception>
    /// <example>
    /// <code>
    /// var results = context.Events
    ///     .Select(e => new {
    ///         e.Id,
    ///         Source = EF.Functions.RawSql&lt;string&gt;("JSONExtractString(metadata, 'source')"),
    ///         Score = EF.Functions.RawSql&lt;int&gt;("toInt32(score * 100)")
    ///     })
    ///     .ToListAsync();
    /// </code>
    /// </example>
    public static TResult RawSql<TResult>(this DbFunctions _, string sql)
    {
        throw new InvalidOperationException(
            "EF.Functions.RawSql<TResult>(sql) can only be used in LINQ queries. " +
            "It cannot be called directly.");
    }

    /// <summary>
    /// Inserts a raw SQL expression with parameters into the query projection.
    /// Parameters are substituted using positional placeholders {0}, {1}, etc.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use this when you need to include ClickHouse-specific SQL expressions with parameter values.
    /// Parameters are properly escaped to prevent SQL injection.
    /// </para>
    /// <para>
    /// <b>NOTE:</b> The SQL template itself is not validated. Ensure the SQL is safe
    /// and only use the parameter substitution for values, not for SQL keywords or identifiers.
    /// </para>
    /// </remarks>
    /// <typeparam name="TResult">The return type of the SQL expression.</typeparam>
    /// <param name="_">The DbFunctions instance (not used).</param>
    /// <param name="sql">The raw SQL expression with {0}, {1}, etc. placeholders.</param>
    /// <param name="parameters">The parameter values to substitute.</param>
    /// <returns>This method throws at runtime - it's only meant to be translated to SQL.</returns>
    /// <exception cref="InvalidOperationException">Always thrown if called directly.</exception>
    /// <example>
    /// <code>
    /// var threshold = 100;
    /// var results = context.Events
    ///     .Select(e => new {
    ///         e.Id,
    ///         AboveThreshold = EF.Functions.RawSql&lt;bool&gt;("score > {0}", threshold)
    ///     })
    ///     .ToListAsync();
    /// </code>
    /// </example>
    public static TResult RawSql<TResult>(this DbFunctions _, string sql, params object[] parameters)
    {
        throw new InvalidOperationException(
            "EF.Functions.RawSql<TResult>(sql, parameters) can only be used in LINQ queries. " +
            "It cannot be called directly.");
    }
}
