using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for querying ClickHouse parameterized views.
/// </summary>
/// <remarks>
/// <para>
/// Parameterized views in ClickHouse allow you to create views with parameters that are
/// substituted at query time. They use the syntax <c>{name:Type}</c> in the view definition.
/// </para>
/// <para>
/// Example view definition:
/// <code>
/// CREATE VIEW user_events_view AS
/// SELECT * FROM events
/// WHERE user_id = {user_id:UInt64}
///   AND timestamp >= {start_date:DateTime}
/// </code>
/// </para>
/// <para>
/// Query syntax:
/// <code>
/// SELECT * FROM user_events_view(user_id = 123, start_date = '2024-01-01 00:00:00')
/// </code>
/// </para>
/// </remarks>
public static class ClickHouseParameterizedViewExtensions
{
    /// <summary>
    /// Queries a parameterized view with the specified parameters.
    /// </summary>
    /// <typeparam name="TResult">The result entity type (must be configured as keyless).</typeparam>
    /// <param name="context">The DbContext.</param>
    /// <param name="viewName">The name of the parameterized view.</param>
    /// <param name="parameters">
    /// An anonymous object containing parameter names and values.
    /// Property names should match the parameter names in the view definition.
    /// </param>
    /// <returns>An IQueryable that can be further composed with LINQ.</returns>
    /// <example>
    /// <code>
    /// var results = context.FromParameterizedView&lt;EventView&gt;(
    ///     "user_events_view",
    ///     new { user_id = 123UL, start_date = new DateTime(2024, 1, 1) })
    ///     .Where(e => e.EventType == "click")
    ///     .OrderByDescending(e => e.Timestamp)
    ///     .ToListAsync();
    /// </code>
    /// </example>
    public static IQueryable<TResult> FromParameterizedView<TResult>(
        this DbContext context,
        string viewName,
        object parameters)
        where TResult : class
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);
        ArgumentNullException.ThrowIfNull(parameters);

        var sql = BuildParameterizedViewSql(viewName, parameters);
        return context.Set<TResult>().FromSqlRaw(sql);
    }

    /// <summary>
    /// Queries a parameterized view with the specified parameters using a dictionary.
    /// </summary>
    /// <typeparam name="TResult">The result entity type (must be configured as keyless).</typeparam>
    /// <param name="context">The DbContext.</param>
    /// <param name="viewName">The name of the parameterized view.</param>
    /// <param name="parameters">
    /// A dictionary containing parameter names and values.
    /// </param>
    /// <returns>An IQueryable that can be further composed with LINQ.</returns>
    /// <example>
    /// <code>
    /// var parameters = new Dictionary&lt;string, object?&gt;
    /// {
    ///     ["user_id"] = 123UL,
    ///     ["start_date"] = new DateTime(2024, 1, 1)
    /// };
    /// var results = context.FromParameterizedView&lt;EventView&gt;("user_events_view", parameters)
    ///     .ToListAsync();
    /// </code>
    /// </example>
    public static IQueryable<TResult> FromParameterizedView<TResult>(
        this DbContext context,
        string viewName,
        IDictionary<string, object?> parameters)
        where TResult : class
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);
        ArgumentNullException.ThrowIfNull(parameters);

        var sql = BuildParameterizedViewSql(viewName, parameters);
        return context.Set<TResult>().FromSqlRaw(sql);
    }

    /// <summary>
    /// Builds the SQL string for querying a parameterized view.
    /// </summary>
    internal static string BuildParameterizedViewSql(string viewName, object parameters)
    {
        var properties = parameters.GetType().GetProperties();
        var paramDict = new Dictionary<string, object?>();

        foreach (var prop in properties)
        {
            var name = ToSnakeCase(prop.Name);
            var value = prop.GetValue(parameters);
            paramDict[name] = value;
        }

        return BuildParameterizedViewSql(viewName, paramDict);
    }

    /// <summary>
    /// Builds the SQL string for querying a parameterized view.
    /// </summary>
    internal static string BuildParameterizedViewSql(string viewName, IDictionary<string, object?> parameters)
    {
        var sb = new StringBuilder();
        sb.Append("SELECT * FROM \"");
        sb.Append(EscapeIdentifier(viewName));
        sb.Append('"');

        if (parameters.Count > 0)
        {
            sb.Append('(');

            var first = true;
            foreach (var kvp in parameters)
            {
                if (!first)
                {
                    sb.Append(", ");
                }
                first = false;

                sb.Append(kvp.Key);
                sb.Append(" = ");
                sb.Append(FormatParameterValue(kvp.Value));
            }

            sb.Append(')');
        }

        return sb.ToString();
    }

    /// <summary>
    /// Formats a parameter value for use in the view call syntax.
    /// </summary>
    /// <param name="value">The value to format.</param>
    /// <returns>The formatted SQL literal.</returns>
    internal static string FormatParameterValue(object? value)
    {
        return value switch
        {
            null => "NULL",
            string s => $"'{EscapeString(s)}'",
            char c => $"'{EscapeString(c.ToString())}'",
            bool b => b ? "1" : "0",
            DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss}'",
            DateTimeOffset dto => $"'{dto.UtcDateTime:yyyy-MM-dd HH:mm:ss}'",
            DateOnly d => $"'{d:yyyy-MM-dd}'",
            TimeOnly t => $"'{t:HH:mm:ss}'",
            decimal dec => dec.ToString(CultureInfo.InvariantCulture),
            double dbl => dbl.ToString(CultureInfo.InvariantCulture),
            float flt => flt.ToString(CultureInfo.InvariantCulture),
            Guid g => $"'{g}'",
            byte[] bytes => FormatByteArray(bytes),
            Enum e => Convert.ToInt64(e).ToString(CultureInfo.InvariantCulture),
            _ when value.GetType().IsValueType => value.ToString() ?? "NULL",
            _ => $"'{EscapeString(value.ToString() ?? string.Empty)}'"
        };
    }

    /// <summary>
    /// Converts a PascalCase or camelCase property name to snake_case.
    /// </summary>
    /// <param name="name">The property name.</param>
    /// <returns>The snake_case version.</returns>
    internal static string ToSnakeCase(string name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return name;
        }

        // Already snake_case
        if (name.Contains('_'))
        {
            return name.ToLowerInvariant();
        }

        var sb = new StringBuilder();
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                if (i > 0)
                {
                    sb.Append('_');
                }
                sb.Append(char.ToLowerInvariant(c));
            }
            else
            {
                sb.Append(c);
            }
        }

        return sb.ToString();
    }

    /// <summary>
    /// Escapes a string value for use in SQL.
    /// </summary>
    private static string EscapeString(string value)
    {
        return value.Replace("\\", "\\\\").Replace("'", "\\'");
    }

    /// <summary>
    /// Escapes an identifier for use in SQL.
    /// </summary>
    private static string EscapeIdentifier(string identifier)
    {
        return identifier.Replace("\"", "\"\"");
    }

    /// <summary>
    /// Formats a byte array as a hex literal.
    /// </summary>
    private static string FormatByteArray(byte[] bytes)
    {
        return $"unhex('{Convert.ToHexString(bytes)}')";
    }
}
