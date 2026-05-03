using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace EF.CH.Infrastructure;

/// <summary>
/// Intercepts database commands to route them to the appropriate read/write endpoint.
/// </summary>
/// <remarks>
/// This interceptor examines command text to determine if it's a read (SELECT) or write
/// (INSERT, ALTER, etc.) operation and sets the active endpoint on the routing connection
/// accordingly. SELECT and WITH (CTE) queries go to read endpoints while all other
/// operations go to the write endpoint.
/// </remarks>
public class ClickHouseCommandInterceptor : DbCommandInterceptor
{
    /// <inheritdoc />
    public override InterceptionResult<DbDataReader> ReaderExecuting(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<DbDataReader> result)
    {
        SetActiveEndpoint(eventData.Context, command.CommandText);
        return base.ReaderExecuting(command, eventData, result);
    }

    /// <inheritdoc />
    public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<DbDataReader> result,
        CancellationToken cancellationToken = default)
    {
        SetActiveEndpoint(eventData.Context, command.CommandText);
        return base.ReaderExecutingAsync(command, eventData, result, cancellationToken);
    }

    /// <inheritdoc />
    public override InterceptionResult<int> NonQueryExecuting(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<int> result)
    {
        // Non-query operations (INSERT, UPDATE, DELETE, ALTER) always go to write endpoint
        SetActiveEndpoint(eventData.Context, EndpointType.Write);
        return base.NonQueryExecuting(command, eventData, result);
    }

    /// <inheritdoc />
    public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        SetActiveEndpoint(eventData.Context, EndpointType.Write);
        return base.NonQueryExecutingAsync(command, eventData, result, cancellationToken);
    }

    /// <inheritdoc />
    public override InterceptionResult<object> ScalarExecuting(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<object> result)
    {
        SetActiveEndpoint(eventData.Context, command.CommandText);
        return base.ScalarExecuting(command, eventData, result);
    }

    /// <inheritdoc />
    public override ValueTask<InterceptionResult<object>> ScalarExecutingAsync(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<object> result,
        CancellationToken cancellationToken = default)
    {
        SetActiveEndpoint(eventData.Context, command.CommandText);
        return base.ScalarExecutingAsync(command, eventData, result, cancellationToken);
    }

    private static void SetActiveEndpoint(Microsoft.EntityFrameworkCore.DbContext? context, string commandText)
    {
        var endpointType = IsReadOperation(commandText) ? EndpointType.Read : EndpointType.Write;
        SetActiveEndpoint(context, endpointType);
    }

    private static void SetActiveEndpoint(Microsoft.EntityFrameworkCore.DbContext? context, EndpointType endpointType)
    {
        if (context == null)
        {
            return;
        }

        // Get the connection from the context
        var connection = context.Database.GetDbConnection();

        // Check if it's a routing connection
        if (connection is ClickHouseRoutingConnection routingConnection)
        {
            routingConnection.ActiveEndpoint = endpointType;
        }
    }

    /// <summary>
    /// Determines if a SQL command is a read operation.
    /// For <c>WITH</c>-led queries, looks past the CTE block(s) to find the
    /// actual statement verb — <c>WITH x AS (SELECT 1) INSERT …</c> must
    /// classify as a write, not a read.
    /// </summary>
    private static bool IsReadOperation(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            return false;
        }

        var span = sql.AsSpan().TrimStart();
        span = SkipLeadingComments(span);

        // For WITH-led queries, advance past every `<name> AS ( <paren-balanced body> )`
        // CTE definition (comma-separated) so the verb-check below sees the
        // actual statement verb rather than the literal "WITH".
        if (StartsWithWord(span, "WITH"))
        {
            span = SkipCteBlock(span["WITH".Length..]);
        }

        return StartsWithWord(span, "SELECT")
            || StartsWithWord(span, "SHOW")
            || StartsWithWord(span, "DESCRIBE")
            || StartsWithWord(span, "DESC")
            || StartsWithWord(span, "EXPLAIN");
    }

    private static ReadOnlySpan<char> SkipCteBlock(ReadOnlySpan<char> span)
    {
        // After "WITH": consume one-or-more `<name> AS ( ... )` separated by commas.
        // Stops at the first character that isn't whitespace, an identifier, AS,
        // a parenthesised body, or a comma — that's where the actual statement
        // verb begins.
        while (true)
        {
            span = span.TrimStart();
            span = SkipLeadingComments(span);
            // CTE name (identifier or backtick / quoted ident — accept anything until whitespace or paren or AS).
            var nameEnd = 0;
            while (nameEnd < span.Length
                && !char.IsWhiteSpace(span[nameEnd])
                && span[nameEnd] != '('
                && span[nameEnd] != ',')
            {
                nameEnd++;
            }
            if (nameEnd == 0)
            {
                return span;
            }
            span = span[nameEnd..].TrimStart();
            span = SkipLeadingComments(span);

            // Optional AS keyword (CH allows it to be omitted in some forms; tolerate either way).
            if (StartsWithWord(span, "AS"))
            {
                span = span["AS".Length..].TrimStart();
                span = SkipLeadingComments(span);
            }

            // CTE body: paren-balanced parenthesised group, OR a bare scalar expression.
            if (span.Length > 0 && span[0] == '(')
            {
                var depth = 0;
                var i = 0;
                for (; i < span.Length; i++)
                {
                    var c = span[i];
                    if (c == '(') depth++;
                    else if (c == ')')
                    {
                        depth--;
                        if (depth == 0) { i++; break; }
                    }
                }
                if (depth != 0)
                {
                    // Unbalanced parens — give up and return what we have; downstream
                    // verb-check will see whatever's left and likely classify as write.
                    return span;
                }
                span = span[i..].TrimStart();
            }
            else
            {
                // Bare scalar CTE (`WITH 42 AS x SELECT …`) — read up to the next
                // top-level comma or whitespace-bounded keyword.
                var depth = 0;
                var i = 0;
                for (; i < span.Length; i++)
                {
                    var c = span[i];
                    if (c == '(') depth++;
                    else if (c == ')') depth--;
                    else if (depth == 0 && c == ',') break;
                    else if (depth == 0 && char.IsWhiteSpace(c)) break;
                }
                span = span[i..].TrimStart();
            }
            span = SkipLeadingComments(span);

            if (span.Length > 0 && span[0] == ',')
            {
                span = span[1..]; // another CTE definition follows
                continue;
            }
            return span;
        }
    }

    private static ReadOnlySpan<char> SkipLeadingComments(ReadOnlySpan<char> span)
    {
        while (true)
        {
            span = span.TrimStart();
            if (span.StartsWith("--", StringComparison.Ordinal))
            {
                var nl = span.IndexOf('\n');
                span = nl < 0 ? ReadOnlySpan<char>.Empty : span[(nl + 1)..];
                continue;
            }
            if (span.StartsWith("/*", StringComparison.Ordinal))
            {
                var end = span.IndexOf("*/", StringComparison.Ordinal);
                span = end < 0 ? ReadOnlySpan<char>.Empty : span[(end + 2)..];
                continue;
            }
            return span;
        }
    }

    private static bool StartsWithWord(ReadOnlySpan<char> span, string word)
    {
        if (!span.StartsWith(word, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }
        if (span.Length == word.Length)
        {
            return true;
        }
        var next = span[word.Length];
        // Word boundary: the keyword must not run into another identifier character.
        return !(char.IsLetterOrDigit(next) || next == '_');
    }
}
