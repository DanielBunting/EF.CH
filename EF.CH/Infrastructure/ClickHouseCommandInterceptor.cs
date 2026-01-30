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
    /// </summary>
    private static bool IsReadOperation(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            return false;
        }

        var trimmed = sql.TrimStart();

        // SELECT queries and CTEs (WITH ... SELECT) are read operations
        return trimmed.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("WITH", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("SHOW", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("DESCRIBE", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("EXPLAIN", StringComparison.OrdinalIgnoreCase);
    }
}
