using System.Data.Common;
using ClickHouse.Driver;
using ClickHouse.Driver.ADO;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace EF.CH.QueryProfiling;

/// <summary>
/// Captures the X-ClickHouse-Summary statistics from each query execution by reading
/// <see cref="ClickHouseCommand.QueryStats"/> after the reader runs, and exposes the
/// most recent value via a shared mutable container accessed through an
/// <see cref="AsyncLocal{T}"/>.
/// </summary>
/// <remarks>
/// AsyncLocal values written inside the interceptor's execution context do not
/// propagate back to the caller's context (changes flow down, not up). To return
/// data to the caller we publish a shared <see cref="StatsContainer"/> via AsyncLocal
/// in the caller's frame, and the interceptor mutates that container.
/// </remarks>
public sealed class ClickHouseQueryStatsInterceptor : DbCommandInterceptor
{
    private static readonly AsyncLocal<StatsContainer?> Current = new();

    /// <summary>
    /// Mutable holder for stats captured during a scoped operation.
    /// </summary>
    public sealed class StatsContainer
    {
        /// <summary>The most recently captured stats; set by the interceptor.</summary>
        public QueryStats? Value { get; internal set; }
    }

    /// <summary>
    /// Starts a stats-capture scope on the current async flow. The returned container
    /// is mutated by the interceptor whenever a <see cref="ClickHouseCommand"/> finishes
    /// executing on this flow. Call this immediately before the operation whose stats
    /// you want to capture.
    /// </summary>
    public static StatsContainer Begin()
    {
        var container = new StatsContainer();
        Current.Value = container;
        return container;
    }

    /// <inheritdoc />
    public override DbDataReader ReaderExecuted(
        DbCommand command,
        CommandExecutedEventData eventData,
        DbDataReader result)
    {
        Capture(command);
        return base.ReaderExecuted(command, eventData, result);
    }

    /// <inheritdoc />
    public override ValueTask<DbDataReader> ReaderExecutedAsync(
        DbCommand command,
        CommandExecutedEventData eventData,
        DbDataReader result,
        CancellationToken cancellationToken = default)
    {
        Capture(command);
        return base.ReaderExecutedAsync(command, eventData, result, cancellationToken);
    }

    private static void Capture(DbCommand command)
    {
        if (Current.Value is { } container
            && command is ClickHouseCommand chCommand
            && chCommand.QueryStats is { } stats)
        {
            container.Value = stats;
        }
    }
}
