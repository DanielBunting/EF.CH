namespace EF.CH.Diagnostics;

/// <summary>
/// Snapshot of a refreshable materialized view's state, sourced from
/// <c>system.view_refreshes</c>.
/// </summary>
public sealed record RefreshableViewStatus(
    string View,
    string? Status,
    DateTime? LastRefreshTime,
    DateTime? LastSuccessTime,
    DateTime? NextRefreshTime,
    string? ExceptionMessage,
    long Retry,
    double Progress,
    long ReadRows,
    long WrittenRows);
