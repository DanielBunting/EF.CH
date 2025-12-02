using Microsoft.Extensions.Logging;

namespace EF.CH.Diagnostics;

/// <summary>
/// Event IDs for ClickHouse-specific diagnostic events.
/// </summary>
/// <remarks>
/// ClickHouse provider uses event IDs starting at 35000 to avoid conflicts
/// with EF Core's built-in event IDs.
/// </remarks>
public static class ClickHouseEventId
{
    private const int BaseId = 35000;

    /// <summary>
    /// A single-row INSERT was detected. ClickHouse is optimized for batch inserts.
    /// </summary>
    public static readonly EventId SingleRowInsertWarning = new(BaseId + 1, nameof(SingleRowInsertWarning));
}
