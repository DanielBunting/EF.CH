using System.Globalization;
using System.Text;

namespace EF.CH.QueryProfiling;

/// <summary>
/// Represents execution statistics for a ClickHouse query.
/// </summary>
public sealed class QueryStatistics
{
    /// <summary>
    /// Gets the number of rows read during query execution.
    /// </summary>
    public long RowsRead { get; init; }

    /// <summary>
    /// Gets the number of bytes read during query execution.
    /// </summary>
    public long BytesRead { get; init; }

    /// <summary>
    /// Gets the query duration in milliseconds.
    /// </summary>
    public double QueryDurationMs { get; init; }

    /// <summary>
    /// Gets the memory usage in bytes during query execution.
    /// </summary>
    public long MemoryUsage { get; init; }

    /// <summary>
    /// Gets the peak memory usage in bytes during query execution.
    /// </summary>
    public long PeakMemoryUsage { get; init; }

    /// <summary>
    /// Gets a formatted summary of the query statistics.
    /// </summary>
    public string Summary
    {
        get
        {
            var sb = new StringBuilder();
            sb.Append(CultureInfo.InvariantCulture, $"Rows read: {RowsRead:N0}");
            sb.Append(CultureInfo.InvariantCulture, $", Bytes read: {FormatBytes(BytesRead)}");
            sb.Append(CultureInfo.InvariantCulture, $", Duration: {QueryDurationMs:F2}ms");
            sb.Append(CultureInfo.InvariantCulture, $", Memory: {FormatBytes(MemoryUsage)}");
            if (PeakMemoryUsage > 0)
            {
                sb.Append(CultureInfo.InvariantCulture, $", Peak: {FormatBytes(PeakMemoryUsage)}");
            }
            return sb.ToString();
        }
    }

    /// <summary>
    /// Returns a string representation of the query statistics.
    /// </summary>
    public override string ToString() => Summary;

    private static string FormatBytes(long bytes)
    {
        string[] suffixes = ["B", "KB", "MB", "GB", "TB"];
        var suffix = 0;
        var value = (double)bytes;

        while (value >= 1024 && suffix < suffixes.Length - 1)
        {
            value /= 1024;
            suffix++;
        }

        return string.Format(CultureInfo.InvariantCulture, "{0:F2} {1}", value, suffixes[suffix]);
    }
}
