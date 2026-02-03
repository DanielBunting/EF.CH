namespace EF.CH.QueryProfiling;

/// <summary>
/// Represents the result of an EXPLAIN query execution.
/// </summary>
public sealed class ExplainResult
{
    /// <summary>
    /// Gets the type of EXPLAIN that was executed.
    /// </summary>
    public ExplainType Type { get; init; }

    /// <summary>
    /// Gets the raw output lines from the EXPLAIN query.
    /// </summary>
    public IReadOnlyList<string> Output { get; init; } = Array.Empty<string>();

    /// <summary>
    /// Gets the original SQL query (without EXPLAIN prefix).
    /// </summary>
    public string OriginalSql { get; init; } = string.Empty;

    /// <summary>
    /// Gets the full EXPLAIN SQL that was executed.
    /// </summary>
    public string ExplainSql { get; init; } = string.Empty;

    /// <summary>
    /// Gets the time elapsed to execute the EXPLAIN query.
    /// </summary>
    public TimeSpan Elapsed { get; init; }

    /// <summary>
    /// Gets the JSON output when <see cref="ExplainOptions.Json"/> was enabled.
    /// Returns null if JSON output was not requested.
    /// </summary>
    public string? JsonOutput { get; init; }

    /// <summary>
    /// Gets the formatted output as a single string with lines joined by newlines.
    /// </summary>
    public string FormattedOutput => string.Join(Environment.NewLine, Output);

    /// <summary>
    /// Returns a string representation of the EXPLAIN result.
    /// </summary>
    public override string ToString() => FormattedOutput;
}
