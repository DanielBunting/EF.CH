using System.Text.RegularExpressions;

namespace EF.CH.Scaffolding.Internal;

/// <summary>
/// Parses ClickHouse MergeTree engine metadata from system.tables columns.
/// </summary>
public partial class ClickHouseEngineParser
{
    /// <summary>
    /// Parses engine metadata from system.tables columns.
    /// </summary>
    /// <param name="engine">The engine name (e.g., "MergeTree", "ReplacingMergeTree")</param>
    /// <param name="engineFull">The full engine definition with parameters (e.g., "ReplacingMergeTree(version)")</param>
    /// <param name="sortingKey">The sorting_key column value</param>
    /// <param name="partitionKey">The partition_key column value</param>
    /// <param name="primaryKey">The primary_key column value</param>
    /// <param name="samplingKey">The sampling_key column value</param>
    /// <returns>Parsed engine metadata</returns>
    public EngineMetadata Parse(
        string engine,
        string engineFull,
        string? sortingKey,
        string? partitionKey,
        string? primaryKey,
        string? samplingKey)
    {
        var metadata = new EngineMetadata
        {
            EngineName = engine,
            OrderBy = ParseKeyColumns(sortingKey),
            PartitionBy = string.IsNullOrWhiteSpace(partitionKey) ? null : partitionKey.Trim(),
            PrimaryKey = ParseKeyColumns(primaryKey),
            SampleBy = string.IsNullOrWhiteSpace(samplingKey) ? null : samplingKey.Trim()
        };

        // Extract engine parameters (version/sign columns) from engine_full
        ExtractEngineParameters(engine, engineFull, metadata);

        return metadata;
    }

    /// <summary>
    /// Extracts engine-specific parameters like version column and sign column.
    /// </summary>
    private void ExtractEngineParameters(string engine, string engineFull, EngineMetadata metadata)
    {
        // Pattern: EngineName(param1, param2, ...) possibly followed by ORDER BY, PARTITION BY, etc.
        // The engine_full from ClickHouse looks like: "ReplacingMergeTree(version) ORDER BY id"
        var match = EngineParametersRegex().Match(engineFull);
        if (!match.Success)
        {
            return;
        }

        var paramsContent = match.Groups[1].Value;

        // Handle empty parameters (e.g., "MergeTree()")
        if (string.IsNullOrWhiteSpace(paramsContent))
        {
            return;
        }
        var parameters = ParseParameters(paramsContent);

        switch (engine)
        {
            case "ReplacingMergeTree" when parameters.Length >= 1 && !string.IsNullOrEmpty(parameters[0]):
                metadata.VersionColumn = parameters[0];
                break;

            case "CollapsingMergeTree" when parameters.Length >= 1:
                metadata.SignColumn = parameters[0];
                break;

            case "VersionedCollapsingMergeTree" when parameters.Length >= 2:
                metadata.SignColumn = parameters[0];
                metadata.VersionColumn = parameters[1];
                break;

            case "SummingMergeTree" when parameters.Length >= 1 && !string.IsNullOrEmpty(parameters[0]):
                // SummingMergeTree can have optional columns to sum
                metadata.SumColumns = parameters;
                break;

            case "GraphiteMergeTree" when parameters.Length >= 1:
                metadata.GraphiteConfigSection = parameters[0];
                break;
        }
    }

    /// <summary>
    /// Parses key columns from expressions like "(col1, col2)" or "col1, col2" or just "col1".
    /// </summary>
    private static string[] ParseKeyColumns(string? keyExpression)
    {
        if (string.IsNullOrWhiteSpace(keyExpression))
        {
            return [];
        }

        // Handle tuple format: (col1, col2) or just col1
        var trimmed = keyExpression.Trim().TrimStart('(').TrimEnd(')');

        return trimmed
            .Split(',')
            .Select(c => c.Trim().Trim('"', '\'', '`'))
            .Where(c => !string.IsNullOrEmpty(c))
            .ToArray();
    }

    /// <summary>
    /// Parses parameters from within parentheses, handling quoted values.
    /// </summary>
    private static string[] ParseParameters(string content)
    {
        var parameters = new List<string>();
        var current = new System.Text.StringBuilder();
        var depth = 0;
        var inQuote = false;
        var quoteChar = '\0';

        foreach (var ch in content)
        {
            if (!inQuote && (ch == '\'' || ch == '"'))
            {
                inQuote = true;
                quoteChar = ch;
            }
            else if (inQuote && ch == quoteChar)
            {
                inQuote = false;
            }
            else if (!inQuote && (ch == '(' || ch == '['))
            {
                depth++;
                current.Append(ch);
            }
            else if (!inQuote && (ch == ')' || ch == ']'))
            {
                depth--;
                current.Append(ch);
            }
            else if (!inQuote && ch == ',' && depth == 0)
            {
                parameters.Add(current.ToString().Trim().Trim('"', '\'', '`'));
                current.Clear();
            }
            else
            {
                current.Append(ch);
            }
        }

        if (current.Length > 0)
        {
            parameters.Add(current.ToString().Trim().Trim('"', '\'', '`'));
        }

        return parameters.ToArray();
    }

    // Match engine name followed by parameters in parentheses
    // Captures content inside first set of parentheses, allows trailing content
    // Examples: "ReplacingMergeTree(version)", "CollapsingMergeTree(Sign) ORDER BY id"
    [GeneratedRegex(@"^\w+\(([^)]*)\)")]
    private static partial Regex EngineParametersRegex();
}

/// <summary>
/// Represents parsed MergeTree engine metadata.
/// </summary>
public class EngineMetadata
{
    /// <summary>
    /// The engine name (e.g., "MergeTree", "ReplacingMergeTree").
    /// </summary>
    public string EngineName { get; set; } = "MergeTree";

    /// <summary>
    /// ORDER BY columns.
    /// </summary>
    public string[] OrderBy { get; set; } = [];

    /// <summary>
    /// PARTITION BY expression (raw string, may include functions like toYYYYMM).
    /// </summary>
    public string? PartitionBy { get; set; }

    /// <summary>
    /// PRIMARY KEY columns (if different from ORDER BY).
    /// </summary>
    public string[]? PrimaryKey { get; set; }

    /// <summary>
    /// SAMPLE BY expression.
    /// </summary>
    public string? SampleBy { get; set; }

    /// <summary>
    /// Version column for ReplacingMergeTree or VersionedCollapsingMergeTree.
    /// </summary>
    public string? VersionColumn { get; set; }

    /// <summary>
    /// Sign column for CollapsingMergeTree or VersionedCollapsingMergeTree.
    /// </summary>
    public string? SignColumn { get; set; }

    /// <summary>
    /// Columns to sum for SummingMergeTree.
    /// </summary>
    public string[]? SumColumns { get; set; }

    /// <summary>
    /// Config section for GraphiteMergeTree.
    /// </summary>
    public string? GraphiteConfigSection { get; set; }
}
