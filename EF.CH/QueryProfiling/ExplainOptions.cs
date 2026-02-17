namespace EF.CH.QueryProfiling;

/// <summary>
/// Options for configuring EXPLAIN query output.
/// </summary>
public sealed class ExplainOptions
{
    /// <summary>
    /// Gets or sets the type of EXPLAIN to execute.
    /// Default is <see cref="ExplainType.Plan"/>.
    /// </summary>
    public ExplainType Type { get; set; } = ExplainType.Plan;

    /// <summary>
    /// Gets or sets whether to output the result as JSON.
    /// Only applicable for <see cref="ExplainType.Plan"/>.
    /// </summary>
    public bool Json { get; set; }

    /// <summary>
    /// Gets or sets whether to show index usage information.
    /// Applicable for <see cref="ExplainType.Plan"/> and <see cref="ExplainType.Pipeline"/>.
    /// </summary>
    public bool Indexes { get; set; }

    /// <summary>
    /// Gets or sets whether to show detailed actions.
    /// Applicable for <see cref="ExplainType.Plan"/>.
    /// </summary>
    public bool Actions { get; set; }

    /// <summary>
    /// Gets or sets whether to show column headers in the output.
    /// Applicable for <see cref="ExplainType.Plan"/> and <see cref="ExplainType.Pipeline"/>.
    /// </summary>
    public bool Header { get; set; }

    /// <summary>
    /// Gets or sets whether to output the pipeline as a DOT graph.
    /// Only applicable for <see cref="ExplainType.Pipeline"/>.
    /// </summary>
    public bool Graph { get; set; }

    /// <summary>
    /// Gets or sets whether to show query tree optimization passes.
    /// Only applicable for <see cref="ExplainType.QueryTree"/>.
    /// </summary>
    public bool Passes { get; set; }

    /// <summary>
    /// Gets or sets whether to enable detailed description output.
    /// Applicable for <see cref="ExplainType.Plan"/> and <see cref="ExplainType.QueryTree"/>.
    /// </summary>
    public bool Description { get; set; }

    /// <summary>
    /// Gets or sets whether to compact output by removing redundant information.
    /// Applicable for <see cref="ExplainType.Pipeline"/>.
    /// </summary>
    public bool Compact { get; set; }
}
