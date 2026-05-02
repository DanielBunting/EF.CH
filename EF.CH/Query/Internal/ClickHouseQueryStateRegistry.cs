using System.Collections.Concurrent;

namespace EF.CH.Query.Internal;

/// <summary>
/// Process-wide registry that hands a per-query <see cref="ClickHouseQueryGenerationContext"/>
/// from <see cref="ClickHouseQueryTranslationPostprocessor"/> to
/// <see cref="ClickHouseQuerySqlGenerator"/>. The postprocessor calls
/// <see cref="Register"/> to stash the context and gets back a tag; it adds
/// that tag to <c>QueryCompilationContext.Tags</c>. EF Core's shaper-compiling
/// visitor then copies the compilation context's Tags onto the outer
/// <c>SelectExpression</c>. The SQL generator lifts the context via
/// <see cref="TryConsume"/> on its first <c>VisitSelect</c>.
/// <para>
/// Adding to <c>QueryCompilationContext.Tags</c> (rather than to the
/// <c>SelectExpression</c> directly) is load-bearing: the shaper-processing
/// visitor at
/// <c>EFCore.Relational/Query/RelationalShapedQueryCompilingExpressionVisitor.ShaperProcessingExpressionVisitor.cs:230</c>
/// calls <c>_selectExpression.ApplyTags(_tags)</c> which OVERWRITES the
/// SelectExpression's tags with the compilation context's. Tags applied
/// directly to the SelectExpression by the postprocessor would be clobbered.
/// </para>
/// </summary>
internal static class ClickHouseQueryStateRegistry
{
    internal const string TagPrefix = "ch-state:";

    private static readonly ConcurrentDictionary<Guid, ClickHouseQueryGenerationContext> _states = new();

    /// <summary>
    /// Stashes <paramref name="ctx"/> and returns the tag to attach to
    /// <c>QueryCompilationContext.Tags</c>. The SQL generator removes the
    /// entry when it consumes it (one-shot); a successful round-trip leaves
    /// the registry empty.
    /// </summary>
    public static string Register(ClickHouseQueryGenerationContext ctx)
    {
        var id = Guid.NewGuid();
        _states[id] = ctx;
        return TagPrefix + id.ToString("N");
    }

    /// <summary>
    /// Looks for a <c>ch-state:</c>-prefixed tag in <paramref name="tags"/>,
    /// removes-and-returns the matching context, and removes the tag from the
    /// set so it doesn't leak into the emitted SQL as a comment. Returns null
    /// when no tag is present (subqueries, non-CH-extended selects).
    /// </summary>
    public static ClickHouseQueryGenerationContext? TryConsume(ISet<string>? tags)
    {
        if (tags is null || tags.Count == 0) return null;
        string? matchedTag = null;
        ClickHouseQueryGenerationContext? matchedCtx = null;
        foreach (var t in tags)
        {
            if (t.Length <= TagPrefix.Length) continue;
            if (!t.StartsWith(TagPrefix, StringComparison.Ordinal)) continue;
            if (Guid.TryParseExact(t.AsSpan(TagPrefix.Length), "N", out var id)
                && _states.TryRemove(id, out var ctx))
            {
                matchedTag = t;
                matchedCtx = ctx;
                break;
            }
        }
        if (matchedTag != null)
        {
            tags.Remove(matchedTag);
        }
        return matchedCtx;
    }

    /// <summary>
    /// Drops a previously-registered entry without consuming it. Used to
    /// prevent registry leaks when the postprocessor registers state but the
    /// pipeline throws before the SQL generator runs (e.g. translation
    /// failure).
    /// </summary>
    public static void Discard(string tag)
    {
        if (tag.Length <= TagPrefix.Length) return;
        if (!tag.StartsWith(TagPrefix, StringComparison.Ordinal)) return;
        if (Guid.TryParseExact(tag.AsSpan(TagPrefix.Length), "N", out var id))
        {
            _states.TryRemove(id, out _);
        }
    }
}
