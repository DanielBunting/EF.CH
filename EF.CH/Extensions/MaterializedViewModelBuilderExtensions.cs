using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Entry point for declaring ClickHouse materialized views via the fluent
/// model-level builder. The MV target entity must be declared first via
/// <c>modelBuilder.Entity&lt;TTarget&gt;(...)</c> with its engine/schema; the
/// MV configuration is then attached via this builder.
/// </summary>
public static class MaterializedViewModelBuilderExtensions
{
    /// <summary>
    /// Begins fluent configuration of a materialized view targeting
    /// <typeparamref name="TTarget"/>. The target entity must already be
    /// declared in the model (with its engine + schema) — the builder attaches
    /// MV annotations only.
    /// </summary>
    /// <example>
    /// <code>
    /// modelBuilder.Entity&lt;DailySummary&gt;(e =&gt;
    /// {
    ///     e.UseSummingMergeTree(x =&gt; new { x.Date, x.ProductId });
    /// });
    /// modelBuilder.MaterializedView&lt;DailySummary&gt;()
    ///     .From&lt;Order&gt;()
    ///     .DefinedAs(orders =&gt; orders
    ///         .GroupBy(o =&gt; new { Date = o.OrderDate.Date, o.ProductId })
    ///         .Select(g =&gt; new DailySummary { … }))
    ///     .Populate();
    /// </code>
    /// </example>
    public static MaterializedViewSourceBuilder<TTarget> MaterializedView<TTarget>(
        this ModelBuilder modelBuilder)
        where TTarget : class
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        var entity = modelBuilder.Model.FindEntityType(typeof(TTarget))
            ?? throw new InvalidOperationException(
                $"Materialized view target entity '{typeof(TTarget).Name}' is not declared in the model. " +
                $"Declare it first via modelBuilder.Entity<{typeof(TTarget).Name}>(...) " +
                $"with its engine and schema, then attach the MV configuration.");

        var spec = new MaterializedViewSpec
        {
            Target = entity,
            Model = modelBuilder.Model,
        };

        return new MaterializedViewSourceBuilder<TTarget>(spec);
    }
}
