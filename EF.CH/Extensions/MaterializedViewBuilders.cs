using System.Linq.Expressions;
using EF.CH.Internal.Intervals;
using EF.CH.Metadata;
using EF.CH.Query.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EF.CH.Extensions;

// =============================================================================
//  Stage 0 — entry from ModelBuilder.MaterializedView<TTarget>()
// =============================================================================

/// <summary>
/// Initial stage of the materialized-view builder. The next call must be either
/// <see cref="From{T1}"/> for a typed LINQ-defined view or <see cref="FromTable"/>
/// for a raw-SQL view.
/// </summary>
public sealed class MaterializedViewSourceBuilder<TTarget>
    where TTarget : class
{
    private readonly MaterializedViewSpec _spec;

    internal MaterializedViewSourceBuilder(MaterializedViewSpec spec) => _spec = spec;

    /// <summary>
    /// Declares the <typeparamref name="T1"/> source as the materialized view's
    /// <strong>INSERT trigger</strong>: only writes into <typeparamref name="T1"/>
    /// cause this MV to fire. Tables added via <c>Join&lt;T&gt;</c> are looked up
    /// at trigger time; writes into them alone do not propagate to the view.
    /// This matches ClickHouse's materialized-view trigger semantics.
    /// </summary>
    /// <typeparam name="T1">The trigger source entity type.</typeparam>
    public MaterializedViewBuilder<TTarget, T1> From<T1>()
        where T1 : class
    {
        _spec.SourceTypes.Add(typeof(T1));
        return new MaterializedViewBuilder<TTarget, T1>(_spec);
    }

    /// <summary>
    /// Declares a raw source table by name (used together with
    /// <see cref="MaterializedViewRawBuilder{TTarget}.DefinedAsRaw"/>).
    /// </summary>
    public MaterializedViewRawBuilder<TTarget> FromTable(string sourceTableName)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceTableName);
        _spec.SourceTableOverride = sourceTableName;
        return new MaterializedViewRawBuilder<TTarget>(_spec);
    }
}

// =============================================================================
//  Stage 1 — single source
// =============================================================================

public sealed class MaterializedViewBuilder<TTarget, T1>
    where TTarget : class
    where T1 : class
{
    private readonly MaterializedViewSpec _spec;

    internal MaterializedViewBuilder(MaterializedViewSpec spec) => _spec = spec;

    /// <summary>
    /// Adds a joined source. The <see cref="MaterializedViewSourceBuilder{TTarget}.From{T1}"/>
    /// table remains the trigger; tables added via <c>Join</c> are looked up at trigger time.
    /// Multi-source MVs with <c>Join</c> but a lambda that doesn't actually reference the
    /// joined parameters do not synthesize CROSS JOINs — the user's lambda is the source of truth.
    /// </summary>
    public MaterializedViewBuilder<TTarget, T1, T2> Join<T2>()
        where T2 : class
    {
        _spec.SourceTypes.Add(typeof(T2));
        return new MaterializedViewBuilder<TTarget, T1, T2>(_spec);
    }

    /// <summary>
    /// Specifies the LINQ query that defines the materialized view body.
    /// </summary>
    public MaterializedViewConfig<TTarget> DefinedAs(
        Expression<Func<IQueryable<T1>, IQueryable<TTarget>>> query)
    {
        ArgumentNullException.ThrowIfNull(query);
        _spec.Query = query;
        return new MaterializedViewConfig<TTarget>(_spec);
    }
}

// =============================================================================
//  Stage 1 raw — FromTable("…")
// =============================================================================

public sealed class MaterializedViewRawBuilder<TTarget>
    where TTarget : class
{
    private readonly MaterializedViewSpec _spec;

    internal MaterializedViewRawBuilder(MaterializedViewSpec spec) => _spec = spec;

    /// <summary>
    /// Specifies the raw SQL <c>SELECT</c> body for the materialized view.
    /// </summary>
    public MaterializedViewConfig<TTarget> DefinedAsRaw(string selectSql)
    {
        ArgumentException.ThrowIfNullOrEmpty(selectSql);
        _spec.RawQuery = selectSql.Trim();
        return new MaterializedViewConfig<TTarget>(_spec);
    }
}

// =============================================================================
//  Stage 2..5 — multi-source, one extra TN per stage
// =============================================================================

public sealed class MaterializedViewBuilder<TTarget, T1, T2>
    where TTarget : class
    where T1 : class where T2 : class
{
    private readonly MaterializedViewSpec _spec;
    internal MaterializedViewBuilder(MaterializedViewSpec spec) => _spec = spec;

    public MaterializedViewBuilder<TTarget, T1, T2, T3> Join<T3>()
        where T3 : class
    {
        _spec.SourceTypes.Add(typeof(T3));
        return new MaterializedViewBuilder<TTarget, T1, T2, T3>(_spec);
    }

    public MaterializedViewConfig<TTarget> DefinedAs(
        Expression<Func<IQueryable<T1>, IQueryable<T2>, IQueryable<TTarget>>> query)
    {
        ArgumentNullException.ThrowIfNull(query);
        _spec.Query = query;
        return new MaterializedViewConfig<TTarget>(_spec);
    }
}

public sealed class MaterializedViewBuilder<TTarget, T1, T2, T3>
    where TTarget : class
    where T1 : class where T2 : class where T3 : class
{
    private readonly MaterializedViewSpec _spec;
    internal MaterializedViewBuilder(MaterializedViewSpec spec) => _spec = spec;

    public MaterializedViewBuilder<TTarget, T1, T2, T3, T4> Join<T4>()
        where T4 : class
    {
        _spec.SourceTypes.Add(typeof(T4));
        return new MaterializedViewBuilder<TTarget, T1, T2, T3, T4>(_spec);
    }

    public MaterializedViewConfig<TTarget> DefinedAs(
        Expression<Func<IQueryable<T1>, IQueryable<T2>, IQueryable<T3>, IQueryable<TTarget>>> query)
    {
        ArgumentNullException.ThrowIfNull(query);
        _spec.Query = query;
        return new MaterializedViewConfig<TTarget>(_spec);
    }
}

public sealed class MaterializedViewBuilder<TTarget, T1, T2, T3, T4>
    where TTarget : class
    where T1 : class where T2 : class where T3 : class where T4 : class
{
    private readonly MaterializedViewSpec _spec;
    internal MaterializedViewBuilder(MaterializedViewSpec spec) => _spec = spec;

    public MaterializedViewBuilder<TTarget, T1, T2, T3, T4, T5> Join<T5>()
        where T5 : class
    {
        _spec.SourceTypes.Add(typeof(T5));
        return new MaterializedViewBuilder<TTarget, T1, T2, T3, T4, T5>(_spec);
    }

    public MaterializedViewConfig<TTarget> DefinedAs(
        Expression<Func<IQueryable<T1>, IQueryable<T2>, IQueryable<T3>, IQueryable<T4>, IQueryable<TTarget>>> query)
    {
        ArgumentNullException.ThrowIfNull(query);
        _spec.Query = query;
        return new MaterializedViewConfig<TTarget>(_spec);
    }
}

public sealed class MaterializedViewBuilder<TTarget, T1, T2, T3, T4, T5>
    where TTarget : class
    where T1 : class where T2 : class where T3 : class where T4 : class where T5 : class
{
    private readonly MaterializedViewSpec _spec;
    internal MaterializedViewBuilder(MaterializedViewSpec spec) => _spec = spec;

    // Terminal — no further Join. Five sources is the supported ceiling; past
    // five, callers can still use closure-captured DbSets inside the lambda.
    public MaterializedViewConfig<TTarget> DefinedAs(
        Expression<Func<IQueryable<T1>, IQueryable<T2>, IQueryable<T3>, IQueryable<T4>, IQueryable<T5>, IQueryable<TTarget>>> query)
    {
        ArgumentNullException.ThrowIfNull(query);
        _spec.Query = query;
        return new MaterializedViewConfig<TTarget>(_spec);
    }
}

// =============================================================================
//  Terminal config — Populate / Deferred / Refresh / Target / etc.
// =============================================================================

/// <summary>
/// Terminal config stage. Each fluent call writes to the spec then immediately
/// flushes annotations onto the target entity, so partial chains still produce
/// a valid annotation set. Final state wins on repeated calls.
/// </summary>
public sealed class MaterializedViewConfig<TTarget>
    where TTarget : class
{
    private readonly MaterializedViewSpec _spec;

    internal MaterializedViewConfig(MaterializedViewSpec spec)
    {
        _spec = spec;
        Flush();
    }

    /// <summary>
    /// Emits <c>POPULATE</c> — backfill existing source data on creation.
    /// Mutually exclusive with <see cref="RefreshEvery(TimeSpan)"/> /
    /// <see cref="RefreshAfter"/>.
    /// </summary>
    public MaterializedViewConfig<TTarget> Populate()
    {
        if (_spec.Refresh is not null)
            throw new InvalidOperationException(
                "Populate() and RefreshEvery/RefreshAfter are mutually exclusive on a single materialized view.");
        _spec.Populate = true;
        Flush();
        return this;
    }

    /// <summary>
    /// Marks this MV as deferred — <c>EnsureCreatedAsync</c> skips its DDL so
    /// the caller can run <c>DatabaseFacade.CreateMaterializedViewAsync&lt;T&gt;</c>
    /// later (after seeding source data). Same semantics as the legacy
    /// <c>.Deferred()</c>.
    /// </summary>
    public MaterializedViewConfig<TTarget> Deferred()
    {
        _spec.Deferred = true;
        Flush();
        return this;
    }

    /// <summary>
    /// Routes the view's output rows into the named target table via
    /// <c>TO &lt;target&gt;</c>. The MV's own ENGINE clause is then omitted.
    /// </summary>
    public MaterializedViewConfig<TTarget> ToTable(string targetTable)
    {
        ArgumentException.ThrowIfNullOrEmpty(targetTable);
        _spec.TargetTable = targetTable;
        Flush();
        return this;
    }

    /// <summary>
    /// Routes the view's output rows into <typeparamref name="TTarget2"/>'s table.
    /// Matches legacy behavior: uses <c>typeof(TTarget2).Name</c> as the target
    /// table identifier (the entity's CLR name, not its EF-resolved table name).
    /// Use <see cref="ToTable"/> if you need a different name.
    /// </summary>
    public MaterializedViewConfig<TTarget> ToTarget<TTarget2>() where TTarget2 : class
    {
        _spec.TargetTable = typeof(TTarget2).Name;
        Flush();
        return this;
    }

    /// <summary>
    /// Adds an <c>ON CLUSTER &lt;cluster&gt;</c> clause to the MV DDL.
    /// </summary>
    public MaterializedViewConfig<TTarget> OnCluster(string cluster)
    {
        ArgumentException.ThrowIfNullOrEmpty(cluster);
        _spec.OnCluster = cluster;
        Flush();
        return this;
    }

    // -------- Refresh schedule (refreshable MV) --------

    public MaterializedViewConfig<TTarget> RefreshEvery(TimeSpan interval)
        => SetSchedule("EVERY", IntervalLiteralConverter.FromTimeSpan(interval));

    public MaterializedViewConfig<TTarget> RefreshEvery(long count, ClickHouseIntervalUnit unit)
        => SetSchedule("EVERY", IntervalLiteralConverter.Format(count, unit));

    public MaterializedViewConfig<TTarget> RefreshAfter(TimeSpan interval)
        => SetSchedule("AFTER", IntervalLiteralConverter.FromTimeSpan(interval));

    public MaterializedViewConfig<TTarget> RefreshAfter(long count, ClickHouseIntervalUnit unit)
        => SetSchedule("AFTER", IntervalLiteralConverter.Format(count, unit));

    public MaterializedViewConfig<TTarget> RandomizeFor(TimeSpan interval)
    {
        var r = _spec.RequireRefresh();
        r.RandomizeFor = IntervalLiteralConverter.FromTimeSpan(interval);
        Flush();
        return this;
    }

    public MaterializedViewConfig<TTarget> RandomizeFor(long count, ClickHouseIntervalUnit unit)
    {
        var r = _spec.RequireRefresh();
        r.RandomizeFor = IntervalLiteralConverter.Format(count, unit);
        Flush();
        return this;
    }

    public MaterializedViewConfig<TTarget> Offset(TimeSpan interval)
    {
        var r = _spec.RequireRefresh();
        r.Offset = IntervalLiteralConverter.FromTimeSpan(interval);
        Flush();
        return this;
    }

    public MaterializedViewConfig<TTarget> Offset(long count, ClickHouseIntervalUnit unit)
    {
        var r = _spec.RequireRefresh();
        r.Offset = IntervalLiteralConverter.Format(count, unit);
        Flush();
        return this;
    }

    public MaterializedViewConfig<TTarget> DependsOn<TDep>() where TDep : class
    {
        var r = _spec.RequireRefresh();
        r.DependsOn.Add(typeof(TDep).Name);
        Flush();
        return this;
    }

    public MaterializedViewConfig<TTarget> DependsOn(params string[] entityNames)
    {
        ArgumentNullException.ThrowIfNull(entityNames);
        var r = _spec.RequireRefresh();
        foreach (var n in entityNames)
            if (!string.IsNullOrWhiteSpace(n)) r.DependsOn.Add(n);
        Flush();
        return this;
    }

    public MaterializedViewConfig<TTarget> Append()
    {
        var r = _spec.RequireRefresh();
        if (r.Empty)
            throw new InvalidOperationException("Append() and Empty() are mutually exclusive.");
        r.Append = true;
        Flush();
        return this;
    }

    public MaterializedViewConfig<TTarget> Empty()
    {
        var r = _spec.RequireRefresh();
        if (r.Append)
            throw new InvalidOperationException("Append() and Empty() are mutually exclusive.");
        r.Empty = true;
        Flush();
        return this;
    }

    public MaterializedViewConfig<TTarget> WithSetting(string name, string value)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(value);
        var r = _spec.RequireRefresh();
        r.Settings[name] = value;
        Flush();
        return this;
    }

    private MaterializedViewConfig<TTarget> SetSchedule(string kind, string interval)
    {
        if (_spec.Populate)
            throw new InvalidOperationException(
                "RefreshEvery/RefreshAfter and Populate() are mutually exclusive on a single materialized view.");
        var r = _spec.RequireRefresh();
        r.Kind = kind;
        r.Interval = interval;
        Flush();
        return this;
    }

    private void Flush() => MaterializedViewSpecApplier.Apply(_spec);
}

// =============================================================================
//  Internal spec records
// =============================================================================

internal sealed class MaterializedViewSpec
{
    public required IMutableEntityType Target { get; init; }
    public required IMutableModel Model { get; init; }

    public List<Type> SourceTypes { get; } = new();      // T1 = trigger, T2..Tn = joined
    public string? SourceTableOverride;                   // FromTable(...)
    public LambdaExpression? Query;                       // DefinedAs(...)
    public string? RawQuery;                              // DefinedAsRaw(...)

    public bool Populate;
    public bool Deferred;
    public string? TargetTable;
    public string? OnCluster;

    public RefreshableMaterializedViewSpec? Refresh;      // null = INSERT trigger MV

    public RefreshableMaterializedViewSpec RequireRefresh()
        => Refresh ??= new RefreshableMaterializedViewSpec();
}

internal sealed class RefreshableMaterializedViewSpec
{
    public string? Kind;
    public string? Interval;
    public string? Offset;
    public string? RandomizeFor;
    public List<string> DependsOn { get; } = new();
    public bool Append;
    public bool Empty;
    public Dictionary<string, string> Settings { get; } = new(StringComparer.Ordinal);
}

// =============================================================================
//  Annotation writer (centralised)
// =============================================================================

internal static class MaterializedViewSpecApplier
{
    public static void Apply(MaterializedViewSpec spec)
    {
        var target = spec.Target;
        var model = spec.Model;

        // Marker
        target.SetAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        // Auto-apply HasNoKey on the target — replaces the explicit
        // entity.HasNoKey() requirement in legacy callers. Skip if the user
        // already declared a key on the entity (e.g. tests using ToTarget<T>()
        // where the underlying target table is a regular table); legacy
        // callers explicitly called HasNoKey() so this only matters for
        // entities that haven't been keyed yet.
        if (target.FindPrimaryKey() is null && !target.IsKeyless)
        {
            target.IsKeyless = true;
        }

        // Source — T1 is the trigger
        if (spec.SourceTypes.Count > 0)
        {
            var triggerTable = ResolveTableName(model, spec.SourceTypes[0]);
            target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, triggerTable);
        }
        else if (spec.SourceTableOverride is { } src)
        {
            target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, src);
        }

        // Joined sources — T2..Tn
        if (spec.SourceTypes.Count > 1)
        {
            var joined = spec.SourceTypes.Skip(1)
                .Select(t => ResolveTableName(model, t))
                .ToArray();
            target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewJoinedSources, joined);
        }
        else
        {
            // Clear any stale annotation on re-entry with fewer sources.
            target.RemoveAnnotation(ClickHouseAnnotationNames.MaterializedViewJoinedSources);
        }

        // Query — call MaterializedViewSqlTranslator with the right arity
        if (spec.Query is { } q)
        {
            var triggerTableName = spec.SourceTypes.Count > 0
                ? ResolveTableName(model, spec.SourceTypes[0])
                : (spec.SourceTableOverride ?? string.Empty);
            var translator = new MaterializedViewSqlTranslator((IModel)model, triggerTableName);
            var sql = TranslateLambda(translator, q);
            target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, sql);
        }
        else if (spec.RawQuery is { } raw)
        {
            target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, raw);
        }

        // Populate / Deferred — match legacy behavior: write the populate
        // annotation when the user opted in (Populate()), but otherwise leave
        // any pre-existing annotation untouched. The legacy raw/LINQ methods
        // wrote the parameter value directly; the new builder defaults to
        // "not populating" so we set false only if no value was already present.
        if (spec.Populate)
        {
            target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewPopulate, true);
        }
        else if (target.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewPopulate) is null)
        {
            target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewPopulate, false);
        }

        if (spec.Deferred)
            target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewDeferred, true);
        else
            target.RemoveAnnotation(ClickHouseAnnotationNames.MaterializedViewDeferred);

        // Target — reuse the existing MaterializedViewRefreshTarget annotation key
        if (spec.TargetTable is { } t)
            target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshTarget, t);
        else
            target.RemoveAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshTarget);

        if (spec.OnCluster is { } c)
            target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewOnCluster, c);

        // Refresh
        if (spec.Refresh is { } r)
        {
            if (!string.IsNullOrEmpty(r.Kind))
                target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshKind, r.Kind);
            if (!string.IsNullOrEmpty(r.Interval))
                target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshInterval, r.Interval);
            if (r.Offset is { } off)
                target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshOffset, off);
            if (r.RandomizeFor is { } rf)
                target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshRandomizeFor, rf);
            if (r.DependsOn.Count > 0)
                target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshDependsOn, r.DependsOn.ToArray());
            if (r.Append)
                target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshAppend, true);
            if (r.Empty)
                target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshEmpty, true);
            if (r.Settings.Count > 0)
                target.SetAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshSettings,
                    new Dictionary<string, string>(r.Settings, StringComparer.Ordinal));
        }
    }

    private static string ResolveTableName(IMutableModel model, Type type)
    {
        var entity = model.FindEntityType(type)
            ?? throw new InvalidOperationException(
                $"Source entity '{type.Name}' is not declared in the model. " +
                $"Declare it via modelBuilder.Entity<{type.Name}>(...) before referencing it from a materialized view.");
        return entity.GetTableName() ?? type.Name;
    }

    /// <summary>
    /// Reflectively dispatches to the right <c>Translate&lt;…&gt;</c> overload
    /// on <see cref="MaterializedViewSqlTranslator"/> based on the lambda's arity.
    /// </summary>
    private static string TranslateLambda(MaterializedViewSqlTranslator translator, LambdaExpression lambda)
    {
        // Generic args: [T1..Tn, TResult]
        var queryableArgs = lambda.Parameters
            .Select(p => p.Type.GetGenericArguments()[0]) // IQueryable<T> → T
            .ToList();
        var resultType = lambda.ReturnType.GetGenericArguments()[0];
        var typeArgs = queryableArgs.Concat(new[] { resultType }).ToArray();

        // Pick the right Translate<…> overload by parameter count.
        var translateMethods = typeof(MaterializedViewSqlTranslator)
            .GetMethods()
            .Where(m => m.Name == nameof(MaterializedViewSqlTranslator.Translate) && m.IsGenericMethod)
            .ToArray();
        var method = translateMethods.FirstOrDefault(m => m.GetGenericArguments().Length == typeArgs.Length)
            ?? throw new InvalidOperationException(
                $"No MaterializedViewSqlTranslator.Translate overload for arity {queryableArgs.Count}.");
        var generic = method.MakeGenericMethod(typeArgs);
        return (string)generic.Invoke(translator, new object[] { lambda })!;
    }
}
