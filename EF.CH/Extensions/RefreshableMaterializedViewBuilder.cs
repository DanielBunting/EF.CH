using EF.CH.Internal.Intervals;

namespace EF.CH.Extensions;

/// <summary>
/// Fluent configuration for a ClickHouse refreshable materialized view.
/// Returned via the <c>configure</c> callback of
/// <see cref="ClickHouseEntityTypeBuilderExtensions.AsRefreshableMaterializedView"/>
/// and its overloads.
/// </summary>
public sealed class RefreshableMaterializedViewBuilder
{
    private string? _kind;
    private string? _interval;
    private string? _offset;
    private string? _randomizeFor;
    private List<string>? _dependsOn;
    private bool _append;
    private bool _empty;
    private Dictionary<string, string>? _settings;
    private string? _target;

    /// <summary>
    /// <c>REFRESH EVERY &lt;interval&gt;</c> — schedule refreshes on a fixed cadence.
    /// </summary>
    public RefreshableMaterializedViewBuilder Every(TimeSpan interval)
        => SetSchedule("EVERY", IntervalLiteralConverter.FromTimeSpan(interval));

    /// <summary>
    /// <c>REFRESH EVERY &lt;count&gt; &lt;UNIT&gt;</c> — explicit unit form for
    /// MONTH/QUARTER/YEAR (which <see cref="TimeSpan"/> cannot represent exactly).
    /// </summary>
    public RefreshableMaterializedViewBuilder Every(long count, ClickHouseIntervalUnit unit)
        => SetSchedule("EVERY", IntervalLiteralConverter.Format(count, unit));

    /// <summary>
    /// <c>REFRESH EVERY &lt;interval&gt;</c> using the existing <see cref="ClickHouseInterval"/> type.
    /// </summary>
    public RefreshableMaterializedViewBuilder Every(ClickHouseInterval interval)
        => SetSchedule("EVERY", IntervalLiteralConverter.Format(interval.Value, interval.Unit));

    /// <summary>
    /// <c>REFRESH AFTER &lt;interval&gt;</c> — refresh again that long after the
    /// previous run finished.
    /// </summary>
    public RefreshableMaterializedViewBuilder After(TimeSpan interval)
        => SetSchedule("AFTER", IntervalLiteralConverter.FromTimeSpan(interval));

    /// <summary>
    /// <c>REFRESH AFTER &lt;count&gt; &lt;UNIT&gt;</c>.
    /// </summary>
    public RefreshableMaterializedViewBuilder After(long count, ClickHouseIntervalUnit unit)
        => SetSchedule("AFTER", IntervalLiteralConverter.Format(count, unit));

    /// <summary>
    /// <c>REFRESH AFTER &lt;interval&gt;</c> via <see cref="ClickHouseInterval"/>.
    /// </summary>
    public RefreshableMaterializedViewBuilder After(ClickHouseInterval interval)
        => SetSchedule("AFTER", IntervalLiteralConverter.Format(interval.Value, interval.Unit));

    /// <summary>
    /// Optional <c>OFFSET &lt;interval&gt;</c> aligning the schedule.
    /// </summary>
    public RefreshableMaterializedViewBuilder Offset(TimeSpan interval)
    {
        _offset = IntervalLiteralConverter.FromTimeSpan(interval);
        return this;
    }

    /// <summary>
    /// Optional <c>OFFSET &lt;count&gt; &lt;UNIT&gt;</c>.
    /// </summary>
    public RefreshableMaterializedViewBuilder Offset(long count, ClickHouseIntervalUnit unit)
    {
        _offset = IntervalLiteralConverter.Format(count, unit);
        return this;
    }

    /// <summary>
    /// Optional <c>RANDOMIZE FOR &lt;interval&gt;</c> jitter window.
    /// </summary>
    public RefreshableMaterializedViewBuilder RandomizeFor(TimeSpan interval)
    {
        _randomizeFor = IntervalLiteralConverter.FromTimeSpan(interval);
        return this;
    }

    /// <summary>
    /// Optional <c>RANDOMIZE FOR &lt;count&gt; &lt;UNIT&gt;</c>.
    /// </summary>
    public RefreshableMaterializedViewBuilder RandomizeFor(long count, ClickHouseIntervalUnit unit)
    {
        _randomizeFor = IntervalLiteralConverter.Format(count, unit);
        return this;
    }

    /// <summary>
    /// Adds entities to the <c>DEPENDS ON</c> list. Entries are CLR entity
    /// names; they are resolved to table names at DDL emission time.
    /// </summary>
    public RefreshableMaterializedViewBuilder DependsOn(params string[] entityNames)
    {
        ArgumentNullException.ThrowIfNull(entityNames);
        _dependsOn ??= new List<string>();
        foreach (var name in entityNames)
        {
            if (!string.IsNullOrWhiteSpace(name))
                _dependsOn.Add(name);
        }
        return this;
    }

    /// <summary>
    /// Adds <typeparamref name="TDep"/> to the <c>DEPENDS ON</c> list.
    /// </summary>
    public RefreshableMaterializedViewBuilder DependsOn<TDep>() where TDep : class
        => DependsOn(typeof(TDep).Name);

    /// <summary>
    /// Emits <c>APPEND</c> — refresh appends rows instead of atomically replacing
    /// the target. Mutually exclusive with <see cref="Empty"/>.
    /// </summary>
    public RefreshableMaterializedViewBuilder Append(bool value = true)
    {
        _append = value;
        return this;
    }

    /// <summary>
    /// Emits <c>EMPTY</c> — skip the initial refresh on creation. Mutually
    /// exclusive with <see cref="Append"/>.
    /// </summary>
    public RefreshableMaterializedViewBuilder Empty(bool value = true)
    {
        _empty = value;
        return this;
    }

    /// <summary>
    /// Adds a refresh-level <c>SETTINGS</c> entry (e.g. <c>refresh_retries=3</c>).
    /// </summary>
    public RefreshableMaterializedViewBuilder WithSetting(string name, string value)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(value);
        _settings ??= new Dictionary<string, string>(StringComparer.Ordinal);
        _settings[name] = value;
        return this;
    }

    /// <summary>
    /// Routes the refreshed rows into an existing target table via
    /// <c>TO &lt;target&gt;</c>. When set, the MV's own ENGINE clause is omitted.
    /// </summary>
    public RefreshableMaterializedViewBuilder ToTarget(string targetTable)
    {
        ArgumentException.ThrowIfNullOrEmpty(targetTable);
        _target = targetTable;
        return this;
    }

    /// <summary>
    /// Routes the refreshed rows into <typeparamref name="TTarget"/>'s table.
    /// </summary>
    public RefreshableMaterializedViewBuilder ToTarget<TTarget>() where TTarget : class
        => ToTarget(typeof(TTarget).Name);

    internal RefreshableMaterializedViewSpec Build()
    {
        if (_interval is null)
            throw new InvalidOperationException(
                "Refreshable materialized view requires Every(...) or After(...).");
        if (_append && _empty)
            throw new InvalidOperationException(
                "APPEND and EMPTY cannot both be set on a refreshable materialized view.");

        return new RefreshableMaterializedViewSpec(
            Kind: _kind!,
            Interval: _interval,
            Offset: _offset,
            RandomizeFor: _randomizeFor,
            DependsOn: _dependsOn?.ToArray(),
            Append: _append,
            Empty: _empty,
            Settings: _settings is null ? null : new Dictionary<string, string>(_settings, StringComparer.Ordinal),
            Target: _target);
    }

    private RefreshableMaterializedViewBuilder SetSchedule(string kind, string interval)
    {
        _kind = kind;
        _interval = interval;
        return this;
    }
}

internal sealed record RefreshableMaterializedViewSpec(
    string Kind,
    string Interval,
    string? Offset,
    string? RandomizeFor,
    string[]? DependsOn,
    bool Append,
    bool Empty,
    IReadOnlyDictionary<string, string>? Settings,
    string? Target);
