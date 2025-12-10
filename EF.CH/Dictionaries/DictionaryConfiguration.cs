using System.Linq.Expressions;
using EF.CH.Extensions;

namespace EF.CH.Dictionaries;

/// <summary>
/// Builder for configuring a ClickHouse dictionary.
/// </summary>
/// <typeparam name="TEntity">The entity type representing the dictionary.</typeparam>
public sealed class DictionaryConfiguration<TEntity> where TEntity : class
{
    internal DictionarySource? Source { get; private set; }
    internal DictionaryLayout Layout { get; private set; } = DictionaryLayout.Hashed();
    internal DictionaryLifetime Lifetime { get; private set; } = new(300, 360);
    internal string[]? RangeMinColumns { get; private set; }
    internal string[]? RangeMaxColumns { get; private set; }

    /// <summary>
    /// Configures the dictionary to load data from a ClickHouse table.
    /// </summary>
    /// <param name="tableName">The source table name.</param>
    /// <param name="configure">Optional configuration for the table source.</param>
    public DictionaryConfiguration<TEntity> FromTable(
        string tableName,
        Action<ClickHouseTableSourceBuilder>? configure = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(tableName);

        var builder = new ClickHouseTableSourceBuilder { Table = tableName };
        configure?.Invoke(builder);
        Source = builder.Build();
        return this;
    }

    /// <summary>
    /// Configures the dictionary to load data from an HTTP endpoint.
    /// </summary>
    /// <param name="url">The HTTP URL.</param>
    /// <param name="format">The data format.</param>
    public DictionaryConfiguration<TEntity> FromHttp(
        string url,
        HttpDictionaryFormat format = HttpDictionaryFormat.JSONEachRow)
    {
        ArgumentException.ThrowIfNullOrEmpty(url);

        Source = new HttpDictionarySource { Url = url, Format = format };
        return this;
    }

    /// <summary>
    /// Configures the dictionary to load data from an HTTP endpoint with additional options.
    /// </summary>
    /// <param name="url">The HTTP URL.</param>
    /// <param name="configure">Configuration for the HTTP source.</param>
    public DictionaryConfiguration<TEntity> FromHttp(
        string url,
        Action<HttpSourceBuilder> configure)
    {
        ArgumentException.ThrowIfNullOrEmpty(url);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new HttpSourceBuilder { Url = url };
        configure(builder);
        Source = builder.Build();
        return this;
    }

    /// <summary>
    /// Configures the dictionary to load data from a local file.
    /// </summary>
    /// <param name="path">The file path.</param>
    /// <param name="format">The file format.</param>
    public DictionaryConfiguration<TEntity> FromFile(
        string path,
        FileDictionaryFormat format = FileDictionaryFormat.TabSeparated)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        Source = new FileDictionarySource { Path = path, Format = format };
        return this;
    }

    /// <summary>
    /// Configures the dictionary to load data by executing a command.
    /// </summary>
    /// <param name="command">The command to execute.</param>
    /// <param name="format">The output format.</param>
    /// <param name="pool">Whether to use a pool of processes.</param>
    public DictionaryConfiguration<TEntity> FromExecutable(
        string command,
        FileDictionaryFormat format = FileDictionaryFormat.TabSeparated,
        bool pool = false)
    {
        ArgumentException.ThrowIfNullOrEmpty(command);

        Source = new ExecutableDictionarySource { Command = command, Format = format, Pool = pool };
        return this;
    }

    /// <summary>
    /// Sets the dictionary layout.
    /// </summary>
    /// <param name="layout">The layout to use.</param>
    public DictionaryConfiguration<TEntity> WithLayout(DictionaryLayout layout)
    {
        ArgumentNullException.ThrowIfNull(layout);
        Layout = layout;
        return this;
    }

    /// <summary>
    /// Configures range columns for RANGE_HASHED layout.
    /// Required when using RangeHashed() layout for time-based lookups.
    /// </summary>
    /// <typeparam name="TProperty">The range column type.</typeparam>
    /// <param name="rangeMin">Expression selecting the range minimum column.</param>
    /// <param name="rangeMax">Expression selecting the range maximum column.</param>
    public DictionaryConfiguration<TEntity> WithRangeColumns<TProperty>(
        Expression<Func<TEntity, TProperty>> rangeMin,
        Expression<Func<TEntity, TProperty>> rangeMax)
    {
        ArgumentNullException.ThrowIfNull(rangeMin);
        ArgumentNullException.ThrowIfNull(rangeMax);

        RangeMinColumns = new[] { ExpressionExtensions.GetPropertyName(rangeMin) };
        RangeMaxColumns = new[] { ExpressionExtensions.GetPropertyName(rangeMax) };
        return this;
    }

    /// <summary>
    /// Sets the dictionary refresh lifetime with a random interval.
    /// ClickHouse will refresh the dictionary at a random time between min and max seconds.
    /// </summary>
    /// <param name="minSeconds">Minimum seconds between refreshes.</param>
    /// <param name="maxSeconds">Maximum seconds between refreshes.</param>
    public DictionaryConfiguration<TEntity> WithLifetime(int minSeconds, int maxSeconds)
    {
        if (minSeconds < 0)
            throw new ArgumentOutOfRangeException(nameof(minSeconds), "Must be non-negative.");
        if (maxSeconds < minSeconds)
            throw new ArgumentOutOfRangeException(nameof(maxSeconds), "Must be >= minSeconds.");

        Lifetime = new DictionaryLifetime(minSeconds, maxSeconds);
        return this;
    }

    /// <summary>
    /// Sets the dictionary refresh lifetime with a fixed interval.
    /// </summary>
    /// <param name="seconds">Seconds between refreshes. Use 0 for no automatic refresh.</param>
    public DictionaryConfiguration<TEntity> WithLifetime(int seconds)
    {
        if (seconds < 0)
            throw new ArgumentOutOfRangeException(nameof(seconds), "Must be non-negative.");

        Lifetime = new DictionaryLifetime(seconds);
        return this;
    }
}

/// <summary>
/// Builder for ClickHouse table source configuration.
/// </summary>
public sealed class ClickHouseTableSourceBuilder
{
    internal string Table { get; set; } = string.Empty;

    /// <summary>
    /// The database containing the source table.
    /// </summary>
    public string? Database { get; set; }

    /// <summary>
    /// Remote ClickHouse host (for distributed setups).
    /// </summary>
    public string? Host { get; set; }

    /// <summary>
    /// Remote ClickHouse port.
    /// </summary>
    public int? Port { get; set; }

    /// <summary>
    /// Username for remote connection.
    /// </summary>
    public string? User { get; set; }

    /// <summary>
    /// Password for remote connection.
    /// </summary>
    public string? Password { get; set; }

    /// <summary>
    /// WHERE clause to filter source data.
    /// </summary>
    public string? Where { get; set; }

    /// <summary>
    /// Column to check for updates (enables incremental updates).
    /// </summary>
    public string? UpdateField { get; set; }

    /// <summary>
    /// Query to check if dictionary needs invalidation.
    /// </summary>
    public string? InvalidateQuery { get; set; }

    internal ClickHouseTableSource Build() => new()
    {
        Table = Table,
        Database = Database,
        Host = Host,
        Port = Port,
        User = User,
        Password = Password,
        Where = Where,
        UpdateField = UpdateField,
        InvalidateQuery = InvalidateQuery
    };
}

/// <summary>
/// Builder for HTTP source configuration.
/// </summary>
public sealed class HttpSourceBuilder
{
    internal string Url { get; set; } = string.Empty;

    /// <summary>
    /// The data format returned by the endpoint.
    /// </summary>
    public HttpDictionaryFormat Format { get; set; } = HttpDictionaryFormat.JSONEachRow;

    /// <summary>
    /// HTTP headers to include in the request.
    /// </summary>
    public Dictionary<string, string> Headers { get; } = new();

    /// <summary>
    /// HTTP request credentials (user:password for basic auth).
    /// </summary>
    public string? Credentials { get; set; }

    /// <summary>
    /// Adds an HTTP header.
    /// </summary>
    public HttpSourceBuilder WithHeader(string name, string value)
    {
        Headers[name] = value;
        return this;
    }

    internal HttpDictionarySource Build() => new()
    {
        Url = Url,
        Format = Format,
        Headers = Headers.Count > 0 ? Headers : null,
        Credentials = Credentials
    };
}
