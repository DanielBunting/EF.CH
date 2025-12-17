namespace EF.CH.Dictionaries.Sources;

/// <summary>
/// Builder for configuring MySQL as a dictionary source.
/// </summary>
public class MySqlDictionarySourceBuilder
{
    private readonly MySqlDictionarySourceConfig _config = new();

    /// <summary>
    /// Specifies the remote MySQL table name.
    /// </summary>
    /// <param name="table">The table name in MySQL.</param>
    /// <returns>The builder for chaining.</returns>
    public MySqlDictionarySourceBuilder FromTable(string table)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        _config.Table = table;
        return this;
    }

    /// <summary>
    /// Configures the MySQL connection parameters.
    /// </summary>
    /// <param name="configure">Action to configure the connection.</param>
    /// <returns>The builder for chaining.</returns>
    public MySqlDictionarySourceBuilder Connection(Action<DictionaryConnectionBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new DictionaryConnectionBuilder(_config.Connection);
        configure(builder);
        return this;
    }

    /// <summary>
    /// Adds a WHERE clause filter to the dictionary source.
    /// </summary>
    /// <param name="whereClause">The WHERE clause (without the WHERE keyword).</param>
    /// <returns>The builder for chaining.</returns>
    public MySqlDictionarySourceBuilder Where(string whereClause)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(whereClause);
        _config.WhereClause = whereClause;
        return this;
    }

    /// <summary>
    /// Configures a query used to check if the dictionary should be invalidated.
    /// The query should return a single value that changes when data changes
    /// (e.g., SELECT max(updated_at) FROM table).
    /// </summary>
    /// <param name="query">The invalidate query.</param>
    /// <returns>The builder for chaining.</returns>
    public MySqlDictionarySourceBuilder InvalidateQuery(string query)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(query);
        _config.InvalidateQuery = query;
        return this;
    }

    /// <summary>
    /// Configures behavior when connection to MySQL is lost.
    /// When true, throws an exception immediately on connection loss.
    /// When false (default), retries three times before failing.
    /// </summary>
    /// <param name="failOnConnectionLoss">Whether to fail immediately on connection loss.</param>
    /// <returns>The builder for chaining.</returns>
    public MySqlDictionarySourceBuilder FailOnConnectionLoss(bool failOnConnectionLoss = true)
    {
        _config.FailOnConnectionLoss = failOnConnectionLoss;
        return this;
    }

    /// <summary>
    /// Gets the built configuration.
    /// </summary>
    internal MySqlDictionarySourceConfig Build() => _config;
}

/// <summary>
/// Internal storage for MySQL dictionary source configuration.
/// </summary>
internal class MySqlDictionarySourceConfig
{
    public string? Table { get; set; }
    public string? WhereClause { get; set; }
    public string? InvalidateQuery { get; set; }
    public bool? FailOnConnectionLoss { get; set; }
    public DictionaryConnectionConfig Connection { get; } = new();
}
