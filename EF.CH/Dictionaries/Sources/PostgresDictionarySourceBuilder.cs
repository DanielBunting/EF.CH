namespace EF.CH.Dictionaries.Sources;

/// <summary>
/// Builder for configuring PostgreSQL as a dictionary source.
/// </summary>
public class PostgresDictionarySourceBuilder
{
    private readonly PostgresDictionarySourceConfig _config = new();

    /// <summary>
    /// Specifies the remote PostgreSQL table name and optional schema.
    /// </summary>
    /// <param name="table">The table name in PostgreSQL.</param>
    /// <param name="schema">The schema name (default: "public").</param>
    /// <returns>The builder for chaining.</returns>
    public PostgresDictionarySourceBuilder FromTable(string table, string schema = "public")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        _config.Table = table;
        _config.Schema = schema;
        return this;
    }

    /// <summary>
    /// Configures the PostgreSQL connection parameters.
    /// </summary>
    /// <param name="configure">Action to configure the connection.</param>
    /// <returns>The builder for chaining.</returns>
    public PostgresDictionarySourceBuilder Connection(Action<DictionaryConnectionBuilder> configure)
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
    public PostgresDictionarySourceBuilder Where(string whereClause)
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
    public PostgresDictionarySourceBuilder InvalidateQuery(string query)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(query);
        _config.InvalidateQuery = query;
        return this;
    }

    /// <summary>
    /// Gets the built configuration.
    /// </summary>
    internal PostgresDictionarySourceConfig Build() => _config;
}

/// <summary>
/// Internal storage for PostgreSQL dictionary source configuration.
/// </summary>
internal class PostgresDictionarySourceConfig
{
    public string? Table { get; set; }
    public string Schema { get; set; } = "public";
    public string? WhereClause { get; set; }
    public string? InvalidateQuery { get; set; }
    public DictionaryConnectionConfig Connection { get; } = new();
}
