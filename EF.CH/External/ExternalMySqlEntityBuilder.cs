using System.Linq.Expressions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.External;

/// <summary>
/// Builder for configuring external MySQL entities that use the mysql() table function.
/// External entities do not create ClickHouse tables - they query MySQL directly via table functions.
/// </summary>
/// <typeparam name="TEntity">The entity type.</typeparam>
public class ExternalMySqlEntityBuilder<TEntity> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _entityBuilder;
    private readonly MySqlConnectionConfig _connection = new();

    private string? _table;
    private bool _allowInserts;
    private bool _useReplace;
    private string? _onDuplicateClause;

    internal ExternalMySqlEntityBuilder(ModelBuilder modelBuilder)
    {
        _entityBuilder = modelBuilder.Entity<TEntity>();
    }

    /// <summary>
    /// Specifies the remote MySQL table name.
    /// Note: MySQL doesn't have schemas in the same way PostgreSQL does - use database.table notation.
    /// </summary>
    /// <param name="table">The table name in MySQL.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalMySqlEntityBuilder<TEntity> FromTable(string table)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        _table = table;
        return this;
    }

    /// <summary>
    /// Configures the MySQL connection parameters.
    /// </summary>
    /// <param name="configure">Action to configure the connection.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalMySqlEntityBuilder<TEntity> Connection(Action<MySqlConnectionBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new MySqlConnectionBuilder(_connection);
        configure(builder);
        return this;
    }

    /// <summary>
    /// Enables INSERT operations via INSERT INTO FUNCTION mysql(...).
    /// By default, external entities are read-only.
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public ExternalMySqlEntityBuilder<TEntity> AllowInserts()
    {
        _allowInserts = true;
        return this;
    }

    /// <summary>
    /// Explicitly marks as read-only (this is the default).
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public ExternalMySqlEntityBuilder<TEntity> ReadOnly()
    {
        _allowInserts = false;
        return this;
    }

    /// <summary>
    /// Use REPLACE INTO instead of INSERT INTO for inserts.
    /// MySQL-specific: converts INSERT INTO to REPLACE INTO (replace_query=1).
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public ExternalMySqlEntityBuilder<TEntity> UseReplaceForInserts()
    {
        _useReplace = true;
        return this;
    }

    /// <summary>
    /// Sets the ON DUPLICATE KEY clause for MySQL inserts.
    /// MySQL-specific: adds ON DUPLICATE KEY UPDATE clause.
    /// Cannot be used with UseReplaceForInserts().
    /// </summary>
    /// <param name="clause">The ON DUPLICATE KEY UPDATE clause (e.g., "name = VALUES(name)").</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalMySqlEntityBuilder<TEntity> OnDuplicateKey(string clause)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clause);
        _onDuplicateClause = clause;
        return this;
    }

    /// <summary>
    /// Configures a property mapping with optional customization.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyExpression">Expression selecting the property.</param>
    /// <param name="configure">Optional action to configure the property.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalMySqlEntityBuilder<TEntity> Property<TProperty>(
        Expression<Func<TEntity, TProperty>> propertyExpression,
        Action<PropertyBuilder<TProperty>>? configure = null)
    {
        var propBuilder = _entityBuilder.Property(propertyExpression);
        configure?.Invoke(propBuilder);
        return this;
    }

    /// <summary>
    /// Ignores a property (won't be included in queries).
    /// </summary>
    /// <param name="propertyName">The name of the property to ignore.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalMySqlEntityBuilder<TEntity> Ignore(string propertyName)
    {
        _entityBuilder.Ignore(propertyName);
        return this;
    }

    /// <summary>
    /// Applies the configuration to the entity type.
    /// Called automatically by ExternalMySqlEntity extension method.
    /// </summary>
    internal void Build()
    {
        // Validate conflicting options
        if (_useReplace && !string.IsNullOrEmpty(_onDuplicateClause))
        {
            throw new InvalidOperationException(
                "Cannot use both UseReplaceForInserts() and OnDuplicateKey() - they are mutually exclusive.");
        }

        // External entities must be keyless - they use table functions, not tables
        _entityBuilder.HasNoKey();

        // Set core annotations
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction, true);
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalProvider, "mysql");
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalReadOnly, !_allowInserts);

        // Set table - default to snake_case entity name if not specified
        _entityBuilder.HasAnnotation(
            ClickHouseAnnotationNames.ExternalTable,
            _table ?? ToSnakeCase(typeof(TEntity).Name));

        // MySQL-specific options
        if (_useReplace)
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalMySqlReplaceQuery, true);
        }

        if (!string.IsNullOrEmpty(_onDuplicateClause))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalMySqlOnDuplicateClause, _onDuplicateClause);
        }

        // Apply connection configuration
        _connection.ApplyTo(_entityBuilder);
    }

    private static string ToSnakeCase(string str)
    {
        return string.Concat(str.Select((c, i) =>
            i > 0 && char.IsUpper(c) ? "_" + char.ToLower(c) : char.ToLower(c).ToString()));
    }
}
