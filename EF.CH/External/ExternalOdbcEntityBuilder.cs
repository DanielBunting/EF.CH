using System.Linq.Expressions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.External;

/// <summary>
/// Builder for configuring external ODBC entities that use the odbc() table function.
/// ODBC entities reference a pre-configured DSN in odbc.ini, rather than inline credentials.
/// This is useful for connecting to databases like SQL Server that don't have native ClickHouse table functions.
/// </summary>
/// <typeparam name="TEntity">The entity type.</typeparam>
public class ExternalOdbcEntityBuilder<TEntity> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _entityBuilder;

    private string? _table;
    private string? _database;
    private string? _dsnEnv;
    private string? _dsnValue;
    private bool _allowInserts;

    internal ExternalOdbcEntityBuilder(ModelBuilder modelBuilder)
    {
        _entityBuilder = modelBuilder.Entity<TEntity>();
    }

    /// <summary>
    /// Specifies the remote table name.
    /// </summary>
    /// <param name="table">The table name in the external database.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalOdbcEntityBuilder<TEntity> FromTable(string table)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        _table = table;
        return this;
    }

    /// <summary>
    /// Sets the ODBC Data Source Name (DSN) from an environment variable or literal value.
    /// The DSN must be pre-configured in odbc.ini on the ClickHouse server.
    /// </summary>
    /// <param name="env">Environment variable name containing the DSN (e.g., "MSSQL_DSN").</param>
    /// <param name="value">Literal DSN value (e.g., "MsSqlProd").</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalOdbcEntityBuilder<TEntity> Dsn(string? env = null, string? value = null)
    {
        if (env != null) _dsnEnv = env;
        if (value != null) _dsnValue = value;
        return this;
    }

    /// <summary>
    /// Sets the database name for the ODBC connection.
    /// </summary>
    /// <param name="database">The database name.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalOdbcEntityBuilder<TEntity> Database(string database)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(database);
        _database = database;
        return this;
    }

    /// <summary>
    /// Enables INSERT operations via INSERT INTO FUNCTION odbc(...).
    /// By default, external entities are read-only.
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public ExternalOdbcEntityBuilder<TEntity> AllowInserts()
    {
        _allowInserts = true;
        return this;
    }

    /// <summary>
    /// Explicitly marks as read-only (this is the default).
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public ExternalOdbcEntityBuilder<TEntity> ReadOnly()
    {
        _allowInserts = false;
        return this;
    }

    /// <summary>
    /// Configures a property mapping with optional customization.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="propertyExpression">Expression selecting the property.</param>
    /// <param name="configure">Optional action to configure the property.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalOdbcEntityBuilder<TEntity> Property<TProperty>(
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
    public ExternalOdbcEntityBuilder<TEntity> Ignore(string propertyName)
    {
        _entityBuilder.Ignore(propertyName);
        return this;
    }

    /// <summary>
    /// Applies the configuration to the entity type.
    /// Called automatically by ExternalOdbcEntity extension method.
    /// </summary>
    internal void Build()
    {
        // External entities must be keyless - they use table functions, not tables
        _entityBuilder.HasNoKey();

        // Set core annotations
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction, true);
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalProvider, "odbc");
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalReadOnly, !_allowInserts);

        // Set table - default to snake_case entity name if not specified
        _entityBuilder.HasAnnotation(
            ClickHouseAnnotationNames.ExternalTable,
            _table ?? ToSnakeCase(typeof(TEntity).Name));

        // Set database
        if (!string.IsNullOrEmpty(_database))
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalDatabaseValue, _database);
        }

        // Set DSN configuration
        if (_dsnEnv != null)
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalOdbcDsnEnv, _dsnEnv);
        }

        if (_dsnValue != null)
        {
            _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalOdbcDsnValue, _dsnValue);
        }
    }

    private static string ToSnakeCase(string str)
    {
        return string.Concat(str.Select((c, i) =>
            i > 0 && char.IsUpper(c) ? "_" + char.ToLower(c) : char.ToLower(c).ToString()));
    }
}
