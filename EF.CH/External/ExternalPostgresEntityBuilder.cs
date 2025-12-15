using System.Linq.Expressions;
using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.External;

/// <summary>
/// Builder for configuring external PostgreSQL entities that use the postgresql() table function.
/// External entities do not create ClickHouse tables - they query PostgreSQL directly via table functions.
/// </summary>
/// <typeparam name="TEntity">The entity type.</typeparam>
public class ExternalPostgresEntityBuilder<TEntity> where TEntity : class
{
    private readonly ModelBuilder _modelBuilder;
    private readonly EntityTypeBuilder<TEntity> _entityBuilder;
    private readonly PostgresConnectionConfig _connection = new();

    private string? _table;
    private string _schema = "public";
    private bool _allowInserts;

    internal ExternalPostgresEntityBuilder(ModelBuilder modelBuilder)
    {
        _modelBuilder = modelBuilder;
        _entityBuilder = modelBuilder.Entity<TEntity>();
    }

    /// <summary>
    /// Specifies the remote PostgreSQL table name and optional schema.
    /// </summary>
    /// <param name="table">The table name in PostgreSQL.</param>
    /// <param name="schema">The schema name (default: "public").</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalPostgresEntityBuilder<TEntity> FromTable(string table, string schema = "public")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        _table = table;
        _schema = schema;
        return this;
    }

    /// <summary>
    /// Configures the PostgreSQL connection parameters.
    /// </summary>
    /// <param name="configure">Action to configure the connection.</param>
    /// <returns>The builder for chaining.</returns>
    public ExternalPostgresEntityBuilder<TEntity> Connection(Action<PostgresConnectionBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new PostgresConnectionBuilder(_connection);
        configure(builder);
        return this;
    }

    /// <summary>
    /// Enables INSERT operations via INSERT INTO FUNCTION postgresql(...).
    /// By default, external entities are read-only.
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public ExternalPostgresEntityBuilder<TEntity> AllowInserts()
    {
        _allowInserts = true;
        return this;
    }

    /// <summary>
    /// Explicitly marks as read-only (this is the default).
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public ExternalPostgresEntityBuilder<TEntity> ReadOnly()
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
    public ExternalPostgresEntityBuilder<TEntity> Property<TProperty>(
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
    public ExternalPostgresEntityBuilder<TEntity> Ignore(string propertyName)
    {
        _entityBuilder.Ignore(propertyName);
        return this;
    }

    /// <summary>
    /// Applies the configuration to the entity type.
    /// Called automatically by ExternalPostgresEntity extension method.
    /// </summary>
    internal void Build()
    {
        // External entities must be keyless - they use table functions, not tables
        _entityBuilder.HasNoKey();

        // Set core annotations
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction, true);
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalProvider, "postgresql");
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalReadOnly, !_allowInserts);

        // Set table/schema - default to snake_case entity name if not specified
        _entityBuilder.HasAnnotation(
            ClickHouseAnnotationNames.ExternalTable,
            _table ?? ToSnakeCase(typeof(TEntity).Name));
        _entityBuilder.HasAnnotation(ClickHouseAnnotationNames.ExternalSchema, _schema);

        // Apply connection configuration
        _connection.ApplyTo(_entityBuilder);
    }

    private static string ToSnakeCase(string str)
    {
        return string.Concat(str.Select((c, i) =>
            i > 0 && char.IsUpper(c) ? "_" + char.ToLower(c) : char.ToLower(c).ToString()));
    }
}
