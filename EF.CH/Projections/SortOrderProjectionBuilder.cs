using System.Linq.Expressions;
using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Projections;

/// <summary>
/// Fluent builder for configuring sort-order projections.
/// </summary>
/// <typeparam name="TEntity">The entity type.</typeparam>
public class SortOrderProjectionBuilder<TEntity> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _builder;
    private readonly string? _explicitName;
    private readonly List<string> _columns = [];

    /// <summary>
    /// Creates a new sort-order projection builder.
    /// </summary>
    internal SortOrderProjectionBuilder(EntityTypeBuilder<TEntity> builder, string? name)
    {
        _builder = builder;
        _explicitName = name;
    }

    /// <summary>
    /// Adds a column to ORDER BY. Can be called multiple times to add additional columns.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="column">Expression selecting the column.</param>
    /// <returns>This builder for chaining.</returns>
    public SortOrderProjectionBuilder<TEntity> OrderBy<TProperty>(
        Expression<Func<TEntity, TProperty>> column)
    {
        ArgumentNullException.ThrowIfNull(column);
        _columns.Add(ExpressionExtensions.GetPropertyName(column));
        return this;
    }

    /// <summary>
    /// Adds a subsequent column to ORDER BY.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="column">Expression selecting the column.</param>
    /// <returns>This builder for chaining.</returns>
    public SortOrderProjectionBuilder<TEntity> ThenBy<TProperty>(
        Expression<Func<TEntity, TProperty>> column)
    {
        ArgumentNullException.ThrowIfNull(column);
        if (_columns.Count == 0)
        {
            throw new InvalidOperationException("ThenBy must be called after OrderBy.");
        }
        _columns.Add(ExpressionExtensions.GetPropertyName(column));
        return this;
    }

    /// <summary>
    /// Builds and registers the projection with the entity configuration.
    /// </summary>
    /// <param name="materialize">Whether to materialize existing data (default: true).</param>
    /// <returns>The entity type builder for chaining.</returns>
    public EntityTypeBuilder<TEntity> Build(bool materialize = true)
    {
        if (_columns.Count == 0)
        {
            throw new InvalidOperationException(
                "Sort-order projection must have at least one ORDER BY column. Call OrderBy() first.");
        }

        var name = _explicitName ?? GenerateName();
        var projection = ProjectionDefinition.SortOrder(
            name,
            _columns.Select(c => (c, false)),
            materialize);

        AddProjectionWithValidation(projection);
        return _builder;
    }

    private string GenerateName()
    {
        var tableName = _builder.Metadata.GetTableName() ?? typeof(TEntity).Name;
        var snakeTableName = ExpressionExtensions.ToSnakeCase(tableName);
        var snakeColumns = _columns.Select(ExpressionExtensions.ToSnakeCase);
        return $"{snakeTableName}__prj_ord__{string.Join("__", snakeColumns)}";
    }

    private void AddProjectionWithValidation(ProjectionDefinition projection)
    {
        var annotation = _builder.Metadata.FindAnnotation(ClickHouseAnnotationNames.Projections);
        var projections = annotation?.Value as List<ProjectionDefinition>
            ?? [];

        if (projections.Any(p => p.Name == projection.Name))
        {
            var tableName = _builder.Metadata.GetTableName() ?? typeof(TEntity).Name;
            throw new InvalidOperationException(
                $"A projection named '{projection.Name}' already exists on table '{tableName}'. " +
                "Projection names must be unique within a table.");
        }

        projections.Add(projection);
        _builder.HasAnnotation(ClickHouseAnnotationNames.Projections, projections);
    }
}
