using System.Linq.Expressions;
using System.Reflection;
using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Query.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Projections;

/// <summary>
/// Final builder for aggregation projections with GroupBy and Select configured.
/// </summary>
/// <typeparam name="TEntity">The entity type.</typeparam>
/// <typeparam name="TKey">The grouping key type.</typeparam>
/// <typeparam name="TResult">The result type (can be anonymous).</typeparam>
public class AggregationProjectionBuilder<TEntity, TKey, TResult> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _builder;
    private readonly string? _explicitName;
    private readonly Expression<Func<TEntity, TKey>> _keySelector;
    private readonly Expression<Func<IGrouping<TKey, TEntity>, TResult>> _selector;

    /// <summary>
    /// Creates a new aggregation projection builder.
    /// </summary>
    internal AggregationProjectionBuilder(
        EntityTypeBuilder<TEntity> builder,
        string? name,
        Expression<Func<TEntity, TKey>> keySelector,
        Expression<Func<IGrouping<TKey, TEntity>, TResult>> selector)
    {
        _builder = builder;
        _explicitName = name;
        _keySelector = keySelector;
        _selector = selector;
    }

    /// <summary>
    /// Builds and registers the projection with the entity configuration.
    /// </summary>
    /// <param name="materialize">Whether to materialize existing data (default: true).</param>
    /// <returns>The entity type builder for chaining.</returns>
    public EntityTypeBuilder<TEntity> Build(bool materialize = true)
    {
        var name = _explicitName ?? GenerateName();
        var tableName = _builder.Metadata.GetTableName() ?? typeof(TEntity).Name;

        var translator = new ProjectionSqlTranslator(
            (IModel)_builder.Metadata.Model,
            tableName);
        var selectSql = translator.TranslateAggregation(_keySelector, _selector);

        var projection = ProjectionDefinition.Aggregation(name, selectSql, materialize);
        AddProjectionWithValidation(projection);
        return _builder;
    }

    private string GenerateName()
    {
        var tableName = _builder.Metadata.GetTableName() ?? typeof(TEntity).Name;
        var snakeTableName = ExpressionExtensions.ToSnakeCase(tableName);
        var memberNames = ExtractSelectMemberNames();
        var snakeMembers = memberNames.Select(ExpressionExtensions.ToSnakeCase);
        return $"{snakeTableName}__prj_agg__{string.Join("__", snakeMembers)}";
    }

    private List<string> ExtractSelectMemberNames()
    {
        var names = new List<string>();
        var body = _selector.Body;

        if (body is NewExpression newExpr && newExpr.Members != null)
        {
            names.AddRange(newExpr.Members.Select(m => m.Name));
        }
        else if (body is MemberInitExpression memberInit)
        {
            names.AddRange(memberInit.Bindings
                .OfType<MemberAssignment>()
                .Select(b => b.Member.Name));
        }

        return names;
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
