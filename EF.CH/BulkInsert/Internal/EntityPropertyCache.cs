using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.BulkInsert.Internal;

/// <summary>
/// Caches property metadata for entities to improve bulk insert performance.
/// </summary>
internal sealed class EntityPropertyCache
{
    private readonly ConcurrentDictionary<Type, EntityPropertyInfo> _cache = new();
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly ISqlGenerationHelper _sqlGenerationHelper;
    private readonly IModel _model;

    public EntityPropertyCache(
        IRelationalTypeMappingSource typeMappingSource,
        ISqlGenerationHelper sqlGenerationHelper,
        IModel model)
    {
        _typeMappingSource = typeMappingSource;
        _sqlGenerationHelper = sqlGenerationHelper;
        _model = model;
    }

    public EntityPropertyInfo GetPropertyInfo<TEntity>() where TEntity : class
    {
        return _cache.GetOrAdd(typeof(TEntity), type => BuildPropertyInfo<TEntity>());
    }

    private EntityPropertyInfo BuildPropertyInfo<TEntity>() where TEntity : class
    {
        var entityType = _model.FindEntityType(typeof(TEntity));
        if (entityType == null)
        {
            throw new InvalidOperationException(
                $"Entity type '{typeof(TEntity).Name}' is not part of the model. " +
                "Make sure it is included in the DbContext.");
        }

        var tableName = entityType.GetTableName()
            ?? throw new InvalidOperationException(
                $"Entity type '{typeof(TEntity).Name}' does not have a table name.");
        var schema = entityType.GetSchema();

        var quotedTableName = _sqlGenerationHelper.DelimitIdentifier(tableName, schema);

        var properties = new List<PropertyMapping>();
        var columnNames = new List<string>();

        foreach (var property in entityType.GetProperties())
        {
            // Skip shadow properties that don't have a CLR property
            var propertyInfo = property.PropertyInfo;
            if (propertyInfo == null)
            {
                continue;
            }

            // Skip computed columns (MATERIALIZED/ALIAS) - they have ValueGenerated set
            // and have a ComputedColumnSql defined
            if (property.ValueGenerated != ValueGenerated.Never &&
                !string.IsNullOrEmpty(property.GetComputedColumnSql()))
            {
                continue;
            }

            var columnName = property.GetColumnName();
            if (string.IsNullOrEmpty(columnName))
            {
                continue;
            }

            // Get type mapping - prefer the relational type mapping from the property
            var typeMapping = property.FindRelationalTypeMapping()
                ?? _typeMappingSource.FindMapping(property) as RelationalTypeMapping;

            if (typeMapping == null)
            {
                throw new InvalidOperationException(
                    $"Could not find type mapping for property '{property.Name}' on entity '{typeof(TEntity).Name}'.");
            }

            properties.Add(new PropertyMapping(
                columnName,
                _sqlGenerationHelper.DelimitIdentifier(columnName),
                typeMapping,
                propertyInfo));

            columnNames.Add(_sqlGenerationHelper.DelimitIdentifier(columnName));
        }

        if (properties.Count == 0)
        {
            throw new InvalidOperationException(
                $"Entity type '{typeof(TEntity).Name}' has no insertable properties.");
        }

        return new EntityPropertyInfo(
            quotedTableName,
            string.Join(", ", columnNames),
            properties);
    }
}

/// <summary>
/// Cached property information for an entity type.
/// </summary>
internal sealed record EntityPropertyInfo(
    string QuotedTableName,
    string ColumnList,
    List<PropertyMapping> Properties);

/// <summary>
/// Mapping information for a single property.
/// </summary>
internal sealed record PropertyMapping(
    string ColumnName,
    string QuotedColumnName,
    RelationalTypeMapping TypeMapping,
    PropertyInfo PropertyInfo);
