using System.Collections;
using System.Text;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.BulkInsert.Internal;

/// <summary>
/// Builds INSERT statements with VALUES format for bulk insert operations.
/// </summary>
internal sealed class BulkInsertSqlBuilder
{
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public BulkInsertSqlBuilder(IRelationalTypeMappingSource typeMappingSource)
    {
        _typeMappingSource = typeMappingSource;
    }

    /// <summary>
    /// Builds an INSERT statement for a batch of entities using VALUES format.
    /// </summary>
    public string Build<TEntity>(
        IReadOnlyList<TEntity> entities,
        EntityPropertyInfo propertyInfo,
        Dictionary<string, object> settings) where TEntity : class
    {
        if (entities.Count == 0)
        {
            return string.Empty;
        }

        var sb = new StringBuilder();

        // Build INSERT INTO clause
        sb.Append("INSERT INTO ");
        sb.Append(propertyInfo.QuotedTableName);
        sb.Append(" (");
        sb.Append(propertyInfo.ColumnList);
        sb.Append(')');

        // Append settings before VALUES (ClickHouse requires this order)
        AppendSettings(sb, settings);

        sb.Append(" VALUES ");

        // Build VALUES for each entity
        for (var i = 0; i < entities.Count; i++)
        {
            if (i > 0)
            {
                sb.Append(", ");
            }

            AppendEntityValues(sb, entities[i], propertyInfo.Properties);
        }

        return sb.ToString();
    }

    private void AppendEntityValues<TEntity>(
        StringBuilder sb,
        TEntity entity,
        List<PropertyMapping> properties) where TEntity : class
    {
        sb.Append('(');

        for (var i = 0; i < properties.Count; i++)
        {
            if (i > 0)
            {
                sb.Append(", ");
            }

            var property = properties[i];
            var value = property.PropertyInfo.GetValue(entity);

            AppendValue(sb, value, property.TypeMapping);
        }

        sb.Append(')');
    }

    private void AppendValue(StringBuilder sb, object? value, RelationalTypeMapping typeMapping)
    {
        if (value is null)
        {
            sb.Append("NULL");
            return;
        }

        // Handle Nested type mappings specially
        if (typeMapping is ClickHouseNestedTypeMapping nestedMapping)
        {
            AppendNestedValue(sb, value, nestedMapping);
            return;
        }

        // Use the type mapping's GenerateSqlLiteral for proper escaping
        sb.Append(typeMapping.GenerateSqlLiteral(value));
    }

    private void AppendNestedValue(StringBuilder sb, object value, ClickHouseNestedTypeMapping nestedMapping)
    {
        // Nested types are stored as parallel arrays in ClickHouse
        // For a Nested(ID UInt32, Name String) with value [{ID:1, Name:"a"}, {ID:2, Name:"b"}]
        // We output: [1, 2], ['a', 'b']

        var items = new List<object>();
        foreach (var item in (IEnumerable)value)
        {
            if (item is not null)
            {
                items.Add(item);
            }
        }

        var fieldMappings = nestedMapping.FieldMappings;

        for (var fieldIndex = 0; fieldIndex < fieldMappings.Count; fieldIndex++)
        {
            if (fieldIndex > 0)
            {
                sb.Append(", ");
            }

            var (_, propertyInfo, fieldMapping) = fieldMappings[fieldIndex];

            sb.Append('[');

            for (var itemIndex = 0; itemIndex < items.Count; itemIndex++)
            {
                if (itemIndex > 0)
                {
                    sb.Append(", ");
                }

                var fieldValue = propertyInfo.GetValue(items[itemIndex]);
                if (fieldValue is null)
                {
                    sb.Append("NULL");
                }
                else
                {
                    sb.Append(fieldMapping.GenerateSqlLiteral(fieldValue));
                }
            }

            sb.Append(']');
        }
    }

    private static void AppendSettings(StringBuilder sb, Dictionary<string, object> settings)
    {
        if (settings.Count == 0)
        {
            return;
        }

        sb.Append(" SETTINGS ");

        var first = true;
        foreach (var kvp in settings)
        {
            if (!first)
            {
                sb.Append(", ");
            }
            first = false;

            ValidateSettingKey(kvp.Key);
            sb.Append(kvp.Key);
            sb.Append(" = ");

            if (kvp.Value is string s)
            {
                sb.Append('\'');
                sb.Append(s.Replace("\\", "\\\\").Replace("'", "\\'"));
                sb.Append('\'');
            }
            else if (kvp.Value is bool b)
            {
                sb.Append(b ? '1' : '0');
            }
            else
            {
                sb.Append(kvp.Value);
            }
        }
    }

    /// <summary>
    /// SETTINGS keys are emitted into raw SQL. Reject anything that isn't a
    /// plain ClickHouse identifier so a malicious or malformed key like
    /// <c>"foo, x = (SELECT 1) --"</c> can't inject DDL into the bulk insert.
    /// </summary>
    private static void ValidateSettingKey(string key)
    {
        if (string.IsNullOrEmpty(key))
        {
            throw new ArgumentException("Bulk-insert SETTINGS key cannot be null or empty.", nameof(key));
        }

        if (!IsValidIdentifier(key))
        {
            throw new ArgumentException(
                $"Bulk-insert SETTINGS key '{key}' is not a valid ClickHouse identifier. " +
                "Keys must match [A-Za-z_][A-Za-z0-9_]*.",
                nameof(key));
        }
    }

    private static bool IsValidIdentifier(string value)
    {
        if (value.Length == 0) return false;
        var first = value[0];
        if (!(char.IsAsciiLetter(first) || first == '_')) return false;
        for (var i = 1; i < value.Length; i++)
        {
            var c = value[i];
            if (!(char.IsAsciiLetterOrDigit(c) || c == '_')) return false;
        }
        return true;
    }
}
