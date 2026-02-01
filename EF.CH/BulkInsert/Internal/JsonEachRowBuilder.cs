using System.Collections;
using System.Text;
using System.Text.Json;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.BulkInsert.Internal;

/// <summary>
/// Builds INSERT statements with JSONEachRow format for bulk insert operations.
/// </summary>
internal sealed class JsonEachRowBuilder
{
    /// <summary>
    /// Builds an INSERT statement for a batch of entities using JSONEachRow format.
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

        // Build INSERT INTO clause with FORMAT
        sb.Append("INSERT INTO ");
        sb.Append(propertyInfo.QuotedTableName);
        sb.Append(" (");
        sb.Append(propertyInfo.ColumnList);
        sb.Append(')');

        // Append settings before FORMAT
        AppendSettings(sb, settings);

        sb.Append(" FORMAT JSONEachRow");
        sb.AppendLine();

        // Build JSON object for each entity
        foreach (var entity in entities)
        {
            AppendEntityJson(sb, entity, propertyInfo.Properties);
            sb.AppendLine();
        }

        return sb.ToString();
    }

    private static void AppendEntityJson<TEntity>(
        StringBuilder sb,
        TEntity entity,
        List<PropertyMapping> properties) where TEntity : class
    {
        sb.Append('{');

        for (var i = 0; i < properties.Count; i++)
        {
            if (i > 0)
            {
                sb.Append(',');
            }

            var property = properties[i];
            var value = property.PropertyInfo.GetValue(entity);

            // Use column name without quotes for JSON key
            sb.Append('"');
            sb.Append(property.ColumnName);
            sb.Append("\":");

            AppendJsonValue(sb, value, property.TypeMapping);
        }

        sb.Append('}');
    }

    private static void AppendJsonValue(StringBuilder sb, object? value, RelationalTypeMapping typeMapping)
    {
        if (value is null)
        {
            sb.Append("null");
            return;
        }

        // Handle Nested types specially
        if (typeMapping is ClickHouseNestedTypeMapping nestedMapping)
        {
            AppendNestedJsonValue(sb, value, nestedMapping);
            return;
        }

        // Handle by CLR type
        switch (value)
        {
            case bool b:
                sb.Append(b ? "true" : "false");
                break;

            case string s:
                sb.Append(JsonSerializer.Serialize(s));
                break;

            case DateTime dt:
                sb.Append('"');
                sb.Append(dt.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                sb.Append('"');
                break;

            case DateTimeOffset dto:
                sb.Append('"');
                sb.Append(dto.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                sb.Append('"');
                break;

            case DateOnly d:
                sb.Append('"');
                sb.Append(d.ToString("yyyy-MM-dd"));
                sb.Append('"');
                break;

            case TimeOnly t:
                sb.Append('"');
                sb.Append(t.ToString("HH:mm:ss.fff"));
                sb.Append('"');
                break;

            case Guid g:
                sb.Append('"');
                sb.Append(g.ToString());
                sb.Append('"');
                break;

            case decimal m:
                sb.Append(m.ToString(System.Globalization.CultureInfo.InvariantCulture));
                break;

            case float f:
                sb.Append(f.ToString(System.Globalization.CultureInfo.InvariantCulture));
                break;

            case double d:
                sb.Append(d.ToString(System.Globalization.CultureInfo.InvariantCulture));
                break;

            case sbyte or short or int or long:
                sb.Append(value);
                break;

            case byte or ushort or uint or ulong:
                sb.Append(value);
                break;

            case IDictionary dict:
                AppendDictionary(sb, dict);
                break;

            case IEnumerable enumerable when value is not string:
                AppendArray(sb, enumerable);
                break;

            default:
                // Fallback to JSON serialization
                sb.Append(JsonSerializer.Serialize(value));
                break;
        }
    }

    private static void AppendNestedJsonValue(StringBuilder sb, object value, ClickHouseNestedTypeMapping nestedMapping)
    {
        // For JSONEachRow, Nested types are arrays of objects
        var items = new List<object>();
        foreach (var item in (IEnumerable)value)
        {
            if (item is not null)
            {
                items.Add(item);
            }
        }

        sb.Append('[');

        for (var itemIndex = 0; itemIndex < items.Count; itemIndex++)
        {
            if (itemIndex > 0)
            {
                sb.Append(',');
            }

            sb.Append('{');

            var fieldMappings = nestedMapping.FieldMappings;
            for (var fieldIndex = 0; fieldIndex < fieldMappings.Count; fieldIndex++)
            {
                if (fieldIndex > 0)
                {
                    sb.Append(',');
                }

                var (fieldName, propertyInfo, fieldMapping) = fieldMappings[fieldIndex];
                var fieldValue = propertyInfo.GetValue(items[itemIndex]);

                sb.Append('"');
                sb.Append(fieldName);
                sb.Append("\":");

                AppendJsonValue(sb, fieldValue, fieldMapping);
            }

            sb.Append('}');
        }

        sb.Append(']');
    }

    private static void AppendArray(StringBuilder sb, IEnumerable enumerable)
    {
        sb.Append('[');
        var first = true;
        foreach (var item in enumerable)
        {
            if (!first)
            {
                sb.Append(',');
            }
            first = false;

            if (item is null)
            {
                sb.Append("null");
            }
            else
            {
                sb.Append(JsonSerializer.Serialize(item));
            }
        }
        sb.Append(']');
    }

    private static void AppendDictionary(StringBuilder sb, IDictionary dict)
    {
        sb.Append('{');
        var first = true;
        foreach (DictionaryEntry entry in dict)
        {
            if (!first)
            {
                sb.Append(',');
            }
            first = false;

            sb.Append(JsonSerializer.Serialize(entry.Key?.ToString() ?? ""));
            sb.Append(':');

            if (entry.Value is null)
            {
                sb.Append("null");
            }
            else
            {
                sb.Append(JsonSerializer.Serialize(entry.Value));
            }
        }
        sb.Append('}');
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
}
