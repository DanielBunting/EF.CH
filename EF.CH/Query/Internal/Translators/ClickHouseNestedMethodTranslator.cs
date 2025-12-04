using System.Reflection;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates .NET collection methods on Nested types to ClickHouse SQL functions.
/// </summary>
/// <remarks>
/// Supported translations:
/// - nested.Count → length("Column.FirstField")
/// - Enumerable.Count(nested) → length("Column.FirstField")
/// - Enumerable.Any(nested) → notEmpty("Column.FirstField")
///
/// ClickHouse Nested types are stored as parallel arrays, so we use the first field's
/// array for length/notEmpty operations since all parallel arrays have the same length.
/// </remarks>
public class ClickHouseNestedMethodTranslator : IMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    // Enumerable.Any<TSource>(IEnumerable<TSource>)
    private static readonly MethodInfo EnumerableAnyWithoutPredicateMethod =
        typeof(Enumerable).GetMethods()
            .First(m => m.Name == nameof(Enumerable.Any) && m.GetParameters().Length == 1);

    // Enumerable.Count<TSource>(IEnumerable<TSource>)
    private static readonly MethodInfo EnumerableCountWithoutPredicateMethod =
        typeof(Enumerable).GetMethods()
            .First(m => m.Name == nameof(Enumerable.Count) && m.GetParameters().Length == 1);

    public ClickHouseNestedMethodTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // Handle static Enumerable methods
        if (method.DeclaringType == typeof(Enumerable))
        {
            return TranslateEnumerableMethod(method, arguments);
        }

        return null;
    }

    private SqlExpression? TranslateEnumerableMethod(
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments)
    {
        if (arguments.Count == 0)
        {
            return null;
        }

        var source = arguments[0];

        // Check if the source is a Nested type
        if (!IsNestedType(source))
        {
            return null;
        }

        // Get the first field column for operations
        var firstFieldColumn = GetFirstFieldColumn(source);
        if (firstFieldColumn is null)
        {
            return null;
        }

        // Enumerable.Any(nested) → notEmpty("Column.FirstField")
        if (method.IsGenericMethod &&
            method.GetGenericMethodDefinition() == EnumerableAnyWithoutPredicateMethod)
        {
            return _sqlExpressionFactory.Function(
                "notEmpty",
                new[] { firstFieldColumn },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(bool));
        }

        // Enumerable.Count(nested) → length("Column.FirstField")
        if (method.IsGenericMethod &&
            method.GetGenericMethodDefinition() == EnumerableCountWithoutPredicateMethod)
        {
            return _sqlExpressionFactory.Function(
                "length",
                new[] { firstFieldColumn },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(int));
        }

        return null;
    }

    /// <summary>
    /// Checks if the expression represents a Nested type.
    /// </summary>
    private static bool IsNestedType(SqlExpression? expression)
    {
        return expression?.TypeMapping is ClickHouseNestedTypeMapping;
    }

    /// <summary>
    /// Gets an expression for the first field of a Nested column.
    /// For a Nested column "Goals" with fields (ID, EventTime), this returns
    /// an expression for "Goals.ID".
    /// </summary>
    private SqlExpression? GetFirstFieldColumn(SqlExpression nestedExpression)
    {
        if (nestedExpression.TypeMapping is not ClickHouseNestedTypeMapping nestedMapping)
        {
            return null;
        }

        if (nestedMapping.FieldMappings.Count == 0)
        {
            return null;
        }

        // Get the column name from the expression
        string? columnName = nestedExpression switch
        {
            ColumnExpression column => column.Name,
            _ => null
        };

        if (columnName is null)
        {
            return null;
        }

        // Get the first field name
        var firstField = nestedMapping.FieldMappings[0];
        var subColumnName = $"{columnName}.{firstField.Name}";

        // Create a SQL fragment for the sub-column with proper quoting
        // ClickHouse uses "Column.Field" syntax for nested field access
        return _sqlExpressionFactory.Fragment($"\"{subColumnName}\"");
    }
}

/// <summary>
/// Translates member access on Nested types (Count property) to ClickHouse functions.
/// </summary>
public class ClickHouseNestedMemberTranslator : IMemberTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    public ClickHouseNestedMemberTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is null)
        {
            return null;
        }

        // Check if the instance is a Nested type
        if (instance.TypeMapping is not ClickHouseNestedTypeMapping nestedMapping)
        {
            return null;
        }

        // List<T>.Count → length("Column.FirstField")
        if (member.Name == "Count" && nestedMapping.FieldMappings.Count > 0)
        {
            var firstFieldColumn = GetFirstFieldColumn(instance, nestedMapping);
            if (firstFieldColumn is not null)
            {
                return _sqlExpressionFactory.Function(
                    "length",
                    new[] { firstFieldColumn },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    typeof(int));
            }
        }

        return null;
    }

    /// <summary>
    /// Gets an expression for the first field of a Nested column.
    /// </summary>
    private SqlExpression? GetFirstFieldColumn(SqlExpression nestedExpression, ClickHouseNestedTypeMapping nestedMapping)
    {
        // Get the column name from the expression
        string? columnName = nestedExpression switch
        {
            ColumnExpression column => column.Name,
            _ => null
        };

        if (columnName is null)
        {
            return null;
        }

        // Get the first field name
        var firstField = nestedMapping.FieldMappings[0];
        var subColumnName = $"{columnName}.{firstField.Name}";

        // Create a SQL fragment for the sub-column with proper quoting
        return _sqlExpressionFactory.Fragment($"\"{subColumnName}\"");
    }
}
