using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EF.CH.Query.Internal;

/// <summary>
/// Translates LINQ expressions into ClickHouse SQL for materialized view definitions.
/// This is a design-time translator that processes expressions during OnModelCreating.
/// </summary>
public class MaterializedViewSqlTranslator
{
    private readonly IModel _model;
    private readonly string _sourceTableName;
    private readonly StringBuilder _sql = new();
    private readonly List<string> _selectColumns = [];
    private readonly List<string> _groupByColumns = [];

    /// <summary>
    /// Creates a new translator for the given model and source table.
    /// </summary>
    public MaterializedViewSqlTranslator(IModel model, string sourceTableName)
    {
        _model = model;
        _sourceTableName = sourceTableName;
    }

    /// <summary>
    /// Translates a LINQ query expression into a ClickHouse SELECT statement.
    /// </summary>
    /// <typeparam name="TSource">The source entity type.</typeparam>
    /// <typeparam name="TResult">The result entity type.</typeparam>
    /// <param name="queryExpression">The LINQ query expression.</param>
    /// <returns>The translated SELECT SQL.</returns>
    public string Translate<TSource, TResult>(
        Expression<Func<IQueryable<TSource>, IQueryable<TResult>>> queryExpression)
        where TSource : class
        where TResult : class
    {
        var visitor = new MaterializedViewExpressionVisitor<TSource>(
            _model,
            _sourceTableName);

        return visitor.Translate(queryExpression);
    }
}

/// <summary>
/// Visits LINQ expression trees and builds ClickHouse SQL.
/// </summary>
/// <typeparam name="TSource">The source entity type.</typeparam>
internal class MaterializedViewExpressionVisitor<TSource> : ExpressionVisitor
    where TSource : class
{
    private readonly IModel _model;
    private readonly string _sourceTableName;
    private readonly IEntityType? _sourceEntityType;
    private readonly StringBuilder _selectSql = new();
    private readonly List<string> _selectColumns = [];
    private readonly List<string> _groupByColumns = [];
    private string? _groupByParameter;
    private readonly Dictionary<string, string> _groupKeyMappings = new();

    public MaterializedViewExpressionVisitor(IModel model, string sourceTableName)
    {
        _model = model;
        _sourceTableName = sourceTableName;
        _sourceEntityType = model.FindEntityType(typeof(TSource));
    }

    /// <summary>
    /// Translates the full query expression to SQL.
    /// </summary>
    public string Translate<TResult>(
        Expression<Func<IQueryable<TSource>, IQueryable<TResult>>> queryExpression)
    {
        // The expression body should be a method chain on the IQueryable parameter
        Visit(queryExpression.Body);

        var sql = new StringBuilder();
        sql.Append("SELECT ");
        sql.Append(string.Join(", ", _selectColumns));
        sql.AppendLine();
        sql.Append("FROM ");
        sql.Append(QuoteIdentifier(_sourceTableName));

        if (_groupByColumns.Count > 0)
        {
            sql.AppendLine();
            sql.Append("GROUP BY ");
            sql.Append(string.Join(", ", _groupByColumns));
        }

        return sql.ToString();
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Handle LINQ methods: GroupBy, Select
        if (node.Method.DeclaringType == typeof(Queryable) ||
            node.Method.DeclaringType == typeof(Enumerable))
        {
            switch (node.Method.Name)
            {
                case "GroupBy":
                    return VisitGroupBy(node);
                case "Select":
                    return VisitSelect(node);
            }
        }

        return base.VisitMethodCall(node);
    }

    private Expression VisitGroupBy(MethodCallExpression node)
    {
        // Visit the source first (IQueryable<TSource>)
        Visit(node.Arguments[0]);

        // Get the key selector lambda
        if (node.Arguments[1] is UnaryExpression { Operand: LambdaExpression keySelector })
        {
            VisitGroupByKeySelector(keySelector);
        }

        return node;
    }

    private void VisitGroupByKeySelector(LambdaExpression keySelector)
    {
        var body = keySelector.Body;

        // Handle anonymous type: new { x.Date, x.ProductId }
        if (body is NewExpression newExpr)
        {
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var memberName = newExpr.Members?[i].Name;
                var columnSql = TranslateExpression(newExpr.Arguments[i]);
                _groupByColumns.Add(columnSql);

                if (memberName != null)
                {
                    _groupKeyMappings[memberName] = columnSql;
                }
            }
        }
        // Handle single column: x.Id
        else if (body is MemberExpression memberExpr)
        {
            var columnSql = TranslateMemberAccess(memberExpr);
            _groupByColumns.Add(columnSql);
            _groupKeyMappings[memberExpr.Member.Name] = columnSql;
        }
        // Handle member init: new KeyClass { Date = x.Date }
        else if (body is MemberInitExpression memberInit)
        {
            foreach (var binding in memberInit.Bindings)
            {
                if (binding is MemberAssignment assignment)
                {
                    var columnSql = TranslateExpression(assignment.Expression);
                    _groupByColumns.Add(columnSql);
                    _groupKeyMappings[assignment.Member.Name] = columnSql;
                }
            }
        }
    }

    private Expression VisitSelect(MethodCallExpression node)
    {
        // Visit the source first (could be GroupBy result)
        Visit(node.Arguments[0]);

        // Get the result selector lambda
        if (node.Arguments[1] is UnaryExpression { Operand: LambdaExpression resultSelector })
        {
            _groupByParameter = resultSelector.Parameters[0].Name;
            VisitSelectProjection(resultSelector);
        }

        return node;
    }

    private void VisitSelectProjection(LambdaExpression resultSelector)
    {
        var body = resultSelector.Body;

        // Handle new TResult { ... }
        if (body is MemberInitExpression memberInit)
        {
            foreach (var binding in memberInit.Bindings)
            {
                if (binding is MemberAssignment assignment)
                {
                    var columnSql = TranslateExpression(assignment.Expression);
                    var alias = assignment.Member.Name;
                    _selectColumns.Add($"{columnSql} AS {QuoteIdentifier(alias)}");
                }
            }
        }
        // Handle anonymous type: new { g.Key.Date, Total = g.Sum(...) }
        else if (body is NewExpression newExpr)
        {
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var memberName = newExpr.Members?[i].Name ?? $"Column{i}";
                var columnSql = TranslateExpression(newExpr.Arguments[i]);
                _selectColumns.Add($"{columnSql} AS {QuoteIdentifier(memberName)}");
            }
        }
    }

    private string TranslateExpression(Expression expr)
    {
        return expr switch
        {
            MemberExpression memberExpr => TranslateMemberAccess(memberExpr),
            MethodCallExpression methodExpr => TranslateMethodCall(methodExpr),
            ConstantExpression constExpr => TranslateConstant(constExpr),
            BinaryExpression binaryExpr => TranslateBinary(binaryExpr),
            UnaryExpression unaryExpr => TranslateUnary(unaryExpr),
            ConditionalExpression condExpr => TranslateConditional(condExpr),
            DefaultExpression defaultExpr => TranslateDefault(defaultExpr),
            _ => throw new NotSupportedException(
                $"Expression type {expr.GetType().Name} is not supported in materialized view definitions.")
        };
    }

    private string TranslateDefault(DefaultExpression defaultExpr)
    {
        // Default values for common types
        return defaultExpr.Type.Name switch
        {
            "Int64" or "Int32" or "Int16" or "SByte" => "0",
            "UInt64" or "UInt32" or "UInt16" or "Byte" => "0",
            "Double" or "Single" or "Decimal" => "0.0",
            "String" => "''",
            _ => "NULL"
        };
    }

    private string TranslateMemberAccess(MemberExpression memberExpr)
    {
        // Handle static field access (e.g., UInt64.MaxValue)
        if (memberExpr.Expression == null)
        {
            // Static member access
            return memberExpr.Member switch
            {
                FieldInfo { Name: "MaxValue", DeclaringType.Name: "UInt64" } => "18446744073709551615",
                FieldInfo { Name: "MinValue", DeclaringType.Name: "UInt64" } => "0",
                FieldInfo { Name: "MaxValue", DeclaringType.Name: "Int64" } => "9223372036854775807",
                FieldInfo { Name: "MinValue", DeclaringType.Name: "Int64" } => "-9223372036854775808",
                _ => throw new NotSupportedException(
                    $"Static member {memberExpr.Member.DeclaringType?.Name}.{memberExpr.Member.Name} is not supported.")
            };
        }

        // Check if this is accessing the grouping key (g.Key.X)
        if (memberExpr.Expression is MemberExpression parentMember)
        {
            // g.Key.PropertyName
            if (parentMember.Member.Name == "Key" &&
                parentMember.Expression is ParameterExpression param &&
                param.Name == _groupByParameter)
            {
                if (_groupKeyMappings.TryGetValue(memberExpr.Member.Name, out var keySql))
                {
                    return keySql;
                }
            }
        }

        // Check if this is accessing the source entity
        if (memberExpr.Expression is ParameterExpression sourceParam)
        {
            // Direct column access: x.ColumnName
            return GetColumnName(memberExpr.Member);
        }

        // Handle DateTime.Date property
        if (memberExpr.Member.Name == "Date" &&
            memberExpr.Member.DeclaringType == typeof(DateTime))
        {
            var innerSql = TranslateExpression(memberExpr.Expression!);
            return $"toDate({innerSql})";
        }

        // Handle other DateTime properties
        if (memberExpr.Member.DeclaringType == typeof(DateTime))
        {
            var innerSql = TranslateExpression(memberExpr.Expression!);
            return memberExpr.Member.Name switch
            {
                "Year" => $"toYear({innerSql})",
                "Month" => $"toMonth({innerSql})",
                "Day" => $"toDayOfMonth({innerSql})",
                "Hour" => $"toHour({innerSql})",
                "Minute" => $"toMinute({innerSql})",
                "Second" => $"toSecond({innerSql})",
                _ => throw new NotSupportedException($"DateTime.{memberExpr.Member.Name} is not supported.")
            };
        }

        // Could be a nested property - try to translate the full path
        if (memberExpr.Expression != null)
        {
            // This might be x.SomeProperty where x is from GroupBy lambda
            return GetColumnName(memberExpr.Member);
        }

        throw new NotSupportedException(
            $"Member access {memberExpr.Member.Name} could not be translated.");
    }

    private string TranslateMethodCall(MethodCallExpression methodExpr)
    {
        // Handle aggregate methods on IGrouping
        if (methodExpr.Method.DeclaringType == typeof(Enumerable))
        {
            return methodExpr.Method.Name switch
            {
                "Sum" => TranslateAggregate("sum", methodExpr),
                "Count" => TranslateCount(methodExpr),
                "Average" or "Avg" => TranslateAggregate("avg", methodExpr),
                "Min" => TranslateAggregate("min", methodExpr),
                "Max" => TranslateAggregate("max", methodExpr),
                _ => throw new NotSupportedException($"Method {methodExpr.Method.Name} is not supported.")
            };
        }

        // Handle ClickHouse extension methods
        if (methodExpr.Method.DeclaringType?.FullName == "EF.CH.Extensions.ClickHouseFunctions")
        {
            return TranslateClickHouseFunction(methodExpr);
        }

        throw new NotSupportedException($"Method {methodExpr.Method.Name} is not supported.");
    }

    private string TranslateAggregate(string function, MethodCallExpression methodExpr)
    {
        // Sum(x => x.Value) has 2 arguments: source and selector
        if (methodExpr.Arguments.Count >= 2 &&
            methodExpr.Arguments[1] is UnaryExpression { Operand: LambdaExpression selector })
        {
            var innerSql = TranslateExpression(selector.Body);
            return $"{function}({innerSql})";
        }

        // Sum() with no selector - sum all values (rarely used)
        return $"{function}()";
    }

    private string TranslateCount(MethodCallExpression methodExpr)
    {
        // Count() with predicate
        if (methodExpr.Arguments.Count >= 2 &&
            methodExpr.Arguments[1] is UnaryExpression { Operand: LambdaExpression predicate })
        {
            var condition = TranslateExpression(predicate.Body);
            return $"countIf({condition})";
        }

        // Simple Count()
        return "count()";
    }

    private string TranslateClickHouseFunction(MethodCallExpression methodExpr)
    {
        var functionName = methodExpr.Method.Name;
        var arg = methodExpr.Arguments.Count > 0
            ? TranslateExpression(methodExpr.Arguments[0])
            : throw new InvalidOperationException($"ClickHouse function {functionName} requires an argument.");

        return functionName switch
        {
            "ToYYYYMM" => $"toYYYYMM({arg})",
            "ToYYYYMMDD" => $"toYYYYMMDD({arg})",
            "ToStartOfHour" => $"toStartOfHour({arg})",
            "ToStartOfDay" => $"toStartOfDay({arg})",
            "ToStartOfMonth" => $"toStartOfMonth({arg})",
            "ToStartOfYear" => $"toStartOfYear({arg})",
            "ToStartOfWeek" => $"toStartOfWeek({arg})",
            "ToStartOfQuarter" => $"toStartOfQuarter({arg})",
            "ToStartOfMinute" => $"toStartOfMinute({arg})",
            "ToStartOfFiveMinutes" => $"toStartOfFiveMinutes({arg})",
            "ToStartOfFifteenMinutes" => $"toStartOfFifteenMinutes({arg})",
            "ToUnixTimestamp64Milli" => $"toUnixTimestamp64Milli({arg})",
            "CityHash64" => $"cityHash64({arg})",
            "ToISOYear" => $"toISOYear({arg})",
            "ToISOWeek" => $"toISOWeek({arg})",
            "ToDayOfWeek" => $"toDayOfWeek({arg})",
            "ToDayOfYear" => $"toDayOfYear({arg})",
            "ToQuarter" => $"toQuarter({arg})",
            _ => throw new NotSupportedException($"ClickHouse function {functionName} is not supported.")
        };
    }

    private string TranslateConstant(ConstantExpression constExpr)
    {
        return constExpr.Value switch
        {
            null => "NULL",
            string s => $"'{s.Replace("'", "''")}'",
            bool b => b ? "1" : "0",
            sbyte sb => $"toInt8({sb})",
            byte b => $"toUInt8({b})",
            ulong ul => ul.ToString(),
            long l => l.ToString(),
            DateTime dt => $"toDateTime64('{dt:yyyy-MM-dd HH:mm:ss.fff}', 3)",
            IFormattable f => f.ToString(null, System.Globalization.CultureInfo.InvariantCulture),
            _ => constExpr.Value.ToString() ?? "NULL"
        };
    }

    private string TranslateBinary(BinaryExpression binaryExpr)
    {
        var left = TranslateExpression(binaryExpr.Left);
        var right = TranslateExpression(binaryExpr.Right);

        var op = binaryExpr.NodeType switch
        {
            ExpressionType.Add => "+",
            ExpressionType.Subtract => "-",
            ExpressionType.Multiply => "*",
            ExpressionType.Divide => "/",
            ExpressionType.Modulo => "%",
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "!=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.AndAlso => "AND",
            ExpressionType.OrElse => "OR",
            _ => throw new NotSupportedException($"Binary operator {binaryExpr.NodeType} is not supported.")
        };

        return $"({left} {op} {right})";
    }

    private string TranslateUnary(UnaryExpression unaryExpr)
    {
        if (unaryExpr.NodeType == ExpressionType.Convert ||
            unaryExpr.NodeType == ExpressionType.ConvertChecked)
        {
            // Often just unwrap conversions
            return TranslateExpression(unaryExpr.Operand);
        }

        if (unaryExpr.NodeType == ExpressionType.Not)
        {
            return $"NOT ({TranslateExpression(unaryExpr.Operand)})";
        }

        if (unaryExpr.NodeType == ExpressionType.Negate)
        {
            return $"-({TranslateExpression(unaryExpr.Operand)})";
        }

        throw new NotSupportedException($"Unary operator {unaryExpr.NodeType} is not supported.");
    }

    private string TranslateConditional(ConditionalExpression condExpr)
    {
        var test = TranslateExpression(condExpr.Test);
        var ifTrue = TranslateExpression(condExpr.IfTrue);
        var ifFalse = TranslateExpression(condExpr.IfFalse);

        return $"if({test}, {ifTrue}, {ifFalse})";
    }

    private string GetColumnName(MemberInfo member)
    {
        // Try to get the column name from EF Core model
        if (_sourceEntityType != null)
        {
            var property = _sourceEntityType.FindProperty(member.Name);
            if (property != null)
            {
                var columnName = property.GetColumnName() ?? member.Name;
                return QuoteIdentifier(columnName);
            }
        }

        // Fall back to member name
        return QuoteIdentifier(member.Name);
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\\\"")}\"";
    }
}
