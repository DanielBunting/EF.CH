using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EF.CH.Query.Internal;

/// <summary>
/// Translates LINQ expressions into ClickHouse SQL for projection definitions.
/// This is a design-time translator that processes expressions during OnModelCreating.
/// Unlike materialized views, projections don't need a FROM clause as they operate
/// on the parent table implicitly.
/// </summary>
public class ProjectionSqlTranslator
{
    private readonly IModel _model;
    private readonly string _tableName;

    /// <summary>
    /// Creates a new translator for the given model and source table.
    /// </summary>
    public ProjectionSqlTranslator(IModel model, string tableName)
    {
        _model = model;
        _tableName = tableName;
    }

    /// <summary>
    /// Translates a LINQ query expression into a ClickHouse projection SELECT statement.
    /// </summary>
    /// <typeparam name="TSource">The source entity type.</typeparam>
    /// <typeparam name="TResult">The result entity type.</typeparam>
    /// <param name="queryExpression">The LINQ query expression.</param>
    /// <returns>The translated SELECT SQL (without FROM clause).</returns>
    public string Translate<TSource, TResult>(
        Expression<Func<IQueryable<TSource>, IQueryable<TResult>>> queryExpression)
        where TSource : class
        where TResult : class
    {
        var visitor = new ProjectionExpressionVisitor<TSource>(
            _model,
            _tableName);

        return visitor.Translate(queryExpression);
    }

    /// <summary>
    /// Translates separate GroupBy and Select expressions into a ClickHouse projection SELECT statement.
    /// </summary>
    /// <typeparam name="TSource">The source entity type.</typeparam>
    /// <typeparam name="TKey">The grouping key type.</typeparam>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="keySelector">The GROUP BY key selector expression.</param>
    /// <param name="resultSelector">The SELECT result selector expression.</param>
    /// <returns>The translated SELECT SQL (without FROM clause).</returns>
    public string TranslateAggregation<TSource, TKey, TResult>(
        Expression<Func<TSource, TKey>> keySelector,
        Expression<Func<IGrouping<TKey, TSource>, TResult>> resultSelector)
        where TSource : class
    {
        var visitor = new ProjectionExpressionVisitor<TSource>(
            _model,
            _tableName);

        return visitor.TranslateAggregation(keySelector, resultSelector);
    }
}

/// <summary>
/// Visits LINQ expression trees and builds ClickHouse projection SQL.
/// Key difference from MaterializedViewExpressionVisitor: no FROM clause is generated.
/// </summary>
/// <typeparam name="TSource">The source entity type.</typeparam>
internal class ProjectionExpressionVisitor<TSource> : ExpressionVisitor
    where TSource : class
{
    private readonly IModel _model;
    private readonly string _tableName;
    private readonly IEntityType? _sourceEntityType;
    private readonly List<string> _selectColumns = [];
    private readonly List<string> _groupByColumns = [];
    private string? _groupByParameter;
    private readonly Dictionary<string, string> _groupKeyMappings = new();

    public ProjectionExpressionVisitor(IModel model, string tableName)
    {
        _model = model;
        _tableName = tableName;
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

        return BuildSql();
    }

    /// <summary>
    /// Translates separate GroupBy and Select expressions to SQL.
    /// </summary>
    public string TranslateAggregation<TKey, TResult>(
        Expression<Func<TSource, TKey>> keySelector,
        Expression<Func<IGrouping<TKey, TSource>, TResult>> resultSelector)
    {
        // Process keySelector to extract GROUP BY columns
        VisitGroupByKeySelector(keySelector);

        // Process resultSelector to extract SELECT columns
        _groupByParameter = resultSelector.Parameters[0].Name;
        VisitSelectProjection(resultSelector);

        return BuildSql();
    }

    private string BuildSql()
    {
        var sql = new StringBuilder();
        sql.Append("SELECT ");
        sql.Append(string.Join(", ", _selectColumns));

        // Note: No FROM clause for projections (they operate on parent table implicitly)

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
        // Handle single column/expression: x.Id or x.Date.Date
        else if (body is MemberExpression memberExpr)
        {
            var columnSql = TranslateMemberAccess(memberExpr);
            _groupByColumns.Add(columnSql);
            _groupKeyMappings[memberExpr.Member.Name] = columnSql;
            // Also store with empty key for direct g.Key access
            _groupKeyMappings[""] = columnSql;
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
                $"Expression type {expr.GetType().Name} is not supported in projection definitions.")
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

        // Check if this is accessing g.Key directly (single-value key)
        if (memberExpr.Member.Name == "Key" &&
            memberExpr.Expression is ParameterExpression keyParam &&
            keyParam.Name == _groupByParameter)
        {
            // For single-value keys, the key expression is stored with an empty string key
            if (_groupKeyMappings.TryGetValue("", out var singleKeySql))
            {
                return singleKeySql;
            }
            // Fallback: if there's exactly one entry, use it
            if (_groupKeyMappings.Count == 1)
            {
                return _groupKeyMappings.Values.First();
            }
        }

        // Check if this is accessing the source entity
        if (memberExpr.Expression is ParameterExpression)
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

        // Handle ClickHouse aggregate functions
        if (methodExpr.Method.DeclaringType?.FullName == "EF.CH.Extensions.ClickHouseAggregates")
        {
            return TranslateClickHouseAggregate(methodExpr);
        }

        throw new NotSupportedException($"Method {methodExpr.Method.Name} is not supported.");
    }

    private string TranslateAggregate(string function, MethodCallExpression methodExpr)
    {
        // Sum(x => x.Value) has 2 arguments: source and selector
        if (methodExpr.Arguments.Count >= 2)
        {
            var selectorArg = methodExpr.Arguments[1];

            // Lambda might be wrapped in UnaryExpression (Quote) or be direct
            LambdaExpression? selector = selectorArg switch
            {
                UnaryExpression { Operand: LambdaExpression lambda } => lambda,
                LambdaExpression lambda => lambda,
                _ => null
            };

            if (selector != null)
            {
                var innerSql = TranslateExpression(selector.Body);
                return $"{function}({innerSql})";
            }
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

    private string TranslateClickHouseAggregate(MethodCallExpression methodExpr)
    {
        var methodName = methodExpr.Method.Name;

        return methodName switch
        {
            // Phase 1 - Simple single-selector aggregates
            "Uniq" => TranslateSimpleClickHouseAggregate("uniq", methodExpr),
            "UniqExact" => TranslateSimpleClickHouseAggregate("uniqExact", methodExpr),
            "AnyValue" => TranslateSimpleClickHouseAggregate("any", methodExpr),
            "AnyLastValue" => TranslateSimpleClickHouseAggregate("anyLast", methodExpr),

            // Phase 1 - Two-selector aggregates
            "ArgMax" => TranslateTwoSelectorAggregate("argMax", methodExpr),
            "ArgMin" => TranslateTwoSelectorAggregate("argMin", methodExpr),

            // Phase 2 - Statistical aggregates
            "Median" => TranslateSimpleClickHouseAggregate("median", methodExpr),
            "StddevPop" => TranslateSimpleClickHouseAggregate("stddevPop", methodExpr),
            "StddevSamp" => TranslateSimpleClickHouseAggregate("stddevSamp", methodExpr),
            "VarPop" => TranslateSimpleClickHouseAggregate("varPop", methodExpr),
            "VarSamp" => TranslateSimpleClickHouseAggregate("varSamp", methodExpr),

            // Phase 2 - Parameterized aggregates
            "Quantile" => TranslateParametricQuantile("quantile", methodExpr),

            // Approximate count distinct
            "UniqCombined" => TranslateSimpleClickHouseAggregate("uniqCombined", methodExpr),
            "UniqCombined64" => TranslateSimpleClickHouseAggregate("uniqCombined64", methodExpr),
            "UniqHLL12" => TranslateSimpleClickHouseAggregate("uniqHLL12", methodExpr),
            "UniqTheta" => TranslateSimpleClickHouseAggregate("uniqTheta", methodExpr),

            // Quantile variants
            "QuantileTDigest" => TranslateParametricQuantile("quantileTDigest", methodExpr),
            "QuantileDD" => TranslateQuantileDD(methodExpr),
            "QuantileExact" => TranslateParametricQuantile("quantileExact", methodExpr),
            "QuantileTiming" => TranslateParametricQuantile("quantileTiming", methodExpr),

            // Multi-quantile
            "Quantiles" => TranslateMultiQuantile("quantiles", methodExpr),
            "QuantilesTDigest" => TranslateMultiQuantile("quantilesTDigest", methodExpr),

            // Phase 3 - Array aggregates
            "GroupArray" => TranslateGroupArray(methodExpr),
            "GroupUniqArray" => TranslateSimpleClickHouseAggregate("groupUniqArray", methodExpr),
            "TopK" => TranslateTopK(methodExpr),
            "TopKWeighted" => TranslateTopKWeighted(methodExpr),

            // State combinators - for AggregatingMergeTree storage
            "CountState" => "countState()",
            "SumState" => TranslateSimpleClickHouseAggregate("sumState", methodExpr),
            "AvgState" => TranslateSimpleClickHouseAggregate("avgState", methodExpr),
            "MinState" => TranslateSimpleClickHouseAggregate("minState", methodExpr),
            "MaxState" => TranslateSimpleClickHouseAggregate("maxState", methodExpr),
            "UniqState" => TranslateSimpleClickHouseAggregate("uniqState", methodExpr),
            "UniqExactState" => TranslateSimpleClickHouseAggregate("uniqExactState", methodExpr),
            "QuantileState" => TranslateQuantileState(methodExpr),
            "AnyState" => TranslateSimpleClickHouseAggregate("anyState", methodExpr),
            "AnyLastState" => TranslateSimpleClickHouseAggregate("anyLastState", methodExpr),

            // Merge combinators - for reading from AggregatingMergeTree
            "CountMerge" => TranslateSimpleClickHouseAggregate("countMerge", methodExpr),
            "SumMerge" => TranslateSimpleClickHouseAggregate("sumMerge", methodExpr),
            "AvgMerge" => TranslateSimpleClickHouseAggregate("avgMerge", methodExpr),
            "MinMerge" => TranslateSimpleClickHouseAggregate("minMerge", methodExpr),
            "MaxMerge" => TranslateSimpleClickHouseAggregate("maxMerge", methodExpr),
            "UniqMerge" => TranslateSimpleClickHouseAggregate("uniqMerge", methodExpr),
            "UniqExactMerge" => TranslateSimpleClickHouseAggregate("uniqExactMerge", methodExpr),
            "QuantileMerge" => TranslateQuantileMerge(methodExpr),
            "AnyMerge" => TranslateSimpleClickHouseAggregate("anyMerge", methodExpr),
            "AnyLastMerge" => TranslateSimpleClickHouseAggregate("anyLastMerge", methodExpr),

            // If combinators - conditional aggregation
            "CountIf" => TranslateCountIfAggregate(methodExpr),
            "SumIf" => TranslateIfAggregate("sumIf", methodExpr),
            "AvgIf" => TranslateIfAggregate("avgIf", methodExpr),
            "MinIf" => TranslateIfAggregate("minIf", methodExpr),
            "MaxIf" => TranslateIfAggregate("maxIf", methodExpr),
            "UniqIf" => TranslateIfAggregate("uniqIf", methodExpr),
            "UniqExactIf" => TranslateIfAggregate("uniqExactIf", methodExpr),
            "AnyIf" => TranslateIfAggregate("anyIf", methodExpr),
            "AnyLastIf" => TranslateIfAggregate("anyLastIf", methodExpr),
            "QuantileIf" => TranslateQuantileIfAggregate(methodExpr),

            _ => throw new NotSupportedException($"ClickHouse aggregate {methodName} is not supported.")
        };
    }

    private string TranslateSimpleClickHouseAggregate(string function, MethodCallExpression methodExpr)
    {
        // Pattern: Method(source, selector) where selector is the last argument
        var selector = ExtractLambda(methodExpr.Arguments[^1]);
        var innerSql = TranslateExpression(selector.Body);
        return $"{function}({innerSql})";
    }

    private string TranslateTwoSelectorAggregate(string function, MethodCallExpression methodExpr)
    {
        // Pattern: Method(source, argSelector, valSelector)
        var argSelector = ExtractLambda(methodExpr.Arguments[1]);
        var valSelector = ExtractLambda(methodExpr.Arguments[2]);
        var argSql = TranslateExpression(argSelector.Body);
        var valSql = TranslateExpression(valSelector.Body);
        return $"{function}({argSql}, {valSql})";
    }

    private string TranslateParametricQuantile(string functionName, MethodCallExpression methodExpr)
    {
        // Pattern: QuantileXxx(source, level, selector)
        var level = ExtractConstantValue<double>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var innerSql = TranslateExpression(selector.Body);
        return $"{functionName}({level.ToString(System.Globalization.CultureInfo.InvariantCulture)})({innerSql})";
    }

    private string TranslateGroupArray(MethodCallExpression methodExpr)
    {
        // Pattern: GroupArray(source, selector) or GroupArray(source, maxSize, selector)
        if (methodExpr.Arguments.Count == 2)
        {
            // Simple groupArray without limit
            var selector = ExtractLambda(methodExpr.Arguments[1]);
            return $"groupArray({TranslateExpression(selector.Body)})";
        }
        else
        {
            // groupArray with maxSize limit
            var maxSize = ExtractConstantValue<int>(methodExpr.Arguments[1]);
            var selector = ExtractLambda(methodExpr.Arguments[2]);
            return $"groupArray({maxSize})({TranslateExpression(selector.Body)})";
        }
    }

    private string TranslateTopK(MethodCallExpression methodExpr)
    {
        // Pattern: TopK(source, k, selector)
        var k = ExtractConstantValue<int>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var innerSql = TranslateExpression(selector.Body);
        return $"topK({k})({innerSql})";
    }

    private string TranslateQuantileState(MethodCallExpression methodExpr)
    {
        // Pattern: QuantileState(source, level, selector)
        var level = ExtractConstantValue<double>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var innerSql = TranslateExpression(selector.Body);
        return $"quantileState({level.ToString(System.Globalization.CultureInfo.InvariantCulture)})({innerSql})";
    }

    private string TranslateQuantileMerge(MethodCallExpression methodExpr)
    {
        // Pattern: QuantileMerge(source, level, stateSelector)
        var level = ExtractConstantValue<double>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var innerSql = TranslateExpression(selector.Body);
        return $"quantileMerge({level.ToString(System.Globalization.CultureInfo.InvariantCulture)})({innerSql})";
    }

    private string TranslateCountIfAggregate(MethodCallExpression methodExpr)
    {
        // Pattern: CountIf(source, predicate)
        var predicate = ExtractLambda(methodExpr.Arguments[1]);
        var conditionSql = TranslateExpression(predicate.Body);
        return $"countIf({conditionSql})";
    }

    private string TranslateIfAggregate(string function, MethodCallExpression methodExpr)
    {
        // Pattern: SumIf(source, selector, predicate) etc.
        var selector = ExtractLambda(methodExpr.Arguments[1]);
        var predicate = ExtractLambda(methodExpr.Arguments[2]);
        var valueSql = TranslateExpression(selector.Body);
        var conditionSql = TranslateExpression(predicate.Body);
        return $"{function}({valueSql}, {conditionSql})";
    }

    private string TranslateQuantileDD(MethodCallExpression methodExpr)
    {
        // Pattern: QuantileDD(source, relativeAccuracy, level, selector)
        var accuracy = ExtractConstantValue<double>(methodExpr.Arguments[1]);
        var level = ExtractConstantValue<double>(methodExpr.Arguments[2]);
        var selector = ExtractLambda(methodExpr.Arguments[3]);
        var innerSql = TranslateExpression(selector.Body);
        var accuracyStr = accuracy.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var levelStr = level.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return $"quantileDD({accuracyStr}, {levelStr})({innerSql})";
    }

    private string TranslateMultiQuantile(string functionName, MethodCallExpression methodExpr)
    {
        // Pattern: Quantiles(source, levels[], selector)
        var levels = ExtractConstantValue<double[]>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var innerSql = TranslateExpression(selector.Body);
        var levelsStr = string.Join(", ", levels.Select(l => l.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        return $"{functionName}({levelsStr})({innerSql})";
    }

    private string TranslateTopKWeighted(MethodCallExpression methodExpr)
    {
        // Pattern: TopKWeighted(source, k, selector, weightSelector)
        var k = ExtractConstantValue<int>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var weightSelector = ExtractLambda(methodExpr.Arguments[3]);
        var valueSql = TranslateExpression(selector.Body);
        var weightSql = TranslateExpression(weightSelector.Body);
        return $"topKWeighted({k})({valueSql}, {weightSql})";
    }

    private string TranslateQuantileIfAggregate(MethodCallExpression methodExpr)
    {
        // Pattern: QuantileIf(source, level, selector, predicate)
        var level = ExtractConstantValue<double>(methodExpr.Arguments[1]);
        var selector = ExtractLambda(methodExpr.Arguments[2]);
        var predicate = ExtractLambda(methodExpr.Arguments[3]);
        var valueSql = TranslateExpression(selector.Body);
        var conditionSql = TranslateExpression(predicate.Body);
        return $"quantileIf({level.ToString(System.Globalization.CultureInfo.InvariantCulture)})({valueSql}, {conditionSql})";
    }

    private static LambdaExpression ExtractLambda(Expression arg)
    {
        return arg switch
        {
            UnaryExpression { Operand: LambdaExpression lambda } => lambda,
            LambdaExpression lambda => lambda,
            _ => throw new NotSupportedException($"Cannot extract lambda from {arg.GetType().Name}")
        };
    }

    private static T ExtractConstantValue<T>(Expression arg)
    {
        // Handle constant directly
        if (arg is ConstantExpression constExpr && constExpr.Value is T value)
        {
            return value;
        }

        // Handle member access to a captured variable (closure)
        if (arg is MemberExpression memberExpr)
        {
            var container = memberExpr.Expression;
            if (container is ConstantExpression containerConst)
            {
                var field = memberExpr.Member as System.Reflection.FieldInfo;
                var prop = memberExpr.Member as System.Reflection.PropertyInfo;

                if (field != null)
                {
                    return (T)field.GetValue(containerConst.Value)!;
                }
                if (prop != null)
                {
                    return (T)prop.GetValue(containerConst.Value)!;
                }
            }
        }

        // Handle inline array initialization: new[] { 0.5, 0.9, 0.99 }
        if (arg is NewArrayExpression newArrayExpr && typeof(T).IsArray)
        {
            var elementType = typeof(T).GetElementType()!;
            var array = Array.CreateInstance(elementType, newArrayExpr.Expressions.Count);
            for (int i = 0; i < newArrayExpr.Expressions.Count; i++)
            {
                if (newArrayExpr.Expressions[i] is ConstantExpression elemConst)
                {
                    array.SetValue(Convert.ChangeType(elemConst.Value, elementType), i);
                }
                else
                {
                    throw new NotSupportedException(
                        $"Array element at index {i} is not a constant expression.");
                }
            }
            return (T)(object)array;
        }

        throw new NotSupportedException($"Cannot extract constant value of type {typeof(T).Name} from {arg.GetType().Name}");
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
