using System.Linq.Expressions;
using System.Reflection;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;

namespace EF.CH.Query.Internal;

/// <summary>
/// Preprocesses queries to rewrite ClickHouse-specific extension methods (ArrayJoin, AsofJoin)
/// into standard LINQ methods before the NavigationExpandingExpressionVisitor runs.
/// This is necessary because type-changing custom methods are rejected by the navigation expander.
/// </summary>
public class ClickHouseQueryTranslationPreprocessor : RelationalQueryTranslationPreprocessor
{
    private readonly RelationalQueryCompilationContext _queryCompilationContext;

    public ClickHouseQueryTranslationPreprocessor(
        QueryTranslationPreprocessorDependencies dependencies,
        RelationalQueryTranslationPreprocessorDependencies relationalDependencies,
        QueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        _queryCompilationContext = (RelationalQueryCompilationContext)queryCompilationContext;
    }

    public override Expression Process(Expression query)
    {
        // Rewrite custom methods BEFORE the navigation expander runs
        query = new ClickHouseMethodRewritingVisitor(_queryCompilationContext).Visit(query);
        return base.Process(query);
    }
}

/// <summary>
/// Factory for creating ClickHouse query translation preprocessors.
/// </summary>
public class ClickHouseQueryTranslationPreprocessorFactory : IQueryTranslationPreprocessorFactory
{
    private readonly QueryTranslationPreprocessorDependencies _dependencies;
    private readonly RelationalQueryTranslationPreprocessorDependencies _relationalDependencies;

    public ClickHouseQueryTranslationPreprocessorFactory(
        QueryTranslationPreprocessorDependencies dependencies,
        RelationalQueryTranslationPreprocessorDependencies relationalDependencies)
    {
        _dependencies = dependencies;
        _relationalDependencies = relationalDependencies;
    }

    public QueryTranslationPreprocessor Create(QueryCompilationContext queryCompilationContext)
        => new ClickHouseQueryTranslationPreprocessor(
            _dependencies, _relationalDependencies, queryCompilationContext);
}

/// <summary>
/// Rewrites ArrayJoin and AsofJoin method calls into standard LINQ methods
/// so the NavigationExpandingExpressionVisitor can process them.
/// Stores ClickHouse-specific metadata in the query compilation context options.
/// </summary>
internal class ClickHouseMethodRewritingVisitor : ExpressionVisitor
{
    private readonly RelationalQueryCompilationContext _queryCompilationContext;
    private readonly ClickHouseQueryCompilationContextOptions _options;

    public ClickHouseMethodRewritingVisitor(RelationalQueryCompilationContext queryCompilationContext)
    {
        _queryCompilationContext = queryCompilationContext;
        _options = queryCompilationContext.QueryCompilationContextOptions();
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Visit arguments first (handles nested calls like .Final().ArrayJoin(...))
        var visited = (MethodCallExpression)base.VisitMethodCall(node);

        if (!visited.Method.IsGenericMethod)
            return visited;

        var genericDef = visited.Method.GetGenericMethodDefinition();

        if (genericDef == ClickHouseQueryableExtensions.ArrayJoinMethodInfo)
            return RewriteArrayJoin(visited, isLeft: false);

        if (genericDef == ClickHouseQueryableExtensions.LeftArrayJoinMethodInfo)
            return RewriteArrayJoin(visited, isLeft: true);

        if (genericDef == ClickHouseQueryableExtensions.ArrayJoin2MethodInfo)
            return RewriteArrayJoin2(visited);

        if (genericDef == ClickHouseQueryableExtensions.AsofJoinMethodInfo)
            return RewriteAsofJoin(visited, isLeft: false);

        if (genericDef == ClickHouseQueryableExtensions.AsofLeftJoinMethodInfo)
            return RewriteAsofJoin(visited, isLeft: true);

        return visited;
    }

    /// <summary>
    /// Rewrites ArrayJoin(source, arraySelector, resultSelector) → source.Select(modifiedResultSelector)
    /// where element parameter references are replaced with RawSql calls.
    /// </summary>
    private Expression RewriteArrayJoin(MethodCallExpression call, bool isLeft)
    {
        // Arguments: [0]=source, [1]=arraySelector (quoted), [2]=resultSelector (quoted)
        var source = call.Arguments[0];
        var arraySelector = UnwrapLambda(call.Arguments[1]);
        var resultSelector = UnwrapLambda(call.Arguments[2]);

        var propertyName = ExtractMemberName(arraySelector);
        var columnName = ResolveColumnName(call.Method.GetGenericArguments()[0], propertyName);
        var alias = resultSelector.Parameters[1].Name ?? columnName;
        var elementType = resultSelector.Parameters[1].Type;

        _options.ArrayJoinSpecs.Add(new ArrayJoinSpec
        {
            ColumnName = columnName,
            Alias = alias,
            IsLeft = isLeft
        });

        // Replace element parameter with RawSql<TElement>("\"alias\"")
        var rawSqlMethod = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.RawSql))!
            .MakeGenericMethod(elementType);
        var quotedAlias = "\"" + alias + "\"";
        var rawSqlCall = Expression.Call(rawSqlMethod, Expression.Constant(quotedAlias));

        var newBody = new ParameterReplacer(resultSelector.Parameters[1], rawSqlCall)
            .Visit(resultSelector.Body);
        var selectLambda = Expression.Lambda(newBody, resultSelector.Parameters[0]);

        // Rewrite as Queryable.Select(source, selectLambda)
        var entityType = resultSelector.Parameters[0].Type;
        var resultType = resultSelector.ReturnType;
        var selectMethod = GetQueryableSelectMethod().MakeGenericMethod(entityType, resultType);

        return Expression.Call(null, selectMethod, source, Expression.Quote(selectLambda));
    }

    /// <summary>
    /// Rewrites ArrayJoin with two arrays.
    /// </summary>
    private Expression RewriteArrayJoin2(MethodCallExpression call)
    {
        // Arguments: [0]=source, [1]=arraySelector1, [2]=arraySelector2, [3]=resultSelector
        var source = call.Arguments[0];
        var arraySelector1 = UnwrapLambda(call.Arguments[1]);
        var arraySelector2 = UnwrapLambda(call.Arguments[2]);
        var resultSelector = UnwrapLambda(call.Arguments[3]);

        var genericArgs = call.Method.GetGenericArguments();
        var entityClrType = genericArgs[0];

        var propName1 = ExtractMemberName(arraySelector1);
        var colName1 = ResolveColumnName(entityClrType, propName1);
        var alias1 = resultSelector.Parameters[1].Name ?? colName1;
        var elemType1 = resultSelector.Parameters[1].Type;

        var propName2 = ExtractMemberName(arraySelector2);
        var colName2 = ResolveColumnName(entityClrType, propName2);
        var alias2 = resultSelector.Parameters[2].Name ?? colName2;
        var elemType2 = resultSelector.Parameters[2].Type;

        _options.ArrayJoinSpecs.Add(new ArrayJoinSpec { ColumnName = colName1, Alias = alias1, IsLeft = false });
        _options.ArrayJoinSpecs.Add(new ArrayJoinSpec { ColumnName = colName2, Alias = alias2, IsLeft = false });

        var rawSqlMethod = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.RawSql))!;

        var rawSql1 = Expression.Call(rawSqlMethod.MakeGenericMethod(elemType1),
            Expression.Constant("\"" + alias1 + "\""));
        var rawSql2 = Expression.Call(rawSqlMethod.MakeGenericMethod(elemType2),
            Expression.Constant("\"" + alias2 + "\""));

        var newBody = new ParameterReplacer(resultSelector.Parameters[1], rawSql1).Visit(resultSelector.Body);
        newBody = new ParameterReplacer(resultSelector.Parameters[2], rawSql2).Visit(newBody);
        var selectLambda = Expression.Lambda(newBody, resultSelector.Parameters[0]);

        var resultType = resultSelector.ReturnType;
        var selectMethod = GetQueryableSelectMethod().MakeGenericMethod(entityClrType, resultType);

        return Expression.Call(null, selectMethod, source, Expression.Quote(selectLambda));
    }

    /// <summary>
    /// Rewrites AsofJoin(outer, inner, outerKey, innerKey, asofCond, resultSelector)
    /// → Queryable.Join(outer, inner, outerKey, innerKey, resultSelector)
    /// </summary>
    private Expression RewriteAsofJoin(MethodCallExpression call, bool isLeft)
    {
        // Arguments: [0]=outer, [1]=inner, [2]=outerKey, [3]=innerKey, [4]=asofCondition, [5]=resultSelector
        var asofCondition = UnwrapLambda(call.Arguments[4]);
        var (leftPropName, rightPropName, op) = ParseAsofCondition(asofCondition);

        var genericArgs = call.Method.GetGenericArguments(); // TOuter, TInner, TKey, TResult
        var leftColName = ResolveColumnName(genericArgs[0], leftPropName);
        var rightColName = ResolveColumnName(genericArgs[1], rightPropName);

        if (_options.AsofJoin != null)
        {
            throw new InvalidOperationException("Only one ASOF JOIN per query is supported.");
        }

        _options.AsofJoin = new AsofJoinInfo
        {
            LeftColumnName = leftColName,
            RightColumnName = rightColName,
            Operator = op,
            IsLeft = isLeft
        };

        // Rewrite as Queryable.Join(outer, inner, outerKey, innerKey, resultSelector)
        var joinMethod = typeof(Queryable).GetMethods()
            .First(m => m.Name == "Join" && m.GetParameters().Length == 5)
            .MakeGenericMethod(genericArgs);

        return Expression.Call(
            null, joinMethod,
            call.Arguments[0],  // outer
            call.Arguments[1],  // inner
            call.Arguments[2],  // outerKeySelector
            call.Arguments[3],  // innerKeySelector
            call.Arguments[5]); // resultSelector (skip asofCondition at [4])
    }

    private static (string leftProp, string rightProp, string op) ParseAsofCondition(LambdaExpression lambda)
    {
        if (lambda.Body is not BinaryExpression binary)
        {
            throw new InvalidOperationException("ASOF condition must be a comparison (>=, >, <=, <).");
        }

        var op = binary.NodeType switch
        {
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.GreaterThan => ">",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.LessThan => "<",
            _ => throw new InvalidOperationException("ASOF condition must use >=, >, <=, or < operator.")
        };

        var leftProp = ExtractPropertyName(binary.Left);
        var rightProp = ExtractPropertyName(binary.Right);
        return (leftProp, rightProp, op);
    }

    private static string ExtractPropertyName(Expression expression)
    {
        if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
            expression = unary.Operand;

        if (expression is MemberExpression member)
            return member.Member.Name;

        throw new InvalidOperationException(
            $"ASOF condition must reference entity properties directly. Got: {expression.GetType().Name}");
    }

    private static string ExtractMemberName(LambdaExpression lambda)
    {
        var body = lambda.Body;
        if (body is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
            body = unary.Operand;

        if (body is MemberExpression member)
            return member.Member.Name;

        return body.ToString();
    }

    private string ResolveColumnName(Type entityType, string propertyName)
    {
        var efEntityType = _queryCompilationContext.Model.FindEntityType(entityType);
        if (efEntityType == null)
            throw new InvalidOperationException($"Entity type {entityType.Name} not found in model.");

        var property = efEntityType.FindProperty(propertyName);
        if (property == null)
            throw new InvalidOperationException($"Property {propertyName} not found on {entityType.Name}.");

        return property.GetColumnName() ?? propertyName;
    }

    private static LambdaExpression UnwrapLambda(Expression expression)
    {
        while (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Quote)
            expression = unary.Operand;

        return expression as LambdaExpression
            ?? throw new InvalidOperationException($"Expected lambda expression, got {expression.GetType().Name}");
    }

    private static MethodInfo GetQueryableSelectMethod()
    {
        return typeof(Queryable).GetMethods()
            .Where(m => m.Name == "Select" && m.GetParameters().Length == 2)
            .First(m => m.GetParameters()[1].ParameterType.GetGenericArguments()[0]
                .GetGenericArguments().Length == 2);
    }

    private class ParameterReplacer : ExpressionVisitor
    {
        private readonly ParameterExpression _target;
        private readonly Expression _replacement;

        public ParameterReplacer(ParameterExpression target, Expression replacement)
        {
            _target = target;
            _replacement = replacement;
        }

        protected override Expression VisitParameter(ParameterExpression node)
            => node == _target ? _replacement : base.VisitParameter(node);
    }
}
