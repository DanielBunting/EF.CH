using System.Linq.Expressions;
using EF.CH.Dictionaries;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal;

/// <summary>
/// Translates LINQ expressions to ClickHouse SQL expressions.
/// Handles ClickHouse-specific translation rules for binary operations,
/// method calls, and member access.
/// </summary>
public class ClickHouseSqlTranslatingExpressionVisitor : RelationalSqlTranslatingExpressionVisitor
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    public ClickHouseSqlTranslatingExpressionVisitor(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        : base(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
    {
        _sqlExpressionFactory = (ClickHouseSqlExpressionFactory)dependencies.SqlExpressionFactory;
    }

    /// <summary>
    /// Visits binary expressions and handles ClickHouse-specific translations.
    /// </summary>
    protected override Expression VisitBinary(BinaryExpression binaryExpression)
    {
        // Handle string concatenation: "a" + "b" → concat(a, b)
        if (binaryExpression.NodeType == ExpressionType.Add &&
            binaryExpression.Left.Type == typeof(string))
        {
            var left = Visit(binaryExpression.Left);
            var right = Visit(binaryExpression.Right);

            if (left is SqlExpression sqlLeft && right is SqlExpression sqlRight)
            {
                return _sqlExpressionFactory.ConcatStrings(sqlLeft, sqlRight);
            }
        }

        // Handle DateTime subtraction for TimeSpan operations
        if (binaryExpression.NodeType == ExpressionType.Subtract &&
            binaryExpression.Left.Type == typeof(DateTime) &&
            binaryExpression.Right.Type == typeof(DateTime))
        {
            var left = Visit(binaryExpression.Left);
            var right = Visit(binaryExpression.Right);

            if (left is SqlExpression sqlLeft && right is SqlExpression sqlRight)
            {
                // Result is TimeSpan - we'll handle TotalDays etc. in member access
                // For now, return dateDiff in seconds
                return _sqlExpressionFactory.Function(
                    "dateDiff",
                    new SqlExpression[]
                    {
                        _sqlExpressionFactory.Constant("second"),
                        sqlRight,
                        sqlLeft
                    },
                    nullable: true,
                    argumentsPropagateNullability: new[] { false, true, true },
                    typeof(long));
            }
        }

        return base.VisitBinary(binaryExpression);
    }

    /// <summary>
    /// Visits unary expressions for ClickHouse-specific handling.
    /// </summary>
    protected override Expression VisitUnary(UnaryExpression unaryExpression)
    {
        // Handle NOT operations
        if (unaryExpression.NodeType == ExpressionType.Not &&
            unaryExpression.Operand.Type == typeof(bool))
        {
            var operand = Visit(unaryExpression.Operand);
            if (operand is SqlExpression sqlOperand)
            {
                return _sqlExpressionFactory.Not(sqlOperand);
            }
        }

        return base.VisitUnary(unaryExpression);
    }

    /// <summary>
    /// Visits new expressions, handling tuple creation for ClickHouse.
    /// </summary>
    protected override Expression VisitNew(NewExpression newExpression)
    {
        // Handle anonymous types and tuples for projections
        return base.VisitNew(newExpression);
    }

    /// <summary>
    /// Visits method call expressions, handling ClickHouseDictionary methods specially.
    /// </summary>
    protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
    {
        var method = methodCallExpression.Method;
        var declaringType = method.DeclaringType;

        // Check if this is a ClickHouseDictionary<T,K> method
        if (declaringType != null &&
            declaringType.IsGenericType &&
            declaringType.GetGenericTypeDefinition() == typeof(ClickHouseDictionary<,>))
        {
            return TranslateDictionaryMethod(methodCallExpression);
        }

        return base.VisitMethodCall(methodCallExpression);
    }

    private Expression TranslateDictionaryMethod(MethodCallExpression methodCallExpression)
    {
        var method = methodCallExpression.Method;
        var methodName = method.Name;

        // Get dictionary info from the declaring type
        var declaringType = method.DeclaringType!;
        var typeArgs = declaringType.GetGenericArguments();
        var dictionaryType = typeArgs[0]; // TDictionary

        // Derive dictionary name from entity type (convert to snake_case)
        var dictionaryName = ConvertToSnakeCase(dictionaryType.Name);

        return methodName switch
        {
            "Get" => TranslateDictionaryGet(methodCallExpression, dictionaryName),
            "GetOrDefault" => TranslateDictionaryGetOrDefault(methodCallExpression, dictionaryName),
            "ContainsKey" => TranslateDictionaryContainsKey(methodCallExpression, dictionaryName),
            _ => throw new InvalidOperationException(
                $"Dictionary method '{methodName}' cannot be translated to SQL. " +
                "Use GetAsync/GetOrDefaultAsync/ContainsKeyAsync for direct access outside of LINQ queries.")
        };
    }

    private SqlExpression TranslateDictionaryGet(
        MethodCallExpression methodCall,
        string dictionaryName)
    {
        // Get<TAttribute>(key, c => c.Attr)
        // Arguments[0] = key
        // Arguments[1] = attribute selector lambda

        var keyArg = methodCall.Arguments[0];
        var selectorArg = methodCall.Arguments[1];

        // Translate the key argument to SQL
        var keyExpression = (SqlExpression)Visit(keyArg);

        // Extract property name from the lambda
        var propertyName = ExtractPropertyNameFromSelector(selectorArg);

        // dictGet('dictionary_name', 'AttributeName', key)
        return _sqlExpressionFactory.Function(
            "dictGet",
            new SqlExpression[]
            {
                _sqlExpressionFactory.Constant(dictionaryName),
                _sqlExpressionFactory.Constant(propertyName),
                keyExpression
            },
            nullable: true,
            argumentsPropagateNullability: new[] { false, false, true },
            methodCall.Method.ReturnType);
    }

    private SqlExpression TranslateDictionaryGetOrDefault(
        MethodCallExpression methodCall,
        string dictionaryName)
    {
        // GetOrDefault<TAttribute>(key, c => c.Attr, defaultValue)
        // Arguments[0] = key
        // Arguments[1] = attribute selector lambda
        // Arguments[2] = default value

        var keyArg = methodCall.Arguments[0];
        var selectorArg = methodCall.Arguments[1];
        var defaultArg = methodCall.Arguments[2];

        // Translate arguments to SQL
        var keyExpression = (SqlExpression)Visit(keyArg);
        var defaultExpression = (SqlExpression)Visit(defaultArg);

        // Extract property name from the lambda
        var propertyName = ExtractPropertyNameFromSelector(selectorArg);

        // dictGetOrDefault('dictionary_name', 'AttributeName', key, default)
        return _sqlExpressionFactory.Function(
            "dictGetOrDefault",
            new SqlExpression[]
            {
                _sqlExpressionFactory.Constant(dictionaryName),
                _sqlExpressionFactory.Constant(propertyName),
                keyExpression,
                defaultExpression
            },
            nullable: true,
            argumentsPropagateNullability: new[] { false, false, true, true },
            methodCall.Method.ReturnType);
    }

    private SqlExpression TranslateDictionaryContainsKey(
        MethodCallExpression methodCall,
        string dictionaryName)
    {
        // ContainsKey(key)
        // Arguments[0] = key

        var keyArg = methodCall.Arguments[0];

        // Translate the key argument to SQL
        var keyExpression = (SqlExpression)Visit(keyArg);

        // dictHas('dictionary_name', key)
        return _sqlExpressionFactory.Function(
            "dictHas",
            new SqlExpression[]
            {
                _sqlExpressionFactory.Constant(dictionaryName),
                keyExpression
            },
            nullable: true,
            argumentsPropagateNullability: new[] { false, true },
            typeof(bool));
    }

    private static string ExtractPropertyNameFromSelector(Expression selectorExpression)
    {
        // Unwrap Quote if present (lambdas are often wrapped in Quote)
        if (selectorExpression is UnaryExpression { NodeType: ExpressionType.Quote } unary)
        {
            selectorExpression = unary.Operand;
        }

        // Handle LambdaExpression: c => c.PropertyName
        if (selectorExpression is LambdaExpression lambda)
        {
            if (lambda.Body is MemberExpression member)
            {
                return member.Member.Name;
            }
        }

        throw new InvalidOperationException(
            "Dictionary attribute selector must be a simple property access expression like 'c => c.Name'.");
    }

    private static string ConvertToSnakeCase(string name)
    {
        if (string.IsNullOrEmpty(name))
            return name;

        var result = new System.Text.StringBuilder();
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                if (i > 0)
                    result.Append('_');
                result.Append(char.ToLowerInvariant(c));
            }
            else
            {
                result.Append(c);
            }
        }
        return result.ToString();
    }
}

/// <summary>
/// Factory for creating ClickHouse SQL translating expression visitors.
/// </summary>
public class ClickHouseSqlTranslatingExpressionVisitorFactory : IRelationalSqlTranslatingExpressionVisitorFactory
{
    private readonly RelationalSqlTranslatingExpressionVisitorDependencies _dependencies;

    public ClickHouseSqlTranslatingExpressionVisitorFactory(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    public RelationalSqlTranslatingExpressionVisitor Create(
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
    {
        return new ClickHouseSqlTranslatingExpressionVisitor(
            _dependencies,
            queryCompilationContext,
            queryableMethodTranslatingExpressionVisitor);
    }
}
