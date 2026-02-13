using System.Linq.Expressions;
using EF.CH.Dictionaries;
using EF.CH.Extensions;
using EF.CH.Query.Internal.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Query.Internal;

/// <summary>
/// Translates LINQ expressions to ClickHouse SQL expressions.
/// Handles ClickHouse-specific translation rules for binary operations,
/// method calls, and member access.
/// </summary>
public class ClickHouseSqlTranslatingExpressionVisitor : RelationalSqlTranslatingExpressionVisitor
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public ClickHouseSqlTranslatingExpressionVisitor(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        : base(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
    {
        _sqlExpressionFactory = (ClickHouseSqlExpressionFactory)dependencies.SqlExpressionFactory;
        _typeMappingSource = dependencies.TypeMappingSource;
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
    /// Handles window function expressions that use implicit conversion from WindowBuilder&lt;T&gt;.
    /// </summary>
    protected override Expression VisitUnary(UnaryExpression unaryExpression)
    {
        // Handle window function expressions
        // Pattern: Convert(WindowBuilder<T> method chain, T?)
        if (unaryExpression.NodeType == ExpressionType.Convert &&
            IsWindowBuilderType(unaryExpression.Operand.Type))
        {
            return TranslateWindowFunctionFromBuilder(unaryExpression.Operand);
        }

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
    /// Checks if a type is WindowBuilder&lt;T&gt;.
    /// </summary>
    private static bool IsWindowBuilderType(Type type)
        => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(WindowBuilder<>);

    /// <summary>
    /// Checks if a method call has a Func&lt;WindowSpec, WindowSpec&gt; parameter.
    /// </summary>
    private static bool HasWindowSpecLambdaParameter(MethodCallExpression methodCall)
    {
        var parameters = methodCall.Method.GetParameters();
        return parameters.Any(p =>
            p.ParameterType == typeof(Func<WindowSpec, WindowSpec>));
    }

    /// <summary>
    /// Translates a Window.Xxx() call with lambda-style API.
    /// </summary>
    private SqlExpression TranslateWindowFunctionWithLambda(MethodCallExpression methodCall)
    {
        // Find the lambda parameter index
        var parameters = methodCall.Method.GetParameters();
        var lambdaIndex = -1;
        for (var i = 0; i < parameters.Length; i++)
        {
            if (parameters[i].ParameterType == typeof(Func<WindowSpec, WindowSpec>))
            {
                lambdaIndex = i;
                break;
            }
        }

        if (lambdaIndex < 0)
        {
            throw new InvalidOperationException(
                $"Could not find Func<WindowSpec, WindowSpec> parameter in {methodCall.Method.Name}");
        }

        // Get the lambda expression
        var lambdaArg = methodCall.Arguments[lambdaIndex];

        // Unwrap Quote if present
        if (lambdaArg is UnaryExpression { NodeType: ExpressionType.Quote } quote)
        {
            lambdaArg = quote.Operand;
        }

        if (lambdaArg is not LambdaExpression lambda)
        {
            throw new InvalidOperationException(
                $"Expected lambda expression for window function configuration, got {lambdaArg.GetType().Name}");
        }

        // Extract partition/order/frame from the lambda body
        var partitionBy = new List<Expression>();
        var orderBy = new List<(Expression Expression, bool Ascending)>();
        WindowFrameType? frameType = null;
        WindowFrameBound? frameStart = null;
        int? frameStartOffset = null;
        WindowFrameBound? frameEnd = null;
        int? frameEndOffset = null;

        ProcessWindowSpecLambda(lambda.Body, lambda.Parameters[0], partitionBy, orderBy,
            ref frameType, ref frameStart, ref frameStartOffset, ref frameEnd, ref frameEndOffset);

        // Get function details from the method call (excluding the lambda)
        var methodName = methodCall.Method.Name;
        var resultType = methodCall.Method.ReturnType;

        // Handle nullable return types
        var underlyingType = Nullable.GetUnderlyingType(resultType);
        var nonNullableResultType = underlyingType ?? resultType;

        string functionName;
        var valueArgs = new List<SqlExpression>();

        switch (methodName)
        {
            case nameof(Window.RowNumber):
                functionName = "row_number";
                break;

            case nameof(Window.Rank):
                functionName = "rank";
                break;

            case nameof(Window.DenseRank):
                functionName = "dense_rank";
                break;

            case nameof(Window.PercentRank):
                functionName = "percent_rank";
                break;

            case nameof(Window.NTile):
                functionName = "ntile";
                valueArgs.Add(TranslateArgument(methodCall.Arguments[0]));
                break;

            case nameof(Window.Lag):
                functionName = "lagInFrame";
                TranslateLagLeadArgs(methodCall, lambdaIndex, valueArgs);
                break;

            case nameof(Window.Lead):
                functionName = "leadInFrame";
                TranslateLagLeadArgs(methodCall, lambdaIndex, valueArgs);
                break;

            case nameof(Window.FirstValue):
                functionName = "first_value";
                valueArgs.Add(TranslateArgument(methodCall.Arguments[0]));
                break;

            case nameof(Window.LastValue):
                functionName = "last_value";
                valueArgs.Add(TranslateArgument(methodCall.Arguments[0]));
                break;

            case nameof(Window.NthValue):
                functionName = "nth_value";
                valueArgs.Add(TranslateArgument(methodCall.Arguments[0]));
                valueArgs.Add(TranslateArgument(methodCall.Arguments[1]));
                break;

            case nameof(Window.Sum):
                functionName = "sum";
                valueArgs.Add(TranslateArgument(methodCall.Arguments[0]));
                break;

            case nameof(Window.Avg):
                functionName = "avg";
                valueArgs.Add(TranslateArgument(methodCall.Arguments[0]));
                break;

            case nameof(Window.Count) when lambdaIndex == 0:
                // Count(lambda) - no value argument
                functionName = "count";
                break;

            case nameof(Window.Count):
                // Count(value, lambda)
                functionName = "count";
                valueArgs.Add(TranslateArgument(methodCall.Arguments[0]));
                break;

            case nameof(Window.Min):
                functionName = "min";
                valueArgs.Add(TranslateArgument(methodCall.Arguments[0]));
                break;

            case nameof(Window.Max):
                functionName = "max";
                valueArgs.Add(TranslateArgument(methodCall.Arguments[0]));
                break;

            default:
                throw new InvalidOperationException(
                    $"Unknown window function: {methodName}");
        }

        // Translate partition/order expressions to SQL
        var partitionBySql = partitionBy
            .Select(e => (SqlExpression)Visit(e)!)
            .Where(e => e != null)
            .ToList();

        var orderBySql = orderBy
            .Select(e =>
            {
                var sqlExpr = (SqlExpression)Visit(e.Expression)!;
                return sqlExpr != null
                    ? new OrderingExpression(sqlExpr, e.Ascending)
                    : null;
            })
            .Where(o => o != null)
            .Cast<OrderingExpression>()
            .ToList();

        // Build frame
        WindowFrame? frame = null;
        if (frameType.HasValue && frameStart.HasValue && frameEnd.HasValue)
        {
            frame = new WindowFrame(
                frameType.Value,
                frameStart.Value,
                frameStartOffset,
                frameEnd.Value,
                frameEndOffset);
        }

        // Auto-add frame for lagInFrame/leadInFrame if not specified
        if ((functionName == "lagInFrame" || functionName == "leadInFrame") && frame == null)
        {
            frame = WindowFrame.RowsUnboundedPrecedingToUnboundedFollowing;
        }

        // For window functions, make the result nullable
        var nullableResultType = nonNullableResultType.IsValueType && Nullable.GetUnderlyingType(nonNullableResultType) == null
            ? typeof(Nullable<>).MakeGenericType(nonNullableResultType)
            : nonNullableResultType;

        return new ClickHouseWindowFunctionExpression(
            functionName,
            valueArgs,
            partitionBySql,
            orderBySql,
            frame,
            nonNullableResultType,
            nullableResultType,
            _typeMappingSource.FindMapping(nonNullableResultType));
    }

    /// <summary>
    /// Translates Lag/Lead arguments for lambda-style calls.
    /// </summary>
    private void TranslateLagLeadArgs(MethodCallExpression methodCall, int lambdaIndex, List<SqlExpression> args)
    {
        // Value is always at index 0
        args.Add(TranslateArgument(methodCall.Arguments[0]));

        // Check for offset (before lambda)
        if (lambdaIndex >= 2)
        {
            args.Add(TranslateArgument(methodCall.Arguments[1]));

            // Check for default value
            if (lambdaIndex >= 3)
            {
                args.Add(TranslateArgument(methodCall.Arguments[2]));
            }
        }
        else
        {
            // Default offset of 1
            args.Add(_sqlExpressionFactory.Constant(1, _typeMappingSource.FindMapping(typeof(int))));
        }
    }

    /// <summary>
    /// Processes a WindowSpec lambda body to extract partition/order/frame info.
    /// </summary>
    private static void ProcessWindowSpecLambda(
        Expression body,
        ParameterExpression windowSpecParam,
        List<Expression> partitionBy,
        List<(Expression, bool)> orderBy,
        ref WindowFrameType? frameType,
        ref WindowFrameBound? frameStart,
        ref int? frameStartOffset,
        ref WindowFrameBound? frameEnd,
        ref int? frameEndOffset)
    {
        // Walk the method chain - the body is the outermost call
        // e.g., w.PartitionBy(x).OrderBy(y).Rows().UnboundedPreceding().CurrentRow()
        // We need to walk from outer to inner, collecting in reverse order

        var calls = new List<MethodCallExpression>();
        var current = body;

        while (current is MethodCallExpression call && call.Method.DeclaringType == typeof(WindowSpec))
        {
            calls.Add(call);
            current = call.Object!;
        }

        // Reverse to process from inner to outer (matches user's reading order)
        calls.Reverse();

        var boundIndex = 0;
        foreach (var call in calls)
        {
            ProcessWindowSpecMethod(call, partitionBy, orderBy,
                ref frameType, ref frameStart, ref frameStartOffset,
                ref frameEnd, ref frameEndOffset, ref boundIndex);
        }
    }

    /// <summary>
    /// Processes a WindowSpec method call, extracting partition/order/frame information.
    /// </summary>
    private static void ProcessWindowSpecMethod(
        MethodCallExpression call,
        List<Expression> partitionBy,
        List<(Expression, bool)> orderBy,
        ref WindowFrameType? frameType,
        ref WindowFrameBound? frameStart,
        ref int? frameStartOffset,
        ref WindowFrameBound? frameEnd,
        ref int? frameEndOffset,
        ref int boundIndex)
    {
        switch (call.Method.Name)
        {
            case nameof(WindowSpec.PartitionBy):
                partitionBy.Add(call.Arguments[0]);
                break;

            case nameof(WindowSpec.OrderBy):
                orderBy.Add((call.Arguments[0], true));
                break;

            case nameof(WindowSpec.OrderByDescending):
                orderBy.Add((call.Arguments[0], false));
                break;

            case nameof(WindowSpec.Rows):
                frameType = WindowFrameType.Rows;
                break;

            case nameof(WindowSpec.Range):
                frameType = WindowFrameType.Range;
                break;

            case nameof(WindowSpec.UnboundedPreceding):
                SetBound(ref frameStart, ref frameStartOffset, ref frameEnd, ref frameEndOffset,
                    ref boundIndex, WindowFrameBound.UnboundedPreceding, null);
                break;

            case nameof(WindowSpec.CurrentRow):
                SetBound(ref frameStart, ref frameStartOffset, ref frameEnd, ref frameEndOffset,
                    ref boundIndex, WindowFrameBound.CurrentRow, null);
                break;

            case nameof(WindowSpec.UnboundedFollowing):
                SetBound(ref frameStart, ref frameStartOffset, ref frameEnd, ref frameEndOffset,
                    ref boundIndex, WindowFrameBound.UnboundedFollowing, null);
                break;

            case nameof(WindowSpec.Preceding):
                var precedingOffset = GetConstantInt(call.Arguments[0]);
                SetBound(ref frameStart, ref frameStartOffset, ref frameEnd, ref frameEndOffset,
                    ref boundIndex, WindowFrameBound.Preceding, precedingOffset);
                break;

            case nameof(WindowSpec.Following):
                var followingOffset = GetConstantInt(call.Arguments[0]);
                SetBound(ref frameStart, ref frameStartOffset, ref frameEnd, ref frameEndOffset,
                    ref boundIndex, WindowFrameBound.Following, followingOffset);
                break;
        }
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
    /// Visits member expressions.
    /// </summary>
    protected override Expression VisitMember(MemberExpression memberExpression)
    {
        return base.VisitMember(memberExpression);
    }

    /// <summary>
    /// Visits method call expressions, handling ClickHouseDictionary and window function methods specially.
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

        // Check if this is a WindowBuilder<T> method - indicates window function usage
        // The expression tree is: Window.RowNumber().PartitionBy().OrderBy().Build()
        // When we see a WindowBuilder<T> method, we translate the entire chain
        if (declaringType != null &&
            declaringType.IsGenericType &&
            declaringType.GetGenericTypeDefinition() == typeof(WindowBuilder<>))
        {
            // Handle Build() method call - this terminates the builder chain
            if (method.Name == nameof(WindowBuilder<int>.Build))
            {
                return TranslateWindowFunctionFromBuilder(methodCallExpression.Object!);
            }
            return TranslateWindowFunctionFromBuilder(methodCallExpression);
        }

        // Check if this is a ClickHouseFunctions.RawSql<T>() call
        if (declaringType == typeof(ClickHouseFunctions) &&
            method.Name == nameof(ClickHouseFunctions.RawSql))
        {
            return TranslateRawSql(methodCallExpression);
        }

        // Check if this is a Window.Xxx() call with lambda-style API
        if (declaringType == typeof(Window))
        {
            // Check if this is a lambda-style call (has Func<WindowSpec, WindowSpec> parameter)
            if (HasWindowSpecLambdaParameter(methodCallExpression))
            {
                return TranslateWindowFunctionWithLambda(methodCallExpression);
            }

            // Fluent-style call (returns WindowBuilder<T>)
            return TranslateWindowFunctionFromBuilder(methodCallExpression);
        }

        return base.VisitMethodCall(methodCallExpression);
    }

    /// <summary>
    /// Translates <see cref="ClickHouseFunctions.RawSql{T}"/> to a <see cref="ClickHouseRawSqlExpression"/>.
    /// </summary>
    private Expression TranslateRawSql(MethodCallExpression methodCallExpression)
    {
        // Extract the SQL string from the argument
        var sqlArg = methodCallExpression.Arguments[0];

        // Unwrap EF.Constant() if present
        if (sqlArg is MethodCallExpression efConstantCall &&
            efConstantCall.Method.DeclaringType?.FullName == "Microsoft.EntityFrameworkCore.EF" &&
            efConstantCall.Method.Name == "Constant")
        {
            sqlArg = efConstantCall.Arguments[0];
        }

        string? sql = null;

        if (sqlArg is ConstantExpression constant && constant.Value is string s)
        {
            sql = s;
        }
        else
        {
            // Try to evaluate the expression to get the string value
            try
            {
                var lambda = Expression.Lambda<Func<string>>(sqlArg);
                sql = lambda.Compile()();
            }
            catch
            {
                // Fall through to error
            }
        }

        if (string.IsNullOrEmpty(sql))
        {
            throw new InvalidOperationException(
                "ClickHouseFunctions.RawSql requires a non-empty constant string argument.");
        }

        var returnType = methodCallExpression.Method.ReturnType;
        var typeMapping = _typeMappingSource.FindMapping(returnType);
        return new ClickHouseRawSqlExpression(sql, returnType, typeMapping);
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

    #region Window Function Translation

    /// <summary>
    /// Translates a window function from a WindowBuilder&lt;T&gt; method chain.
    /// Walks backwards through the chain to extract partition/order/frame clauses
    /// and find the root Window.Xxx() function call or WindowBuilder constant.
    /// </summary>
    private SqlExpression TranslateWindowFunctionFromBuilder(Expression builderChain)
    {
        var partitionBy = new List<Expression>();
        var orderBy = new List<(Expression Expression, bool Ascending)>();
        WindowFrameType? frameType = null;
        WindowFrameBound? frameStart = null;
        int? frameStartOffset = null;
        WindowFrameBound? frameEnd = null;
        int? frameEndOffset = null;
        var boundIndex = 0;

        var current = builderChain;
        MethodCallExpression? windowFunctionCall = null;
        object? windowBuilderInstance = null;
        Type? builderResultType = null;

        // Walk the method chain backwards
        while (current is MethodCallExpression call)
        {
            var declaringType = call.Method.DeclaringType;

            // Check if this is a WindowBuilder<T> method
            if (declaringType != null &&
                declaringType.IsGenericType &&
                declaringType.GetGenericTypeDefinition() == typeof(WindowBuilder<>))
            {
                ProcessWindowBuilderMethod(call, partitionBy, orderBy,
                    ref frameType, ref frameStart, ref frameStartOffset,
                    ref frameEnd, ref frameEndOffset, ref boundIndex);

                // Move to the instance (the Object of the call)
                current = call.Object!;
            }
            // Check if this is the root Window.Xxx() function call
            else if (declaringType == typeof(Window))
            {
                windowFunctionCall = call;
                break;
            }
            else
            {
                throw new InvalidOperationException(
                    $"Unexpected method in window function chain: {call.Method.Name} on {declaringType?.FullName ?? "null"}");
            }
        }

        // If we didn't find a Window.Xxx() method call, we might have hit a ConstantExpression
        // containing the WindowBuilder instance (happens when Window.RowNumber() etc. are optimized)
        if (windowFunctionCall == null && current is ConstantExpression constExpr)
        {
            var constType = constExpr.Type;
            if (constType.IsGenericType && constType.GetGenericTypeDefinition() == typeof(WindowBuilder<>))
            {
                windowBuilderInstance = constExpr.Value;
                builderResultType = constType.GetGenericArguments()[0];
            }
        }

        if (windowFunctionCall == null && windowBuilderInstance == null)
        {
            // Provide more context about what we found
            var currentDesc = current?.GetType().Name ?? "null";
            if (current is MethodCallExpression mce)
                currentDesc = $"MethodCall: {mce.Method.DeclaringType?.Name}.{mce.Method.Name}";
            else if (current is MemberExpression me)
                currentDesc = $"Member: {me.Member.Name}";

            throw new InvalidOperationException(
                $"Could not find root window function call (Window.RowNumber, Window.Lag, etc.). Current expression: {currentDesc}");
        }

        // Reverse lists since we walked backwards
        partitionBy.Reverse();
        orderBy.Reverse();

        // Swap frame bounds since we encountered them in reverse order
        if (frameStart.HasValue && frameEnd.HasValue)
        {
            (frameStart, frameEnd) = (frameEnd, frameStart);
            (frameStartOffset, frameEndOffset) = (frameEndOffset, frameStartOffset);
        }

        // Translate partition/order expressions to SQL
        var partitionBySql = partitionBy
            .Select(e => (SqlExpression)Visit(e)!)
            .Where(e => e != null)
            .ToList();

        var orderBySql = orderBy
            .Select(e =>
            {
                var sqlExpr = (SqlExpression)Visit(e.Expression)!;
                return sqlExpr != null
                    ? new OrderingExpression(sqlExpr, e.Ascending)
                    : null;
            })
            .Where(o => o != null)
            .Cast<OrderingExpression>()
            .ToList();

        // Get function details - either from the method call or from the WindowBuilder instance
        string functionName;
        List<SqlExpression> valueArgs;
        Type resultType;

        if (windowFunctionCall != null)
        {
            (functionName, valueArgs) = GetWindowFunctionDetailsFromBuilder(windowFunctionCall);
            var methodReturnType = windowFunctionCall.Method.ReturnType;
            resultType = methodReturnType.IsGenericType && methodReturnType.GetGenericTypeDefinition() == typeof(WindowBuilder<>)
                ? methodReturnType.GetGenericArguments()[0]
                : methodReturnType;
        }
        else
        {
            // Extract from WindowBuilder instance
            (functionName, valueArgs) = GetWindowFunctionDetailsFromInstance(windowBuilderInstance!);
            resultType = builderResultType!;
        }

        // Build frame
        WindowFrame? frame = null;
        if (frameType.HasValue && frameStart.HasValue && frameEnd.HasValue)
        {
            frame = new WindowFrame(
                frameType.Value,
                frameStart.Value,
                frameStartOffset,
                frameEnd.Value,
                frameEndOffset);
        }

        // Auto-add frame for lagInFrame/leadInFrame if not specified
        if ((functionName == "lagInFrame" || functionName == "leadInFrame") && frame == null)
        {
            frame = WindowFrame.RowsUnboundedPrecedingToUnboundedFollowing;
        }

        // For window functions, make the result nullable since window functions can return null
        // (e.g., lag/lead when there's no row at the offset)
        var nullableResultType = resultType.IsValueType && Nullable.GetUnderlyingType(resultType) == null
            ? typeof(Nullable<>).MakeGenericType(resultType)
            : resultType;

        return new ClickHouseWindowFunctionExpression(
            functionName,
            valueArgs,
            partitionBySql,
            orderBySql,
            frame,
            resultType,
            nullableResultType,
            _typeMappingSource.FindMapping(resultType));
    }

    /// <summary>
    /// Processes a WindowBuilder method call, extracting partition/order/frame information.
    /// </summary>
    private static void ProcessWindowBuilderMethod(
        MethodCallExpression call,
        List<Expression> partitionBy,
        List<(Expression, bool)> orderBy,
        ref WindowFrameType? frameType,
        ref WindowFrameBound? frameStart,
        ref int? frameStartOffset,
        ref WindowFrameBound? frameEnd,
        ref int? frameEndOffset,
        ref int boundIndex)
    {
        switch (call.Method.Name)
        {
            case nameof(WindowBuilder<int>.PartitionBy):
                partitionBy.Add(call.Arguments[0]);
                break;

            case nameof(WindowBuilder<int>.OrderBy):
                orderBy.Add((call.Arguments[0], true));
                break;

            case nameof(WindowBuilder<int>.OrderByDescending):
                orderBy.Add((call.Arguments[0], false));
                break;

            case nameof(WindowBuilder<int>.Rows):
                frameType = WindowFrameType.Rows;
                break;

            case nameof(WindowBuilder<int>.Range):
                frameType = WindowFrameType.Range;
                break;

            case nameof(WindowBuilder<int>.UnboundedPreceding):
                SetBound(ref frameStart, ref frameStartOffset, ref frameEnd, ref frameEndOffset,
                    ref boundIndex, WindowFrameBound.UnboundedPreceding, null);
                break;

            case nameof(WindowBuilder<int>.CurrentRow):
                SetBound(ref frameStart, ref frameStartOffset, ref frameEnd, ref frameEndOffset,
                    ref boundIndex, WindowFrameBound.CurrentRow, null);
                break;

            case nameof(WindowBuilder<int>.UnboundedFollowing):
                SetBound(ref frameStart, ref frameStartOffset, ref frameEnd, ref frameEndOffset,
                    ref boundIndex, WindowFrameBound.UnboundedFollowing, null);
                break;

            case nameof(WindowBuilder<int>.Preceding):
                var precedingOffset = GetConstantInt(call.Arguments[0]);
                SetBound(ref frameStart, ref frameStartOffset, ref frameEnd, ref frameEndOffset,
                    ref boundIndex, WindowFrameBound.Preceding, precedingOffset);
                break;

            case nameof(WindowBuilder<int>.Following):
                var followingOffset = GetConstantInt(call.Arguments[0]);
                SetBound(ref frameStart, ref frameStartOffset, ref frameEnd, ref frameEndOffset,
                    ref boundIndex, WindowFrameBound.Following, followingOffset);
                break;
        }
    }

    private static void SetBound(
        ref WindowFrameBound? frameStart,
        ref int? frameStartOffset,
        ref WindowFrameBound? frameEnd,
        ref int? frameEndOffset,
        ref int boundIndex,
        WindowFrameBound bound,
        int? offset)
    {
        if (boundIndex == 0)
        {
            frameStart = bound;
            frameStartOffset = offset;
            boundIndex = 1;
        }
        else
        {
            frameEnd = bound;
            frameEndOffset = offset;
        }
    }

    private static int? GetConstantInt(Expression expression)
    {
        if (expression is ConstantExpression constant)
        {
            return Convert.ToInt32(constant.Value);
        }

        if (expression is MemberExpression memberExpr)
        {
            try
            {
                var lambda = Expression.Lambda<Func<int>>(memberExpr);
                return lambda.Compile()();
            }
            catch
            {
                return null;
            }
        }

        return null;
    }

    /// <summary>
    /// Gets the ClickHouse function name and translates value arguments from a Window.Xxx() call.
    /// </summary>
    private (string FunctionName, List<SqlExpression> Arguments) GetWindowFunctionDetailsFromBuilder(
        MethodCallExpression methodCall)
    {
        var methodName = methodCall.Method.Name;
        var args = new List<SqlExpression>();

        switch (methodName)
        {
            // Ranking functions (no value arguments)
            case nameof(Window.RowNumber):
                return ("row_number", args);

            case nameof(Window.Rank):
                return ("rank", args);

            case nameof(Window.DenseRank):
                return ("dense_rank", args);

            case nameof(Window.PercentRank):
                return ("percent_rank", args);

            case nameof(Window.NTile):
                // NTile(buckets) - Arguments[0] is buckets
                args.Add(TranslateArgument(methodCall.Arguments[0]));
                return ("ntile", args);

            // Lag function - ClickHouse uses lagInFrame
            case nameof(Window.Lag):
                return TranslateLagLeadFunctionFromBuilder(methodCall, "lagInFrame");

            // Lead function - ClickHouse uses leadInFrame
            case nameof(Window.Lead):
                return TranslateLagLeadFunctionFromBuilder(methodCall, "leadInFrame");

            // Value functions
            case nameof(Window.FirstValue):
                args.Add(TranslateArgument(methodCall.Arguments[0]));
                return ("first_value", args);

            case nameof(Window.LastValue):
                args.Add(TranslateArgument(methodCall.Arguments[0]));
                return ("last_value", args);

            case nameof(Window.NthValue):
                // NthValue(value, n) - Arguments[0] is value, Arguments[1] is n
                args.Add(TranslateArgument(methodCall.Arguments[0]));
                args.Add(TranslateArgument(methodCall.Arguments[1]));
                return ("nth_value", args);

            // Aggregate window functions
            case nameof(Window.Sum):
                args.Add(TranslateArgument(methodCall.Arguments[0]));
                return ("sum", args);

            case nameof(Window.Avg):
                args.Add(TranslateArgument(methodCall.Arguments[0]));
                return ("avg", args);

            case nameof(Window.Count) when methodCall.Arguments.Count == 0:
                // Count() - no value argument, count all rows
                return ("count", args);

            case nameof(Window.Count):
                // Count(value) - Arguments[0] is the value
                args.Add(TranslateArgument(methodCall.Arguments[0]));
                return ("count", args);

            case nameof(Window.Min):
                args.Add(TranslateArgument(methodCall.Arguments[0]));
                return ("min", args);

            case nameof(Window.Max):
                args.Add(TranslateArgument(methodCall.Arguments[0]));
                return ("max", args);

            default:
                throw new InvalidOperationException(
                    $"Unknown window function: {methodName}. This method is not supported for translation.");
        }
    }

    /// <summary>
    /// Translates Lag/Lead function calls from the new Window API.
    /// </summary>
    private (string FunctionName, List<SqlExpression> Arguments) TranslateLagLeadFunctionFromBuilder(
        MethodCallExpression methodCall,
        string clickHouseFunctionName)
    {
        var args = new List<SqlExpression>();

        // Arguments layout for Window.Lag/Lead:
        // Lag(value) - 1 arg
        // Lag(value, offset) - 2 args
        // Lag(value, offset, default) - 3 args

        var argCount = methodCall.Arguments.Count;

        // Value is always at index 0
        args.Add(TranslateArgument(methodCall.Arguments[0]));

        if (argCount >= 2)
        {
            // Offset at index 1
            args.Add(TranslateArgument(methodCall.Arguments[1]));
        }
        else
        {
            // Default offset of 1
            args.Add(_sqlExpressionFactory.Constant(1, _typeMappingSource.FindMapping(typeof(int))));
        }

        if (argCount >= 3)
        {
            // Default value at index 2
            args.Add(TranslateArgument(methodCall.Arguments[2]));
        }

        return (clickHouseFunctionName, args);
    }

    /// <summary>
    /// Extracts function name and arguments from a WindowBuilder instance
    /// (used when Window.Xxx() calls are optimized away by the compiler).
    /// </summary>
    private (string FunctionName, List<SqlExpression> Arguments) GetWindowFunctionDetailsFromInstance(object builderInstance)
    {
        // Use reflection to get the FunctionName and FunctionArguments properties
        var builderType = builderInstance.GetType();
        var functionNameProp = builderType.GetProperty("FunctionName",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var functionArgsProp = builderType.GetProperty("FunctionArguments",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        var functionName = (string)functionNameProp!.GetValue(builderInstance)!;
        var functionArgs = (object?[])functionArgsProp!.GetValue(builderInstance)!;

        var args = new List<SqlExpression>();

        // Translate function arguments - these are literals stored in the WindowBuilder
        foreach (var arg in functionArgs)
        {
            if (arg == null)
            {
                continue;
            }

            // Create constant expression for literal values
            var argType = arg.GetType();
            var typeMapping = _typeMappingSource.FindMapping(argType);
            args.Add(_sqlExpressionFactory.Constant(arg, typeMapping));
        }

        return (functionName, args);
    }

    /// <summary>
    /// Translates an argument expression to SQL.
    /// </summary>
    private SqlExpression TranslateArgument(Expression argument)
    {
        var translated = Visit(argument);
        if (translated is not SqlExpression sqlExpression)
        {
            throw new InvalidOperationException(
                $"Could not translate argument '{argument}' to SQL expression.");
        }

        // Ensure constants have type mappings
        if (sqlExpression is SqlConstantExpression constant && constant.TypeMapping == null && constant.Value != null)
        {
            var typeMapping = _typeMappingSource.FindMapping(constant.Type);
            if (typeMapping != null)
            {
                return _sqlExpressionFactory.Constant(constant.Value, typeMapping);
            }
        }

        return sqlExpression;
    }

    #endregion
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
