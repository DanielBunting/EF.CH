using System.Reflection;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates ClickHouse dictionary function calls to SQL.
/// </summary>
/// <remarks>
/// Supported translations:
/// - EF.Functions.DictGet(...) → dictGet(...)
/// - EF.Functions.DictGetOrDefault(...) → dictGetOrDefault(...)
/// - EF.Functions.DictHas(...) → dictHas(...)
/// - EF.Functions.DictGetHierarchy(...) → dictGetHierarchy(...)
/// - EF.Functions.DictIsIn(...) → dictIsIn(...)
/// - EF.Functions.DictGetForDate/DateTime(...) → dictGet(..., date/datetime)
/// </remarks>
public class ClickHouseDictionaryTranslator : IMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    public ClickHouseDictionaryTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // Only handle methods from ClickHouseDictionaryFunctions
        if (method.DeclaringType != typeof(ClickHouseDictionaryFunctions))
        {
            return null;
        }

        return method.Name switch
        {
            nameof(ClickHouseDictionaryFunctions.DictGet)
                => TranslateDictGet(method, arguments),
            nameof(ClickHouseDictionaryFunctions.DictGetOrDefault)
                => TranslateDictGetOrDefault(method, arguments),
            nameof(ClickHouseDictionaryFunctions.DictHas)
                => TranslateDictHas(arguments),
            nameof(ClickHouseDictionaryFunctions.DictGetHierarchy)
                => TranslateDictGetHierarchy(arguments),
            nameof(ClickHouseDictionaryFunctions.DictIsIn)
                => TranslateDictIsIn(arguments),
            nameof(ClickHouseDictionaryFunctions.DictGetForDate)
                => TranslateDictGetForDate(method, arguments),
            nameof(ClickHouseDictionaryFunctions.DictGetForDateTime)
                => TranslateDictGetForDateTime(method, arguments),
            _ => null
        };
    }

    /// <summary>
    /// Translates DictGet to dictGet('dictName', 'attrName', key) or dictGet('dictName', 'attrName', (k1, k2, ...))
    /// </summary>
    private SqlExpression TranslateDictGet(MethodInfo method, IReadOnlyList<SqlExpression> arguments)
    {
        // arguments[0] = DbFunctions instance (ignored)
        // arguments[1] = dictionary name (string constant)
        // arguments[2] = attribute name (string constant)
        // arguments[3+] = key parts

        var returnType = method.ReturnType;
        var functionArgs = new List<SqlExpression>
        {
            arguments[1], // dictionary name
            arguments[2]  // attribute name
        };

        // Single key vs composite key
        if (arguments.Count == 4)
        {
            // Single key: dictGet('dict', 'attr', key)
            functionArgs.Add(arguments[3]);
        }
        else
        {
            // Composite key: dictGet('dict', 'attr', tuple(k1, k2, ...))
            var keyParts = arguments.Skip(3).ToArray();
            var tupleExpr = _sqlExpressionFactory.Function(
                "tuple",
                keyParts,
                nullable: false,
                argumentsPropagateNullability: keyParts.Select(_ => true).ToArray(),
                typeof(object));
            functionArgs.Add(tupleExpr);
        }

        return _sqlExpressionFactory.Function(
            "dictGet",
            functionArgs,
            nullable: true,
            argumentsPropagateNullability: new[] { false, false, true },
            returnType);
    }

    /// <summary>
    /// Translates DictGetOrDefault to dictGetOrDefault('dictName', 'attrName', key, defaultValue)
    /// </summary>
    private SqlExpression TranslateDictGetOrDefault(MethodInfo method, IReadOnlyList<SqlExpression> arguments)
    {
        // arguments[0] = DbFunctions instance (ignored)
        // arguments[1] = dictionary name
        // arguments[2] = attribute name
        // arguments[3] = key (or first part of composite key)
        // arguments[4] = second key part (if composite) or default value (if single key)
        // arguments[5] = default value (if composite key with 2 parts)

        var returnType = method.ReturnType;
        var functionArgs = new List<SqlExpression>
        {
            arguments[1], // dictionary name
            arguments[2]  // attribute name
        };

        // Determine if this is single key or composite key based on argument count
        // Single key: 5 arguments (DbFunctions, dict, attr, key, default)
        // Composite key (2): 6 arguments (DbFunctions, dict, attr, key1, key2, default)
        if (arguments.Count == 5)
        {
            // Single key
            functionArgs.Add(arguments[3]); // key
            functionArgs.Add(arguments[4]); // default
        }
        else if (arguments.Count == 6)
        {
            // Composite key (2 parts)
            var tupleExpr = _sqlExpressionFactory.Function(
                "tuple",
                new[] { arguments[3], arguments[4] },
                nullable: false,
                argumentsPropagateNullability: new[] { true, true },
                typeof(object));
            functionArgs.Add(tupleExpr);
            functionArgs.Add(arguments[5]); // default
        }

        return _sqlExpressionFactory.Function(
            "dictGetOrDefault",
            functionArgs,
            nullable: true,
            argumentsPropagateNullability: new[] { false, false, true, false },
            returnType);
    }

    /// <summary>
    /// Translates DictHas to dictHas('dictName', key)
    /// </summary>
    private SqlExpression TranslateDictHas(IReadOnlyList<SqlExpression> arguments)
    {
        // arguments[0] = DbFunctions instance (ignored)
        // arguments[1] = dictionary name
        // arguments[2] = key (or first part)
        // arguments[3] = second key part (optional)

        var functionArgs = new List<SqlExpression> { arguments[1] };

        if (arguments.Count == 3)
        {
            // Single key
            functionArgs.Add(arguments[2]);
        }
        else if (arguments.Count == 4)
        {
            // Composite key
            var tupleExpr = _sqlExpressionFactory.Function(
                "tuple",
                new[] { arguments[2], arguments[3] },
                nullable: false,
                argumentsPropagateNullability: new[] { true, true },
                typeof(object));
            functionArgs.Add(tupleExpr);
        }

        return _sqlExpressionFactory.Function(
            "dictHas",
            functionArgs,
            nullable: false,
            argumentsPropagateNullability: new[] { false, true },
            typeof(bool));
    }

    /// <summary>
    /// Translates DictGetHierarchy to dictGetHierarchy('dictName', key)
    /// </summary>
    private SqlExpression TranslateDictGetHierarchy(IReadOnlyList<SqlExpression> arguments)
    {
        // arguments[0] = DbFunctions instance (ignored)
        // arguments[1] = dictionary name
        // arguments[2] = key

        return _sqlExpressionFactory.Function(
            "dictGetHierarchy",
            new[] { arguments[1], arguments[2] },
            nullable: true,
            argumentsPropagateNullability: new[] { false, true },
            typeof(ulong[]));
    }

    /// <summary>
    /// Translates DictIsIn to dictIsIn('dictName', childKey, ancestorKey)
    /// </summary>
    private SqlExpression TranslateDictIsIn(IReadOnlyList<SqlExpression> arguments)
    {
        // arguments[0] = DbFunctions instance (ignored)
        // arguments[1] = dictionary name
        // arguments[2] = child key
        // arguments[3] = ancestor key

        return _sqlExpressionFactory.Function(
            "dictIsIn",
            new[] { arguments[1], arguments[2], arguments[3] },
            nullable: false,
            argumentsPropagateNullability: new[] { false, true, true },
            typeof(bool));
    }

    /// <summary>
    /// Translates DictGetForDate to dictGet('dictName', 'attrName', key, date)
    /// </summary>
    private SqlExpression TranslateDictGetForDate(MethodInfo method, IReadOnlyList<SqlExpression> arguments)
    {
        // arguments[0] = DbFunctions instance (ignored)
        // arguments[1] = dictionary name
        // arguments[2] = attribute name
        // arguments[3] = key
        // arguments[4] = date

        var returnType = method.ReturnType;

        return _sqlExpressionFactory.Function(
            "dictGet",
            new[] { arguments[1], arguments[2], arguments[3], arguments[4] },
            nullable: true,
            argumentsPropagateNullability: new[] { false, false, true, true },
            returnType);
    }

    /// <summary>
    /// Translates DictGetForDateTime to dictGet('dictName', 'attrName', key, datetime)
    /// </summary>
    private SqlExpression TranslateDictGetForDateTime(MethodInfo method, IReadOnlyList<SqlExpression> arguments)
    {
        // arguments[0] = DbFunctions instance (ignored)
        // arguments[1] = dictionary name
        // arguments[2] = attribute name
        // arguments[3] = key
        // arguments[4] = datetime

        var returnType = method.ReturnType;

        return _sqlExpressionFactory.Function(
            "dictGet",
            new[] { arguments[1], arguments[2], arguments[3], arguments[4] },
            nullable: true,
            argumentsPropagateNullability: new[] { false, false, true, true },
            returnType);
    }
}
