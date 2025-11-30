using EF.CH.Query.Internal.Translators;
using Microsoft.EntityFrameworkCore.Query;

namespace EF.CH.Query.Internal;

/// <summary>
/// Provides method call translators for ClickHouse.
/// </summary>
public class ClickHouseMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
{
    public ClickHouseMethodCallTranslatorProvider(
        RelationalMethodCallTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        var sqlExpressionFactory = (ClickHouseSqlExpressionFactory)dependencies.SqlExpressionFactory;

        // Register ClickHouse-specific translators
        AddTranslators(
        [
            new ClickHouseStringMethodTranslator(sqlExpressionFactory),
            new ClickHouseDateTimeMethodTranslator(sqlExpressionFactory),
            new ClickHouseMathMethodTranslator(sqlExpressionFactory),
            new ClickHouseConvertMethodTranslator(sqlExpressionFactory),
            new ClickHouseGuidMethodTranslator(sqlExpressionFactory),
            new ClickHouseDbFunctionsTranslator(sqlExpressionFactory),
            new ClickHouseArrayMethodTranslator(sqlExpressionFactory),
            new ClickHouseMapMethodTranslator(sqlExpressionFactory),
        ]);
    }
}
