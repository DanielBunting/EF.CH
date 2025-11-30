using EF.CH.Query.Internal.Translators;
using Microsoft.EntityFrameworkCore.Query;

namespace EF.CH.Query.Internal;

/// <summary>
/// Provides member translators for ClickHouse.
/// </summary>
public class ClickHouseMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public ClickHouseMemberTranslatorProvider(
        RelationalMemberTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        var sqlExpressionFactory = (ClickHouseSqlExpressionFactory)dependencies.SqlExpressionFactory;

        // Register ClickHouse-specific member translators
        AddTranslators(
        [
            new ClickHouseStringMemberTranslator(sqlExpressionFactory),
            new ClickHouseDateTimeMemberTranslator(sqlExpressionFactory),
            new ClickHouseDateOnlyMemberTranslator(sqlExpressionFactory),
            new ClickHouseGuidMemberTranslator(sqlExpressionFactory),
            new ClickHouseArrayMemberTranslator(sqlExpressionFactory),
            new ClickHouseMapMemberTranslator(sqlExpressionFactory),
        ]);
    }
}
