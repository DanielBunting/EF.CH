using System.Reflection;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// Reflection meta-test: every custom subtype of <see cref="TableExpressionBase"/>
/// in the EF.CH assembly must be discoverable via the assembly enumeration
/// path used by the SQL nullability processor and SQL generator. The actual
/// dispatch lives in <c>ClickHouseSqlNullabilityProcessor.Visit</c> and
/// <c>ClickHouseQuerySqlGenerator.VisitExtension</c>; this test pins the
/// inventory so a new subtype added without a corresponding visit case shows
/// up as a list mismatch.
/// </summary>
public class TableExpressionBaseSubtypeMetaTests
{
    [Fact]
    public void EveryCustomSubtype_IsHandledByNullabilityProcessor()
    {
        var asm = typeof(ClickHouseDbContextOptionsExtensions).Assembly;
        var subtypes = asm.GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract)
            .Where(t => typeof(TableExpressionBase).IsAssignableFrom(t))
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();

        // Pin the known subtypes. When a new one is added, append it here
        // AND wire the visit case in ClickHouseSqlNullabilityProcessor.Visit
        // and ClickHouseQuerySqlGenerator.VisitExtension; the meta-test
        // proves the pair stays in sync.
        var expected = new[]
        {
            "EF.CH.Query.Internal.ClickHouseCteReferenceExpression",
            "EF.CH.Query.Internal.ClickHouseDictionaryTableExpression",
            "EF.CH.Query.Internal.ClickHouseExternalTableFunctionExpression",
            "EF.CH.Query.Internal.ClickHouseTableModifierExpression",
        };

        var actual = subtypes.Select(t => t.FullName!).ToArray();
        Assert.Equal(expected, actual);
    }
}
