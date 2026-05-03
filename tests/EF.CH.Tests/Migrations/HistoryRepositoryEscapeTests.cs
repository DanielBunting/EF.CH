using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Pins the SQL-literal escape used by <see cref="ClickHouseHistoryRepository"/>'s
/// <c>GetInsertScript</c> / <c>GetDeleteScript</c>. The previous implementation
/// escaped <c>'</c> as <c>\'</c> but did not escape <c>\</c> first — a value
/// ending in <c>\</c> (or containing <c>\'</c>) breaks out of the surrounding
/// <c>'…'</c> literal because ClickHouse interprets <c>\</c> as a C-style
/// escape character. The same bug class was fixed in R2 for the External /
/// Dictionary / Enum / JSON resolvers; this is the migrations-history-table
/// path that R2 missed.
/// </summary>
public class HistoryRepositoryEscapeTests
{
    public static IEnumerable<object[]> Vectors() =>
    [
        ["plain", "1.0", "'plain'", "'1.0'"],
        ["with'apos", "1.0", @"'with\'apos'", "'1.0'"],
        [@"trailing\", "1.0", @"'trailing\\'", "'1.0'"],
        [@"a\'b", "1.0", @"'a\\\'b'", "'1.0'"],
        [@"a'\b", "1.0", @"'a\'\\b'", "'1.0'"],
    ];

    [Theory]
    [MemberData(nameof(Vectors))]
    public void GetInsertScript_EscapesValues(string migrationId, string productVersion, string expectedMigrationLiteral, string expectedVersionLiteral)
    {
        using var ctx = Create();
        var repo = ctx.GetService<IHistoryRepository>();

        var sql = repo.GetInsertScript(new HistoryRow(migrationId, productVersion));

        Assert.Contains(expectedMigrationLiteral, sql);
        Assert.Contains(expectedVersionLiteral, sql);
    }

    [Theory]
    [InlineData(@"trailing\", @"'trailing\\'")]
    [InlineData(@"a\'b", @"'a\\\'b'")]
    public void GetDeleteScript_EscapesMigrationId(string migrationId, string expectedLiteral)
    {
        using var ctx = Create();
        var repo = ctx.GetService<IHistoryRepository>();

        var sql = repo.GetDeleteScript(migrationId);

        Assert.Contains(expectedLiteral, sql);
    }

    private static HistoryEscCtx Create() =>
        new(new DbContextOptionsBuilder<HistoryEscCtx>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options);

    public sealed class HistoryEscCtx(DbContextOptions<HistoryEscCtx> o) : DbContext(o);
}
