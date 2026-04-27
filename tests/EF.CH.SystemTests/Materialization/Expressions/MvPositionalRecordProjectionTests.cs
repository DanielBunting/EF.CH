using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// Pins the <c>NewExpression</c> branch of <c>VisitSelectProjection</c>
/// (lines 711–722). Reachable via positional-record target projected through
/// constructor call: <c>new Tgt(g.Key, g.Sum(...))</c> produces a
/// <c>NewExpression</c> with <c>Members</c> populated from the record's
/// positional parameters. The existing tests all hit the
/// <c>MemberInitExpression</c> branch via <c>new Tgt { … = … }</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvPositionalRecordProjectionTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvPositionalRecordProjectionTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task RecordPositionalConstructor_HitsNewExpressionBranch()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new Row { Id = 1, A = "x", N = 5 },
            new Row { Id = 2, A = "x", N = 7 },
            new Row { Id = 3, A = "y", N = 3 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvRecordProjTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT A, toInt64(Total) AS T FROM \"MvRecordProjTarget\" FINAL ORDER BY A");
        Assert.Equal(2, rows.Count);
        Assert.Equal("x", (string)rows[0]["A"]!); Assert.Equal(12L, Convert.ToInt64(rows[0]["T"]));
        Assert.Equal("y", (string)rows[1]["A"]!); Assert.Equal( 3L, Convert.ToInt64(rows[1]["T"]));
    }

    public sealed class Row { public long Id { get; set; } public string A { get; set; } = ""; public long N { get; set; } }
    public sealed record Tgt(string A, long Total);

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>();
        public DbSet<Tgt> Target => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvRecordProjSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvRecordProjTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.A);

            });
            mb.MaterializedView<Tgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => r.A)
                    .Select(g => new Tgt(g.Key, g.Sum(r => r.N))));
        }
    }
}
