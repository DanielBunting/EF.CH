using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// LINQ <c>Count(predicate)</c> overload — translates to ClickHouse
/// <c>countIf(predicate)</c>. See <c>MaterializedViewSqlTranslator.TranslateCount</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvCountIfPredicateTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvCountIfPredicateTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Count_WithPredicate_TranslatesToCountIf()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new Row { Id = 1, Bucket = "a", Status = "ok"   },
            new Row { Id = 2, Bucket = "a", Status = "fail" },
            new Row { Id = 3, Bucket = "a", Status = "ok"   },
            new Row { Id = 4, Bucket = "b", Status = "fail" },
            new Row { Id = 5, Bucket = "b", Status = "fail" });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvCountIfTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Bucket, toInt64(Total) AS Total, toInt64(Failures) AS Failures FROM \"MvCountIfTarget\" FINAL ORDER BY Bucket");
        Assert.Equal(2, rows.Count);
        Assert.Equal("a", (string)rows[0]["Bucket"]!);
        Assert.Equal(3L, Convert.ToInt64(rows[0]["Total"]));
        Assert.Equal(1L, Convert.ToInt64(rows[0]["Failures"]));
        Assert.Equal("b", (string)rows[1]["Bucket"]!);
        Assert.Equal(2L, Convert.ToInt64(rows[1]["Total"]));
        Assert.Equal(2L, Convert.ToInt64(rows[1]["Failures"]));
    }

    public sealed class Row { public long Id { get; set; } public string Bucket { get; set; } = ""; public string Status { get; set; } = ""; }
    public sealed class Tgt { public string Bucket { get; set; } = ""; public long Total { get; set; } public long Failures { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Tgt> Target => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvCountIfSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvCountIfTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<Tgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => r.Bucket)
                    .Select(g => new Tgt
                    {
                        Bucket = g.Key,
                        Total = g.Count(),
                        Failures = g.Count(r => r.Status == "fail"),
                    }));
        }
    }
}
