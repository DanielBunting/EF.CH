using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// LINQ <c>default(T)</c> in MV selectors — translates to ClickHouse-typed zero
/// literal (0 / 0.0 / '' / NULL). See <c>MaterializedViewSqlTranslator.TranslateDefault</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvDefaultExpressionTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvDefaultExpressionTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Default_LiteralProjection_ForEachPrimitive()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new Row { Id = 1, Original = 100 },
            new Row { Id = 2, Original = 200 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvDefaultTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt64(Id) AS Id, toInt64(ZeroLong) AS ZL, toFloat64(ZeroDouble) AS ZD, ZeroString AS ZS FROM \"MvDefaultTarget\" ORDER BY Id");
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r =>
        {
            Assert.Equal(0L,  Convert.ToInt64(r["ZL"]));
            Assert.Equal(0.0, Convert.ToDouble(r["ZD"]));
            Assert.Equal("",  (string)r["ZS"]!);
        });
    }

    public sealed class Row { public long Id { get; set; } public long Original { get; set; } }
    public sealed class Tgt
    {
        public long Id { get; set; }
        public long ZeroLong { get; set; }
        public double ZeroDouble { get; set; }
        public string ZeroString { get; set; } = "";
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Tgt> Target => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvDefaultSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvDefaultTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
                e.AsMaterializedView<Tgt, Row>(rows => rows
                    .Select(r => new Tgt
                    {
                        Id = r.Id,
                        ZeroLong = default(long),
                        ZeroDouble = default(double),
                        ZeroString = default(string)!,
                    }));
            });
        }
    }
}
