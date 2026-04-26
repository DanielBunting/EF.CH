using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// Inline <c>new DateTime(...)</c> literal in an MV Select projection. The C# compiler
/// emits a <c>NewExpression</c> in the expression tree, which
/// <c>MaterializedViewSqlTranslator.TranslateExpression</c> does not currently handle —
/// the default arm throws <c>NotSupportedException("Expression type NewExpression is
/// not supported")</c> at design time during <c>EnsureCreatedAsync</c>.
///
/// Currently red. Will go green when the translator gains a <c>NewExpression</c> arm
/// that constant-folds known constructible types via reflection.
/// TODO: green when MaterializedViewSqlTranslator handles NewExpression for known-constructible types.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvDateTimeConstructorLiteralTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvDateTimeConstructorLiteralTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task LinqDateTimeConstructor_ShouldEventuallyWork()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvDateTimeCtorTarget");

        var s = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT toString(V) FROM \"MvDateTimeCtorTarget\" LIMIT 1");
        Assert.StartsWith("2026-01-15 10:00:00", s);
    }

    public sealed class Row { public long Id { get; set; } public long N { get; set; } }
    public sealed class Tgt { public long Id { get; set; } public DateTime V { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>();
        public DbSet<Tgt> Target => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvDateTimeCtorSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvDateTimeCtorTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
                e.Property(x => x.V).HasColumnType("DateTime64(3, 'UTC')");
                // TODO: green when MaterializedViewSqlTranslator handles NewExpression for known-constructible types.
                e.AsMaterializedView<Tgt, Row>(rows => rows
                    .Select(r => new Tgt
                    {
                        Id = r.Id,
                        V = new DateTime(2026, 1, 15, 10, 0, 0, DateTimeKind.Utc),
                    }));
            });
        }
    }
}
