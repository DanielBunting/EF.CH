using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// Inline <c>new DateTime(...)</c> literal in an MV Select projection. The C#
/// compiler emits a <c>NewExpression</c> in the expression tree, and the
/// materialized-view translator should constant-fold known constructible
/// literals into ClickHouse SQL.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvDateTimeConstructorLiteralTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvDateTimeConstructorLiteralTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task LinqDateTimeConstructor_ConstantFoldsLiteral()
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

            });
            mb.MaterializedView<Tgt>().From<Row>().DefinedAs(rows => rows
                    .Select(r => new Tgt
                    {
                        Id = r.Id,
                        V = new DateTime(2026, 1, 15, 10, 0, 0, DateTimeKind.Utc),
                    }));
        }
    }
}
