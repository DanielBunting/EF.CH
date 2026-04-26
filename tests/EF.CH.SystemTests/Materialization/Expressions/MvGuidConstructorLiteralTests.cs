using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// Inline <c>new Guid(string)</c> literal in an MV Select projection — same
/// <c>NewExpression</c> path as the DateTime ctor case. Currently caught by
/// <c>TranslateExpression</c>'s default arm.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvGuidConstructorLiteralTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvGuidConstructorLiteralTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task LinqGuidConstructor_ShouldEventuallyWork()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvGuidCtorTarget");

        var s = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT toString(V) FROM \"MvGuidCtorTarget\" LIMIT 1");
        Assert.Equal("11111111-1111-1111-1111-111111111111", s);
    }

    public sealed class Row { public long Id { get; set; } public long N { get; set; } }
    public sealed class Tgt { public long Id { get; set; } public Guid V { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>();
        public DbSet<Tgt> Target => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvGuidCtorSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvGuidCtorTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
                e.AsMaterializedView<Tgt, Row>(rows => rows
                    .Select(r => new Tgt
                    {
                        Id = r.Id,
                        V = new Guid("11111111-1111-1111-1111-111111111111"),
                    }));
            });
        }
    }
}
