using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>Boolean AndAlso / OrElse / mixed combinations in MV WHERE predicates.</summary>
[Collection(SingleNodeCollection.Name)]
public class MvBooleanLogicExpressionTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvBooleanLogicExpressionTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task AndAlso_InWhere()
    {
        await using var ctx = TestContextFactory.Create<AndCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        for (long i = 1; i <= 5; i++)
            await RawClickHouse.ExecuteAsync(Conn, $"INSERT INTO \"MvBoolAndSource\" (\"Id\", \"A\", \"B\") VALUES ({i}, {i}, {i*10})");
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvBoolAndTarget");
        // A > 1 AND B < 50 → rows where i in {2,3,4} → 3 rows
        Assert.Equal(3UL, await RawClickHouse.RowCountAsync(Conn, "MvBoolAndTarget"));
    }

    [Fact]
    public async Task OrElse_InWhere()
    {
        await using var ctx = TestContextFactory.Create<OrCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        for (long i = 1; i <= 5; i++)
            await RawClickHouse.ExecuteAsync(Conn, $"INSERT INTO \"MvBoolOrSource\" (\"Id\", \"A\", \"B\") VALUES ({i}, {i}, {i*10})");
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvBoolOrTarget");
        // A == 1 OR B == 50 → rows where i in {1,5} → 2 rows
        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "MvBoolOrTarget"));
    }

    [Fact]
    public async Task Mixed_AndOr_WithGrouping()
    {
        await using var ctx = TestContextFactory.Create<MixedCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        for (long i = 1; i <= 5; i++)
            await RawClickHouse.ExecuteAsync(Conn, $"INSERT INTO \"MvBoolMixedSource\" (\"Id\", \"A\", \"B\") VALUES ({i}, {i}, {i*10})");
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvBoolMixedTarget");
        // (A < 3) OR (A > 4 AND B == 50) → rows where i in {1,2,5} → 3 rows
        Assert.Equal(3UL, await RawClickHouse.RowCountAsync(Conn, "MvBoolMixedTarget"));
    }

    public sealed class Row { public long Id { get; set; } public long A { get; set; } public long B { get; set; } }
    public sealed class Target { public long Id { get; set; } public long A { get; set; } public long B { get; set; } }

    public sealed class AndCtx(DbContextOptions<AndCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Target> Target => Set<Target>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvBoolAndSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Target>(e =>
            {
                e.ToTable("MvBoolAndTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);

            });
            mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows
                    .Where(r => r.A > 1 && r.B < 50)
                    .Select(r => new Target { Id = r.Id, A = r.A, B = r.B }));
        }
    }
    public sealed class OrCtx(DbContextOptions<OrCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Target> Target => Set<Target>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvBoolOrSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Target>(e =>
            {
                e.ToTable("MvBoolOrTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);

            });
            mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows
                    .Where(r => r.A == 1 || r.B == 50)
                    .Select(r => new Target { Id = r.Id, A = r.A, B = r.B }));
        }
    }
    public sealed class MixedCtx(DbContextOptions<MixedCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Target> Target => Set<Target>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvBoolMixedSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Target>(e =>
            {
                e.ToTable("MvBoolMixedTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);

            });
            mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows
                    .Where(r => r.A < 3 || (r.A > 4 && r.B == 50))
                    .Select(r => new Target { Id = r.Id, A = r.A, B = r.B }));
        }
    }
}
