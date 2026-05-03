using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// Each comparison binary operator (=, !=, &lt;, &lt;=, &gt;, &gt;=) used inside the
/// MV's WHERE predicate. Each Fact has its own Ctx for isolated failure surfaces.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvBinaryComparisonExpressionTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvBinaryComparisonExpressionTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact] public Task Equal_InWhere()              => Run<EqCtx>("MvCmpEqTarget",  expectedHits: 1);
    [Fact] public Task NotEqual_InWhere()           => Run<NeqCtx>("MvCmpNeqTarget", expectedHits: 4);
    [Fact] public Task LessThan_InWhere()           => Run<LtCtx>("MvCmpLtTarget",   expectedHits: 2);
    [Fact] public Task LessThanOrEqual_InWhere()    => Run<LeCtx>("MvCmpLeTarget",   expectedHits: 3);
    [Fact] public Task GreaterThan_InWhere()        => Run<GtCtx>("MvCmpGtTarget",   expectedHits: 2);
    [Fact] public Task GreaterThanOrEqual_InWhere() => Run<GeCtx>("MvCmpGeTarget",   expectedHits: 3);

    private async Task Run<TCtx>(string targetTable, int expectedHits) where TCtx : DbContext
    {
        await using var ctx = TestContextFactory.Create<TCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();

        // Seed 5 rows with values 1..5
        for (long i = 1; i <= 5; i++)
            await RawClickHouse.ExecuteAsync(Conn,
                $"INSERT INTO \"{ResolveSourceTable(targetTable)}\" (\"Id\", \"Value\") VALUES ({i}, {i})");

        await RawClickHouse.SettleMaterializationAsync(Conn, targetTable);
        Assert.Equal((ulong)expectedHits, await RawClickHouse.RowCountAsync(Conn, targetTable));
    }

    private static string ResolveSourceTable(string targetTable) => targetTable.Replace("Target", "Source");

    public sealed class Row { public long Id { get; set; } public long Value { get; set; } }
    public sealed class Target { public long Id { get; set; } public long Value { get; set; } }

    public sealed class EqCtx(DbContextOptions<EqCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Target> Target => Set<Target>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvCmpEqSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Target>(e => { e.ToTable("MvCmpEqTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
 });
            mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows.Where(r => r.Value == 3).Select(r => new Target { Id = r.Id, Value = r.Value }));
        }
    }
    public sealed class NeqCtx(DbContextOptions<NeqCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Target> Target => Set<Target>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvCmpNeqSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Target>(e => { e.ToTable("MvCmpNeqTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
 });
            mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows.Where(r => r.Value != 3).Select(r => new Target { Id = r.Id, Value = r.Value }));
        }
    }
    public sealed class LtCtx(DbContextOptions<LtCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Target> Target => Set<Target>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvCmpLtSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Target>(e => { e.ToTable("MvCmpLtTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
 });
            mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows.Where(r => r.Value < 3).Select(r => new Target { Id = r.Id, Value = r.Value }));
        }
    }
    public sealed class LeCtx(DbContextOptions<LeCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Target> Target => Set<Target>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvCmpLeSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Target>(e => { e.ToTable("MvCmpLeTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
 });
            mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows.Where(r => r.Value <= 3).Select(r => new Target { Id = r.Id, Value = r.Value }));
        }
    }
    public sealed class GtCtx(DbContextOptions<GtCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Target> Target => Set<Target>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvCmpGtSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Target>(e => { e.ToTable("MvCmpGtTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
 });
            mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows.Where(r => r.Value > 3).Select(r => new Target { Id = r.Id, Value = r.Value }));
        }
    }
    public sealed class GeCtx(DbContextOptions<GeCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Target> Target => Set<Target>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvCmpGeSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Target>(e => { e.ToTable("MvCmpGeTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
 });
            mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows.Where(r => r.Value >= 3).Select(r => new Target { Id = r.Id, Value = r.Value }));
        }
    }
}
