using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// Per-shape coverage of <c>TranslateMemberAccess</c>'s <c>g.Key</c> branch:
/// composite-key field access (<c>g.Key.X</c>), single-value direct access
/// (<c>g.Key</c>) when the key is a member, and the harder
/// "single-value-from-method-call key" path which falls through to
/// <c>_groupByColumns[0]</c> because <c>_groupKeyMappings</c> has no entry
/// for it. A regression in any of these silently swaps which column the MV
/// projects.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvGroupKeyAccessShapeTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvGroupKeyAccessShapeTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task CompositeKey_DotAccess()
    {
        await using var ctx = TestContextFactory.Create<CompCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.AddRange(
            new Row { Id = 1, A = "x", B = 1, N = 5 },
            new Row { Id = 2, A = "x", B = 1, N = 7 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvKeyCompTarget");
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT A, toInt32(B) AS B, toInt64(Total) AS T FROM \"MvKeyCompTarget\" FINAL");
        Assert.Equal("x", (string)rows[0]["A"]!);
        Assert.Equal(1,   Convert.ToInt32(rows[0]["B"]));
        Assert.Equal(12L, Convert.ToInt64(rows[0]["T"]));
    }

    [Fact]
    public async Task SingleMemberKey_DirectAccess()
    {
        await using var ctx = TestContextFactory.Create<MemCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.AddRange(
            new Row { Id = 1, A = "x", N = 5 },
            new Row { Id = 2, A = "x", N = 7 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvKeyMemTarget");
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT A, toInt64(Total) AS T FROM \"MvKeyMemTarget\" FINAL");
        Assert.Equal("x", (string)rows[0]["A"]!);
        Assert.Equal(12L, Convert.ToInt64(rows[0]["T"]));
    }

    [Fact]
    public async Task SingleMethodCallKey_DirectAccess()
    {
        // The key is a method call (toStartOfHour). _groupKeyMappings is empty
        // for method calls, so g.Key access falls through to _groupByColumns[0].
        await using var ctx = TestContextFactory.Create<MethodCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        var t = new DateTime(2026, 4, 25, 10, 30, 0, DateTimeKind.Utc);
        ctx.Source.AddRange(
            new Row { Id = 1, At = t,                N = 5 },
            new Row { Id = 2, At = t.AddMinutes(15), N = 7 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvKeyMethodTarget");
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toString(Bucket) AS Bucket, toInt64(Total) AS T FROM \"MvKeyMethodTarget\" FINAL");
        Assert.Equal("2026-04-25 10:00:00", (string)rows[0]["Bucket"]!);
        Assert.Equal(12L, Convert.ToInt64(rows[0]["T"]));
    }

    [Fact]
    public async Task SingleConstantKey_DirectAccess()
    {
        await using var ctx = TestContextFactory.Create<ConstCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.AddRange(
            new Row { Id = 1, N = 5 }, new Row { Id = 2, N = 7 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvKeyConstTarget");
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt64(Bucket) AS K, toInt64(Total) AS T FROM \"MvKeyConstTarget\" FINAL");
        Assert.Equal(1L,  Convert.ToInt64(rows[0]["K"]));
        Assert.Equal(12L, Convert.ToInt64(rows[0]["T"]));
    }

    public sealed class Row
    {
        public long Id { get; set; }
        public string A { get; set; } = "";
        public int B { get; set; }
        public DateTime At { get; set; }
        public long N { get; set; }
    }

    public sealed class CompTgt   { public string A { get; set; } = ""; public int B { get; set; } public long Total { get; set; } }
    public sealed class MemTgt    { public string A { get; set; } = ""; public long Total { get; set; } }
    public sealed class MethodTgt { public DateTime Bucket { get; set; } public long Total { get; set; } }
    public sealed class ConstTgt  { public long Bucket { get; set; } public long Total { get; set; } }

    public sealed class CompCtx(DbContextOptions<CompCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<CompTgt> Target => Set<CompTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvKeyCompSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<CompTgt>(e =>
            {
                e.ToTable("MvKeyCompTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => new { x.A, x.B });

            });
            mb.MaterializedView<CompTgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => new { r.A, r.B })
                    .Select(g => new CompTgt { A = g.Key.A, B = g.Key.B, Total = g.Sum(r => r.N) }));
        }
    }

    public sealed class MemCtx(DbContextOptions<MemCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<MemTgt> Target => Set<MemTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvKeyMemSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<MemTgt>(e =>
            {
                e.ToTable("MvKeyMemTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.A);

            });
            mb.MaterializedView<MemTgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => r.A)
                    .Select(g => new MemTgt { A = g.Key, Total = g.Sum(r => r.N) }));
        }
    }

    public sealed class MethodCtx(DbContextOptions<MethodCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<MethodTgt> Target => Set<MethodTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvKeyMethodSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<MethodTgt>(e =>
            {
                e.ToTable("MvKeyMethodTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<MethodTgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => ClickHouseFunctions.ToStartOfHour(r.At))
                    .Select(g => new MethodTgt { Bucket = g.Key, Total = g.Sum(r => r.N) }));
        }
    }

    public sealed class ConstCtx(DbContextOptions<ConstCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<ConstTgt> Target => Set<ConstTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvKeyConstSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<ConstTgt>(e =>
            {
                e.ToTable("MvKeyConstTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<ConstTgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(_ => 1L)
                    .Select(g => new ConstTgt { Bucket = g.Key, Total = g.Sum(r => r.N) }));
        }
    }
}
