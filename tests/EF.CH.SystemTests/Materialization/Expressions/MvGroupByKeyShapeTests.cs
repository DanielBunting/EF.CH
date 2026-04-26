using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// Each branch of <c>MaterializedViewSqlTranslator.VisitGroupByKeySelector</c>
/// gets its own Fact: anonymous type, single member, member-init, method call,
/// and the catch-all fallback (conditional, constant, binary, unary). Without
/// the fallback case <c>g.Key</c> projection emits the literal identifier
/// "Key" — caught originally by MvConditionalExpressionTests and now pinned
/// per-shape to localise any future regression.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvGroupByKeyShapeTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvGroupByKeyShapeTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Anonymous_CompositeKey()
    {
        await using var ctx = TestContextFactory.Create<AnonCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.AddRange(
            new Row { Id = 1, A = "x", B = 1, N = 5 },
            new Row { Id = 2, A = "x", B = 1, N = 7 },
            new Row { Id = 3, A = "x", B = 2, N = 3 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvGbAnonTarget");
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT A, toInt32(B) AS B, toInt64(Total) AS T FROM \"MvGbAnonTarget\" FINAL ORDER BY B");
        Assert.Equal(2, rows.Count);
        Assert.Equal(12L, Convert.ToInt64(rows[0]["T"]));
        Assert.Equal(3L,  Convert.ToInt64(rows[1]["T"]));
    }

    [Fact]
    public async Task SingleMember_Key()
    {
        await using var ctx = TestContextFactory.Create<MemberCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.AddRange(
            new Row { Id = 1, A = "x", N = 5 },
            new Row { Id = 2, A = "x", N = 7 },
            new Row { Id = 3, A = "y", N = 3 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvGbMemberTarget");
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT A, toInt64(Total) AS T FROM \"MvGbMemberTarget\" FINAL ORDER BY A");
        Assert.Equal(12L, Convert.ToInt64(rows[0]["T"]));
        Assert.Equal( 3L, Convert.ToInt64(rows[1]["T"]));
    }

    [Fact]
    public async Task MethodCall_Key()
    {
        await using var ctx = TestContextFactory.Create<MethodCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        var t = new DateTime(2026, 4, 25, 10, 0, 0, DateTimeKind.Utc);
        ctx.Source.AddRange(
            new Row { Id = 1, At = t,                N = 1 },
            new Row { Id = 2, At = t.AddMinutes(20), N = 1 },
            new Row { Id = 3, At = t.AddHours(1),    N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvGbMethodTarget");
        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "MvGbMethodTarget", final: true));
    }

    [Fact]
    public async Task MemberInit_Key()
    {
        await using var ctx = TestContextFactory.Create<InitCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.AddRange(
            new Row { Id = 1, A = "x", B = 1, N = 5 },
            new Row { Id = 2, A = "x", B = 1, N = 7 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvGbInitTarget");
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT A, toInt32(B) AS B, toInt64(Total) AS T FROM \"MvGbInitTarget\" FINAL");
        Assert.Equal(12L, Convert.ToInt64(rows[0]["T"]));
    }

    [Fact]
    public async Task ConstantKey_LiteralLong()
    {
        await using var ctx = TestContextFactory.Create<ConstCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.AddRange(
            new Row { Id = 1, N = 5 }, new Row { Id = 2, N = 7 }, new Row { Id = 3, N = 3 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvGbConstTarget");
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt64(Bucket) AS K, toInt64(Total) AS T FROM \"MvGbConstTarget\" FINAL");
        Assert.Single(rows);
        Assert.Equal(1L,  Convert.ToInt64(rows[0]["K"]));
        Assert.Equal(15L, Convert.ToInt64(rows[0]["T"]));
    }

    [Fact]
    public async Task BinaryKey_Arithmetic()
    {
        await using var ctx = TestContextFactory.Create<BinCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.AddRange(
            new Row { Id = 1, B = 10, N = 1 },
            new Row { Id = 2, B = 12, N = 1 },
            new Row { Id = 3, B =  3, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvGbBinTarget");
        // Key = B % 2 → 0 (even) for B in {10,12}; 1 (odd) for B = 3.
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt64(Parity) AS K, toInt64(Total) AS T FROM \"MvGbBinTarget\" FINAL ORDER BY K");
        Assert.Equal(2, rows.Count);
        Assert.Equal(2L, Convert.ToInt64(rows[0]["T"]));
        Assert.Equal(1L, Convert.ToInt64(rows[1]["T"]));
    }

    public sealed class Row
    {
        public long Id { get; set; }
        public string A { get; set; } = "";
        public int B { get; set; }
        public DateTime At { get; set; }
        public long N { get; set; }
    }

    public sealed class AnonTgt { public string A { get; set; } = ""; public int B { get; set; } public long Total { get; set; } }
    public sealed class MemberTgt { public string A { get; set; } = ""; public long Total { get; set; } }
    public sealed class MethodTgt { public DateTime Bucket { get; set; } public long Total { get; set; } }
    public sealed class InitTgt { public string A { get; set; } = ""; public int B { get; set; } public long Total { get; set; } }
    public sealed class ConstTgt { public long Bucket { get; set; } public long Total { get; set; } }
    public sealed class BinTgt { public long Parity { get; set; } public long Total { get; set; } }

    public sealed class AnonCtx(DbContextOptions<AnonCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<AnonTgt> Target => Set<AnonTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvGbAnonSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<AnonTgt>(e =>
            {
                e.ToTable("MvGbAnonTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => new { x.A, x.B });
                e.AsMaterializedView<AnonTgt, Row>(rows => rows
                    .GroupBy(r => new { r.A, r.B })
                    .Select(g => new AnonTgt { A = g.Key.A, B = g.Key.B, Total = g.Sum(r => r.N) }));
            });
        }
    }

    public sealed class MemberCtx(DbContextOptions<MemberCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<MemberTgt> Target => Set<MemberTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvGbMemberSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<MemberTgt>(e =>
            {
                e.ToTable("MvGbMemberTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.A);
                e.AsMaterializedView<MemberTgt, Row>(rows => rows
                    .GroupBy(r => r.A)
                    .Select(g => new MemberTgt { A = g.Key, Total = g.Sum(r => r.N) }));
            });
        }
    }

    public sealed class MethodCtx(DbContextOptions<MethodCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<MethodTgt> Target => Set<MethodTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvGbMethodSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<MethodTgt>(e =>
            {
                e.ToTable("MvGbMethodTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);
                e.AsMaterializedView<MethodTgt, Row>(rows => rows
                    .GroupBy(r => ClickHouseFunctions.ToStartOfHour(r.At))
                    .Select(g => new MethodTgt { Bucket = g.Key, Total = g.Sum(r => r.N) }));
            });
        }
    }

    public sealed class InitCtx(DbContextOptions<InitCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<InitTgt> Target => Set<InitTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvGbInitSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<InitTgt>(e =>
            {
                e.ToTable("MvGbInitTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => new { x.A, x.B });
                e.AsMaterializedView<InitTgt, Row>(rows => rows
                    .GroupBy(r => new InitTgt { A = r.A, B = r.B })
                    .Select(g => new InitTgt { A = g.Key.A, B = g.Key.B, Total = g.Sum(r => r.N) }));
            });
        }
    }

    public sealed class ConstCtx(DbContextOptions<ConstCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<ConstTgt> Target => Set<ConstTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvGbConstSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<ConstTgt>(e =>
            {
                e.ToTable("MvGbConstTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);
                e.AsMaterializedView<ConstTgt, Row>(rows => rows
                    .GroupBy(_ => 1L)
                    .Select(g => new ConstTgt { Bucket = g.Key, Total = g.Sum(r => r.N) }));
            });
        }
    }

    public sealed class BinCtx(DbContextOptions<BinCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<BinTgt> Target => Set<BinTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvGbBinSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<BinTgt>(e =>
            {
                e.ToTable("MvGbBinTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Parity);
                e.AsMaterializedView<BinTgt, Row>(rows => rows
                    .GroupBy(r => (long)(r.B % 2))
                    .Select(g => new BinTgt { Parity = g.Key, Total = g.Sum(r => r.N) }));
            });
        }
    }
}
