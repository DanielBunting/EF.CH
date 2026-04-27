using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// Each arithmetic binary operator (+, -, *, /, %) used inside an MV
/// aggregate selector. Per-operator Ctx so each failure surface is isolated.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvBinaryArithmeticExpressionTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvBinaryArithmeticExpressionTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact] public Task Add_InSelector()      => RunAdd();
    [Fact] public Task Subtract_InSelector() => RunSub();
    [Fact] public Task Multiply_InSelector() => RunMul();
    [Fact] public Task Divide_InSelector()   => RunDiv();
    [Fact] public Task Modulo_InSelector()   => RunMod();

    private async Task RunAdd()
    {
        await using var ctx = TestContextFactory.Create<AddCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.AddRange(
            new Row { Id = 1, Bucket = "x", A = 100, B = 10 },
            new Row { Id = 2, Bucket = "x", A =  20, B =  5 },
            new Row { Id = 3, Bucket = "y", A =  10, B = 20 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvArithAddTarget");
        var rows = await RawClickHouse.RowsAsync(Conn, "SELECT Bucket, toInt64(Value) AS V FROM \"MvArithAddTarget\" FINAL ORDER BY Bucket");
        Assert.Equal(135L, Convert.ToInt64(rows[0]["V"]));
        Assert.Equal( 30L, Convert.ToInt64(rows[1]["V"]));
    }
    private async Task RunSub()
    {
        await using var ctx = TestContextFactory.Create<SubCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.AddRange(
            new Row { Id = 1, Bucket = "x", A = 100, B = 10 },
            new Row { Id = 2, Bucket = "x", A =  20, B =  5 },
            new Row { Id = 3, Bucket = "y", A =  10, B = 20 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvArithSubTarget");
        var rows = await RawClickHouse.RowsAsync(Conn, "SELECT Bucket, toInt64(Value) AS V FROM \"MvArithSubTarget\" FINAL ORDER BY Bucket");
        Assert.Equal(105L, Convert.ToInt64(rows[0]["V"]));
        Assert.Equal(-10L, Convert.ToInt64(rows[1]["V"]));
    }
    private async Task RunMul()
    {
        await using var ctx = TestContextFactory.Create<MulCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.AddRange(
            new Row { Id = 1, Bucket = "x", A = 100, B = 10 },
            new Row { Id = 2, Bucket = "x", A =  20, B =  5 },
            new Row { Id = 3, Bucket = "y", A =  10, B = 20 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvArithMulTarget");
        var rows = await RawClickHouse.RowsAsync(Conn, "SELECT Bucket, toInt64(Value) AS V FROM \"MvArithMulTarget\" FINAL ORDER BY Bucket");
        Assert.Equal(1100L, Convert.ToInt64(rows[0]["V"]));
        Assert.Equal( 200L, Convert.ToInt64(rows[1]["V"]));
    }
    private async Task RunDiv()
    {
        await using var ctx = TestContextFactory.Create<DivCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        // Note: SummingMergeTree drops rows where every numeric column is 0,
        // so use values where every bucket sums to non-zero.
        ctx.Source.AddRange(
            new Row { Id = 1, Bucket = "x", A = 100, B = 10 },
            new Row { Id = 2, Bucket = "x", A =  20, B =  5 },
            new Row { Id = 3, Bucket = "y", A =  30, B =  6 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvArithDivTarget");
        var rows = await RawClickHouse.RowsAsync(Conn, "SELECT Bucket, toInt64(Value) AS V FROM \"MvArithDivTarget\" FINAL ORDER BY Bucket");
        Assert.Equal(14L, Convert.ToInt64(rows[0]["V"]));     // 100/10 + 20/5 = 10 + 4
        Assert.Equal( 5L, Convert.ToInt64(rows[1]["V"]));     // 30/6 = 5
    }
    private async Task RunMod()
    {
        await using var ctx = TestContextFactory.Create<ModCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.AddRange(
            new Row { Id = 1, Bucket = "x", A = 103, B = 10 },
            new Row { Id = 2, Bucket = "x", A =  22, B =  5 },
            new Row { Id = 3, Bucket = "y", A =  10, B = 20 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvArithModTarget");
        var rows = await RawClickHouse.RowsAsync(Conn, "SELECT Bucket, toInt64(Value) AS V FROM \"MvArithModTarget\" FINAL ORDER BY Bucket");
        Assert.Equal( 5L, Convert.ToInt64(rows[0]["V"]));     // 103%10 + 22%5 = 3 + 2
        Assert.Equal(10L, Convert.ToInt64(rows[1]["V"]));     // 10%20 = 10
    }

    public sealed class Row { public long Id { get; set; } public string Bucket { get; set; } = ""; public long A { get; set; } public long B { get; set; } }
    public sealed class Target { public string Bucket { get; set; } = ""; public long Value { get; set; } }

    public sealed class AddCtx(DbContextOptions<AddCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Target> Target => Set<Target>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvArithAddSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Target>(e =>
            {
                e.ToTable("MvArithAddTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => r.Bucket)
                    .Select(g => new Target { Bucket = g.Key, Value = g.Sum(r => r.A + r.B) }));
        }
    }
    public sealed class SubCtx(DbContextOptions<SubCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Target> Target => Set<Target>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvArithSubSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Target>(e =>
            {
                e.ToTable("MvArithSubTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => r.Bucket)
                    .Select(g => new Target { Bucket = g.Key, Value = g.Sum(r => r.A - r.B) }));
        }
    }
    public sealed class MulCtx(DbContextOptions<MulCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Target> Target => Set<Target>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvArithMulSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Target>(e =>
            {
                e.ToTable("MvArithMulTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => r.Bucket)
                    .Select(g => new Target { Bucket = g.Key, Value = g.Sum(r => r.A * r.B) }));
        }
    }
    public sealed class DivCtx(DbContextOptions<DivCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Target> Target => Set<Target>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvArithDivSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Target>(e =>
            {
                e.ToTable("MvArithDivTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => r.Bucket)
                    .Select(g => new Target { Bucket = g.Key, Value = g.Sum(r => r.A / r.B) }));
        }
    }
    public sealed class ModCtx(DbContextOptions<ModCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Target> Target => Set<Target>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvArithModSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Target>(e =>
            {
                e.ToTable("MvArithModTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<Target>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => r.Bucket)
                    .Select(g => new Target { Bucket = g.Key, Value = g.Sum(r => r.A % r.B) }));
        }
    }
}
