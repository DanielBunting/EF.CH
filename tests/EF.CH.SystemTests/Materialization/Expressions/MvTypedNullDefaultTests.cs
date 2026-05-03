using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// Per-type coverage of <c>TranslateConstant</c>'s typed-null dispatch
/// (<c>TypedNullDefault</c>). C# emits <c>Constant(null, typeof(T))</c> for
/// <c>default(T)</c> when T is a reference type — bare <c>NULL</c> would be
/// rejected by ClickHouse as <c>Nullable(Nothing)</c>. Each Fact pins one
/// arm of the dispatch table so a regression is localised to the type.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvTypedNullDefaultTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvTypedNullDefaultTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task DefaultString_EmitsEmptyStringLiteral()
    {
        await using var ctx = TestContextFactory.Create<StringCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvNullDefStringTarget");
        var v = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT Zero FROM \"MvNullDefStringTarget\" LIMIT 1");
        Assert.Equal("", v);
    }

    [Fact]
    public async Task DefaultLong_EmitsZero()
    {
        await using var ctx = TestContextFactory.Create<LongCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvNullDefLongTarget");
        Assert.Equal(0L, await RawClickHouse.ScalarAsync<long>(Conn,
            "SELECT toInt64(Zero) FROM \"MvNullDefLongTarget\" LIMIT 1"));
    }

    [Fact]
    public async Task DefaultDouble_EmitsZero()
    {
        await using var ctx = TestContextFactory.Create<DoubleCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvNullDefDoubleTarget");
        Assert.Equal(0.0, await RawClickHouse.ScalarAsync<double>(Conn,
            "SELECT toFloat64(Zero) FROM \"MvNullDefDoubleTarget\" LIMIT 1"));
    }

    [Fact]
    public async Task DefaultBool_EmitsZero()
    {
        await using var ctx = TestContextFactory.Create<BoolCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvNullDefBoolTarget");
        var b = await RawClickHouse.ScalarAsync<byte>(Conn,
            "SELECT toUInt8(Zero) FROM \"MvNullDefBoolTarget\" LIMIT 1");
        Assert.Equal((byte)0, b);
    }

    [Fact]
    public async Task DefaultInt_EmitsZero()
    {
        await using var ctx = TestContextFactory.Create<IntCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvNullDefIntTarget");
        Assert.Equal(0, await RawClickHouse.ScalarAsync<int>(Conn,
            "SELECT toInt32(Zero) FROM \"MvNullDefIntTarget\" LIMIT 1"));
    }

    public sealed class Row { public long Id { get; set; } public long N { get; set; } }

    public sealed class StringTgt { public long Id { get; set; } public string Zero { get; set; } = ""; public long N { get; set; } }
    public sealed class LongTgt   { public long Id { get; set; } public long   Zero { get; set; } public long N { get; set; } }
    public sealed class DoubleTgt { public long Id { get; set; } public double Zero { get; set; } public long N { get; set; } }
    public sealed class BoolTgt   { public long Id { get; set; } public bool   Zero { get; set; } public long N { get; set; } }
    public sealed class IntTgt    { public long Id { get; set; } public int    Zero { get; set; } public long N { get; set; } }

    public sealed class StringCtx(DbContextOptions<StringCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<StringTgt> Target => Set<StringTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvNullDefStringSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<StringTgt>(e =>
            {
                e.ToTable("MvNullDefStringTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);

            });
            mb.MaterializedView<StringTgt>().From<Row>().DefinedAs(rows => rows
                    .Select(r => new StringTgt { Id = r.Id, Zero = default(string)!, N = r.N }));
        }
    }

    public sealed class LongCtx(DbContextOptions<LongCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<LongTgt> Target => Set<LongTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvNullDefLongSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<LongTgt>(e =>
            {
                e.ToTable("MvNullDefLongTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);

            });
            mb.MaterializedView<LongTgt>().From<Row>().DefinedAs(rows => rows
                    .Select(r => new LongTgt { Id = r.Id, Zero = default(long), N = r.N }));
        }
    }

    public sealed class DoubleCtx(DbContextOptions<DoubleCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<DoubleTgt> Target => Set<DoubleTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvNullDefDoubleSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<DoubleTgt>(e =>
            {
                e.ToTable("MvNullDefDoubleTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);

            });
            mb.MaterializedView<DoubleTgt>().From<Row>().DefinedAs(rows => rows
                    .Select(r => new DoubleTgt { Id = r.Id, Zero = default(double), N = r.N }));
        }
    }

    public sealed class BoolCtx(DbContextOptions<BoolCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<BoolTgt> Target => Set<BoolTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvNullDefBoolSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<BoolTgt>(e =>
            {
                e.ToTable("MvNullDefBoolTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);

            });
            mb.MaterializedView<BoolTgt>().From<Row>().DefinedAs(rows => rows
                    .Select(r => new BoolTgt { Id = r.Id, Zero = default(bool), N = r.N }));
        }
    }

    public sealed class IntCtx(DbContextOptions<IntCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<IntTgt> Target => Set<IntTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvNullDefIntSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<IntTgt>(e =>
            {
                e.ToTable("MvNullDefIntTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);

            });
            mb.MaterializedView<IntTgt>().From<Row>().DefinedAs(rows => rows
                    .Select(r => new IntTgt { Id = r.Id, Zero = default(int), N = r.N }));
        }
    }
}
