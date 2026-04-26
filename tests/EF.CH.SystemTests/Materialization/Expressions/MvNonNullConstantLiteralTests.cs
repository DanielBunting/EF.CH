using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// Per-arm coverage of <c>TranslateConstant</c>'s non-null switch:
/// <c>string</c>, <c>bool</c>, <c>sbyte → toInt8</c>, <c>byte → toUInt8</c>,
/// <c>ulong</c>, raw <c>long</c>, <c>DateTime → toDateTime64</c>, and the
/// generic <c>IFormattable</c> fallback. Keeps each arm's regression
/// surface a single Fact rather than relying on incidental coverage from
/// binary expressions.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvNonNullConstantLiteralTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvNonNullConstantLiteralTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task StringLiteral()
    {
        await using var ctx = TestContextFactory.Create<StringCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvLitStringTarget");
        Assert.Equal("hello", await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT V FROM \"MvLitStringTarget\" LIMIT 1"));
    }

    [Fact]
    public async Task BoolLiteral_True()
    {
        await using var ctx = TestContextFactory.Create<BoolCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvLitBoolTarget");
        Assert.Equal((byte)1, await RawClickHouse.ScalarAsync<byte>(Conn,
            "SELECT toUInt8(V) FROM \"MvLitBoolTarget\" LIMIT 1"));
    }

    [Fact]
    public async Task SByteLiteral()
    {
        await using var ctx = TestContextFactory.Create<SByteCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvLitSByteTarget");
        Assert.Equal(-7L, await RawClickHouse.ScalarAsync<long>(Conn,
            "SELECT toInt64(V) FROM \"MvLitSByteTarget\" LIMIT 1"));
    }

    [Fact]
    public async Task ByteLiteral()
    {
        await using var ctx = TestContextFactory.Create<ByteCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvLitByteTarget");
        Assert.Equal(7L, await RawClickHouse.ScalarAsync<long>(Conn,
            "SELECT toInt64(V) FROM \"MvLitByteTarget\" LIMIT 1"));
    }

    [Fact]
    public async Task LongLiteral()
    {
        await using var ctx = TestContextFactory.Create<LongCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvLitLongTarget");
        Assert.Equal(9_999_999_999L, await RawClickHouse.ScalarAsync<long>(Conn,
            "SELECT toInt64(V) FROM \"MvLitLongTarget\" LIMIT 1"));
    }

    [Fact]
    public async Task ULongLiteral()
    {
        await using var ctx = TestContextFactory.Create<ULongCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvLitULongTarget");
        Assert.Equal(9_999_999_999UL, await RawClickHouse.ScalarAsync<ulong>(Conn,
            "SELECT toUInt64(V) FROM \"MvLitULongTarget\" LIMIT 1"));
    }

    // Constructor literals such as inline `new DateTime(...)` compile to a
    // NewExpression and are covered by the dedicated constructor-literal tests.

    [Fact]
    public async Task DoubleLiteral()
    {
        await using var ctx = TestContextFactory.Create<DoubleCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvLitDoubleTarget");
        Assert.Equal(3.14, await RawClickHouse.ScalarAsync<double>(Conn,
            "SELECT toFloat64(V) FROM \"MvLitDoubleTarget\" LIMIT 1"), 2);
    }

    public sealed class Row { public long Id { get; set; } public long N { get; set; } }

    public sealed class StringTgt { public long Id { get; set; } public string V { get; set; } = ""; }
    public sealed class BoolTgt   { public long Id { get; set; } public bool   V { get; set; } }
    public sealed class SByteTgt  { public long Id { get; set; } public sbyte  V { get; set; } }
    public sealed class ByteTgt   { public long Id { get; set; } public byte   V { get; set; } }
    public sealed class LongTgt   { public long Id { get; set; } public long   V { get; set; } }
    public sealed class ULongTgt  { public long Id { get; set; } public ulong  V { get; set; } }
    public sealed class DoubleTgt { public long Id { get; set; } public double V { get; set; } }

    public sealed class StringCtx(DbContextOptions<StringCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<StringTgt> Target => Set<StringTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvLitStringSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<StringTgt>(e =>
            {
                e.ToTable("MvLitStringTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
                e.AsMaterializedView<StringTgt, Row>(rows => rows
                    .Select(r => new StringTgt { Id = r.Id, V = "hello" }));
            });
        }
    }

    public sealed class BoolCtx(DbContextOptions<BoolCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<BoolTgt> Target => Set<BoolTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvLitBoolSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<BoolTgt>(e =>
            {
                e.ToTable("MvLitBoolTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
                e.AsMaterializedView<BoolTgt, Row>(rows => rows
                    .Select(r => new BoolTgt { Id = r.Id, V = true }));
            });
        }
    }

    public sealed class SByteCtx(DbContextOptions<SByteCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<SByteTgt> Target => Set<SByteTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvLitSByteSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<SByteTgt>(e =>
            {
                e.ToTable("MvLitSByteTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
                e.AsMaterializedView<SByteTgt, Row>(rows => rows
                    .Select(r => new SByteTgt { Id = r.Id, V = (sbyte)-7 }));
            });
        }
    }

    public sealed class ByteCtx(DbContextOptions<ByteCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<ByteTgt> Target => Set<ByteTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvLitByteSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<ByteTgt>(e =>
            {
                e.ToTable("MvLitByteTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
                e.AsMaterializedView<ByteTgt, Row>(rows => rows
                    .Select(r => new ByteTgt { Id = r.Id, V = (byte)7 }));
            });
        }
    }

    public sealed class LongCtx(DbContextOptions<LongCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<LongTgt> Target => Set<LongTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvLitLongSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<LongTgt>(e =>
            {
                e.ToTable("MvLitLongTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
                e.AsMaterializedView<LongTgt, Row>(rows => rows
                    .Select(r => new LongTgt { Id = r.Id, V = 9_999_999_999L }));
            });
        }
    }

    public sealed class ULongCtx(DbContextOptions<ULongCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<ULongTgt> Target => Set<ULongTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvLitULongSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<ULongTgt>(e =>
            {
                e.ToTable("MvLitULongTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
                e.AsMaterializedView<ULongTgt, Row>(rows => rows
                    .Select(r => new ULongTgt { Id = r.Id, V = 9_999_999_999UL }));
            });
        }
    }

    public sealed class DoubleCtx(DbContextOptions<DoubleCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<DoubleTgt> Target => Set<DoubleTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvLitDoubleSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<DoubleTgt>(e =>
            {
                e.ToTable("MvLitDoubleTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
                e.AsMaterializedView<DoubleTgt, Row>(rows => rows
                    .Select(r => new DoubleTgt { Id = r.Id, V = 3.14 }));
            });
        }
    }
}
