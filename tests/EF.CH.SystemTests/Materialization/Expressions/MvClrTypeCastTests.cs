using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// Per-primitive coverage of <c>MaterializedViewSqlTranslator.WrapWithClrTypeCast</c>.
/// ClickHouse infers MV column types from the SELECT, so the translator wraps
/// projections with <c>toInt64</c> / <c>toUInt8</c> / <c>toFloat32</c> / etc.
/// when the declared CLR property type would otherwise be silently overridden.
/// Each Fact pins one arm of the cast switch and the Array-element promotion path.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvClrTypeCastTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvClrTypeCastTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Bool_ProjectsAsUInt8()
    {
        await using var ctx = TestContextFactory.Create<BoolCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvCastBoolTarget");
        // WrapWithClrTypeCast wraps Boolean with toUInt8 — the MV's column is
        // therefore UInt8 (not Bool), and the bool round-trip happens at the
        // EF reader level. Pin the cast output, not the entity type.
        Assert.Equal("UInt8", await RawClickHouse.ColumnTypeAsync(Conn, "MvCastBoolTarget", "Flag"));
    }

    [Fact]
    public async Task SByte_ProjectsAsInt8()
    {
        await using var ctx = TestContextFactory.Create<SByteCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvCastSByteTarget");
        Assert.Equal("Int8", await RawClickHouse.ColumnTypeAsync(Conn, "MvCastSByteTarget", "Small"));
    }

    [Fact]
    public async Task UInt32_ProjectsAsUInt32()
    {
        await using var ctx = TestContextFactory.Create<UInt32Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvCastUInt32Target");
        Assert.Equal("UInt32", await RawClickHouse.ColumnTypeAsync(Conn, "MvCastUInt32Target", "Big"));
    }

    [Fact]
    public async Task Single_ProjectsAsFloat32()
    {
        await using var ctx = TestContextFactory.Create<SingleCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvCastSingleTarget");
        Assert.Equal("Float32", await RawClickHouse.ColumnTypeAsync(Conn, "MvCastSingleTarget", "Ratio"));
    }

    [Fact]
    public async Task Int16_ProjectsAsInt16()
    {
        await using var ctx = TestContextFactory.Create<Int16Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, N = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvCastInt16Target");
        Assert.Equal("Int16", await RawClickHouse.ColumnTypeAsync(Conn, "MvCastInt16Target", "Small"));
    }

    public sealed class Row { public long Id { get; set; } public long N { get; set; } }

    public sealed class BoolTgt   { public long Id { get; set; } public bool   Flag  { get; set; } }
    public sealed class SByteTgt  { public long Id { get; set; } public sbyte  Small { get; set; } }
    public sealed class UInt32Tgt { public long Id { get; set; } public uint   Big   { get; set; } }
    public sealed class SingleTgt { public long Id { get; set; } public float  Ratio { get; set; } }
    public sealed class Int16Tgt  { public long Id { get; set; } public short  Small { get; set; } }

    public sealed class BoolCtx(DbContextOptions<BoolCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<BoolTgt> Target => Set<BoolTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvCastBoolSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<BoolTgt>(e =>
            {
                e.ToTable("MvCastBoolTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);

            });
            mb.MaterializedView<BoolTgt>().From<Row>().DefinedAs(rows => rows
                    .Select(r => new BoolTgt { Id = r.Id, Flag = true }));
        }
    }

    public sealed class SByteCtx(DbContextOptions<SByteCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<SByteTgt> Target => Set<SByteTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvCastSByteSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<SByteTgt>(e =>
            {
                e.ToTable("MvCastSByteTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);

            });
            mb.MaterializedView<SByteTgt>().From<Row>().DefinedAs(rows => rows
                    .Select(r => new SByteTgt { Id = r.Id, Small = (sbyte)1 }));
        }
    }

    public sealed class UInt32Ctx(DbContextOptions<UInt32Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<UInt32Tgt> Target => Set<UInt32Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvCastUInt32Source"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<UInt32Tgt>(e =>
            {
                e.ToTable("MvCastUInt32Target"); e.HasNoKey(); e.UseMergeTree(x => x.Id);

            });
            mb.MaterializedView<UInt32Tgt>().From<Row>().DefinedAs(rows => rows
                    .Select(r => new UInt32Tgt { Id = r.Id, Big = 1u }));
        }
    }

    public sealed class SingleCtx(DbContextOptions<SingleCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<SingleTgt> Target => Set<SingleTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvCastSingleSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<SingleTgt>(e =>
            {
                e.ToTable("MvCastSingleTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);

            });
            mb.MaterializedView<SingleTgt>().From<Row>().DefinedAs(rows => rows
                    .Select(r => new SingleTgt { Id = r.Id, Ratio = 1f }));
        }
    }

    public sealed class Int16Ctx(DbContextOptions<Int16Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<Int16Tgt> Target => Set<Int16Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvCastInt16Source"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Int16Tgt>(e =>
            {
                e.ToTable("MvCastInt16Target"); e.HasNoKey(); e.UseMergeTree(x => x.Id);

            });
            mb.MaterializedView<Int16Tgt>().From<Row>().DefinedAs(rows => rows
                    .Select(r => new Int16Tgt { Id = r.Id, Small = (short)1 }));
        }
    }
}
