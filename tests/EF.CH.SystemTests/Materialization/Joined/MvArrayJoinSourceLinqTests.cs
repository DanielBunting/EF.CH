using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Joined;

/// <summary>
/// ARRAY JOIN / LEFT ARRAY JOIN in MV definitions (Phase K). Flattens an
/// array-typed column into one row per element. <c>LeftArrayJoin</c>
/// preserves rows whose array is empty by emitting one row with the element's
/// type-default; plain <c>ArrayJoin</c> drops empty-array rows.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvArrayJoinSourceLinqTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvArrayJoinSourceLinqTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task ArrayJoin_FlattensInnerToOneRowPerElement_DropsEmpty()
    {
        await using var ctx = TestContextFactory.Create<ArrayJoinCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Events.AddRange(
            new EvtRow { Id = 1, Tags = new[] { "a", "b", "c" } },
            new EvtRow { Id = 2, Tags = new[] { "d", "e" } },
            new EvtRow { Id = 3, Tags = Array.Empty<string>() }); // dropped by ArrayJoin
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "ArrayJoinTarget");
        // 3 + 2 + 0 = 5 flattened rows.
        Assert.Equal(5UL, await RawClickHouse.RowCountAsync(Conn, "ArrayJoinTarget"));
    }

    [Fact]
    public async Task LeftArrayJoin_PreservesEmptyArraysWithDefaultElement()
    {
        await using var ctx = TestContextFactory.Create<LeftArrayJoinCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Events.AddRange(
            new EvtRow { Id = 1, Tags = new[] { "a", "b" } },
            new EvtRow { Id = 2, Tags = Array.Empty<string>() }); // kept with one default-element row
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "LeftArrayJoinTarget");
        // 2 elements + 1 preserved-empty row = 3.
        Assert.Equal(3UL, await RawClickHouse.RowCountAsync(Conn, "LeftArrayJoinTarget"));
    }

    public sealed class EvtRow
    {
        public long Id { get; set; }
        public string[] Tags { get; set; } = Array.Empty<string>();
    }
    public sealed class FlatTag
    {
        public long EventId { get; set; }
        public string Tag { get; set; } = "";
    }

    public sealed class ArrayJoinCtx(DbContextOptions<ArrayJoinCtx> o) : DbContext(o)
    {
        public DbSet<EvtRow> Events => Set<EvtRow>();
        public DbSet<FlatTag> Target => Set<FlatTag>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<EvtRow>(e => { e.ToTable("ArrayJoinEvents"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<FlatTag>(e =>
            {
                e.ToTable("ArrayJoinTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.EventId);
                e.AsMaterializedView<FlatTag, EvtRow>(events => events
                    .ArrayJoin(x => x.Tags, (x, tag) => new FlatTag { EventId = x.Id, Tag = tag }));
            });
        }
    }

    public sealed class LeftArrayJoinCtx(DbContextOptions<LeftArrayJoinCtx> o) : DbContext(o)
    {
        public DbSet<EvtRow> Events => Set<EvtRow>();
        public DbSet<FlatTag> Target => Set<FlatTag>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<EvtRow>(e => { e.ToTable("LeftArrayJoinEvents"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<FlatTag>(e =>
            {
                e.ToTable("LeftArrayJoinTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.EventId);
                e.AsMaterializedView<FlatTag, EvtRow>(events => events
                    .LeftArrayJoin(x => x.Tags, (x, tag) => new FlatTag { EventId = x.Id, Tag = tag }));
            });
        }
    }
}
