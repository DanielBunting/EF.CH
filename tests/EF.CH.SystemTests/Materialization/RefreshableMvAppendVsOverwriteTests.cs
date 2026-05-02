using EF.CH.Diagnostics;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization;

/// <summary>
/// Pins the refresh semantics for refreshable MVs: by default each refresh
/// fully overwrites the target; with <c>.Append()</c> each refresh appends to
/// it. <c>Append()</c> and <c>Empty()</c> are mutually exclusive
/// (see <c>MaterializedViewBuilders.cs:351-366</c>) but no test today asserts
/// the resulting on-disk row counts after multiple refreshes.
///
/// The tests use a short refresh interval and a stable source population so
/// timing is robust: with overwrite the target row count equals the source
/// row count regardless of how many refreshes ran; with append the target
/// row count strictly exceeds the source row count after at least two
/// refreshes have completed.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class RefreshableMvAppendVsOverwriteTests
{
    private const int SourceRowCount = 3;
    // Each refresh fires every 1 second; this delay covers ≥ 3 refreshes so
    // append accumulates a count strictly greater than SourceRowCount.
    private static readonly TimeSpan RefreshSettlingDelay = TimeSpan.FromSeconds(5);

    private readonly SingleNodeClickHouseFixture _fixture;
    public RefreshableMvAppendVsOverwriteTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task RefreshableMv_FullOverwrite_ReplacesPriorRows()
    {
        await using var ctx = await PrepareContextAsync<OverwriteCtx>();

        ctx.Sources.AddRange(MakeRows(start: 1, count: SourceRowCount));
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        // Wait long enough for multiple refreshes; the target count must
        // settle at the SOURCE row count (not a multiple of it). Default
        // refresh REPLACES the target on each refresh.
        await Task.Delay(RefreshSettlingDelay);

        var target = await RawClickHouse.RowCountAsync(Conn, "OverwriteRow");
        Assert.Equal((ulong)SourceRowCount, target);
    }

    [Fact]
    public async Task RefreshableMv_Append_PreservesPriorRows()
    {
        await using var ctx = await PrepareContextAsync<AppendCtx>();

        ctx.Sources.AddRange(MakeRows(start: 1, count: SourceRowCount));
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        // Wait long enough for multiple refreshes; .Append() makes each
        // refresh add the current SELECT result to the target, so total
        // accumulates beyond the source row count.
        await Task.Delay(RefreshSettlingDelay);

        var target = await RawClickHouse.RowCountAsync(Conn, "AppendRow");
        Assert.True(target > (ulong)SourceRowCount,
            $"with .Append(), target should exceed source ({SourceRowCount}) after multiple refreshes; got {target}");
        // Each refresh appends a full snapshot, so the count is always a
        // multiple of the source row count.
        Assert.Equal(0UL, target % (ulong)SourceRowCount);
    }

    private static IEnumerable<RefreshSource> MakeRows(int start, int count) =>
        Enumerable.Range(start, count).Select(i => new RefreshSource { Id = i, Bucket = i });

    private async Task<TCtx> PrepareContextAsync<TCtx>() where TCtx : DbContext
    {
        await using (var dropCtx = TestContextFactory.Create<TCtx>(Conn))
        {
            await dropCtx.Database.EnsureDeletedAsync();
        }
        var ctx = TestContextFactory.Create<TCtx>(Conn);
        await ctx.Database.EnsureCreatedAsync();
        return ctx;
    }

    public class RefreshSource
    {
        public int Id { get; set; }
        public int Bucket { get; set; }
    }

    public class OverwriteRow
    {
        public int Id { get; set; }
        public int Bucket { get; set; }
    }

    public class AppendRow
    {
        public int Id { get; set; }
        public int Bucket { get; set; }
    }

    public sealed class OverwriteCtx(DbContextOptions<OverwriteCtx> opts) : DbContext(opts)
    {
        public DbSet<RefreshSource> Sources => Set<RefreshSource>();
        public DbSet<OverwriteRow> Targets => Set<OverwriteRow>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RefreshSource>(e => { e.ToTable("OverwriteSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<OverwriteRow>(e =>
            {
                e.ToTable("OverwriteRow"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
            });
            mb.MaterializedView<OverwriteRow>().From<RefreshSource>().DefinedAs(src =>
                src.Select(s => new OverwriteRow { Id = s.Id, Bucket = s.Bucket }))
                .RefreshEvery(TimeSpan.FromSeconds(1));
        }
    }

    public sealed class AppendCtx(DbContextOptions<AppendCtx> opts) : DbContext(opts)
    {
        public DbSet<RefreshSource> Sources => Set<RefreshSource>();
        public DbSet<AppendRow> Targets => Set<AppendRow>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RefreshSource>(e => { e.ToTable("AppendSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<AppendRow>(e =>
            {
                e.ToTable("AppendRow"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
            });
            mb.MaterializedView<AppendRow>().From<RefreshSource>().DefinedAs(src =>
                src.Select(s => new AppendRow { Id = s.Id, Bucket = s.Bucket }))
                .RefreshEvery(TimeSpan.FromSeconds(1))
                .Append();
        }
    }
}
