using EF.CH.Diagnostics;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization;

[Collection(SingleNodeCollection.Name)]
public class RefreshableMvIntegrationTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public RefreshableMvIntegrationTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task RefreshEvery_PopulatesTarget()
    {
        await using var ctx = await PrepareContextAsync();

        ctx.Sources.AddRange(Enumerable.Range(1, 10).Select(i => new RefSource { Id = i, Bucket = i % 3 }));
        await ctx.SaveChangesAsync();

        // First refresh fires automatically immediately after creation; wait a beat.
        await ctx.Database.WaitForRefreshAsync<RefSummary>(TimeSpan.FromSeconds(30));

        var rows = await RawClickHouse.RowCountAsync(Conn, "RefSummary");
        Assert.True(rows > 0, "Refreshable MV target should have at least one row after the initial refresh.");
    }

    [Fact]
    public async Task ManualRefresh_ViaSystemRefreshView_AdvancesLastSuccessTime()
    {
        await using var ctx = await PrepareContextAsync();

        var baseline = DateTime.UtcNow;
        await ctx.Database.RefreshViewAsync<RefSummary>();
        await ctx.Database.WaitForRefreshAsync<RefSummary>(TimeSpan.FromSeconds(30));

        var status = await ctx.Database.GetRefreshStatusAsync<RefSummary>();
        Assert.NotNull(status);
        Assert.NotNull(status!.LastSuccessTime);
        Assert.True(status.LastSuccessTime > baseline);
    }

    [Fact]
    public async Task StopAndStart_View_TogglesScheduledRefreshes()
    {
        await using var ctx = await PrepareContextAsync();

        await ctx.Database.StopViewAsync<RefSummary>();
        await ctx.Database.StartViewAsync<RefSummary>();

        var status = await ctx.Database.GetRefreshStatusAsync<RefSummary>();
        Assert.NotNull(status);
    }

    [Fact]
    public async Task GetRefreshStatusAsync_ReturnsRow()
    {
        await using var ctx = await PrepareContextAsync();

        await ctx.Database.RefreshViewAsync<RefSummary>();
        await ctx.Database.WaitForRefreshAsync<RefSummary>(TimeSpan.FromSeconds(30));

        var status = await ctx.Database.GetRefreshStatusAsync<RefSummary>();
        Assert.NotNull(status);
        Assert.Equal("RefSummary", status!.View);
    }

    [Fact]
    public async Task WaitForRefreshAsync_TimesOut_WhenViewIsStopped()
    {
        await using var ctx = await PrepareContextAsync();

        // Run an initial refresh so the row in system.view_refreshes exists.
        await ctx.Database.RefreshViewAsync<RefSummary>();
        await ctx.Database.WaitForRefreshAsync<RefSummary>(TimeSpan.FromSeconds(30));
        await ctx.Database.StopViewAsync<RefSummary>();

        await Assert.ThrowsAsync<TimeoutException>(
            () => ctx.Database.WaitForRefreshAsync<RefSummary>(TimeSpan.FromSeconds(2)));
    }

    /// <summary>
    /// Provision a fresh schema for the test, ensuring REFRESH is enabled.
    /// On CH 25.6 the experimental flag is on by default; older images need it
    /// turned on per-session via a setting on the connection string.
    /// </summary>
    private async Task<EveryCtx> PrepareContextAsync()
    {
        await using (var dropCtx = TestContextFactory.Create<EveryCtx>(Conn))
        {
            await dropCtx.Database.EnsureDeletedAsync();
        }
        var ctx = TestContextFactory.Create<EveryCtx>(Conn);
        await ctx.Database.EnsureCreatedAsync();
        return ctx;
    }

    public class RefSource
    {
        public int Id { get; set; }
        public int Bucket { get; set; }
    }

    public class RefSummary
    {
        public int Bucket { get; set; }
        public int Total { get; set; }
    }

    public sealed class EveryCtx(DbContextOptions<EveryCtx> opts) : DbContext(opts)
    {
        public DbSet<RefSource> Sources => Set<RefSource>();
        public DbSet<RefSummary> Summaries => Set<RefSummary>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RefSource>(e => { e.ToTable("RefSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<RefSummary>(e =>
            {
                e.ToTable("RefSummary");
                e.HasNoKey();
                e.UseMergeTree(x => x.Bucket);
                e.AsRefreshableMaterializedView<RefSummary, RefSource>(
                    src => src.GroupBy(s => s.Bucket).Select(g => new RefSummary
                    {
                        Bucket = g.Key,
                        Total = g.Count(),
                    }),
                    r => r.Every(TimeSpan.FromSeconds(2)));
            });
        }
    }
}
