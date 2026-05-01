using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// Runtime projection lifecycle: <c>AddProjectionAsync</c>, <c>MaterializeProjectionAsync</c>,
/// <c>DropProjectionAsync</c>. Existing fluent + migration coverage doesn't exercise the
/// runtime APIs directly.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public sealed class ProjectionLifecycleTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ProjectionLifecycleTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task MaterializeProjectionAsync_BackfillsProjectionParts()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        for (int i = 0; i < 100; i++)
            ctx.Events.Add(new Event { Id = (uint)i, Bucket = i % 5, Value = i * 10 });
        await ctx.SaveChangesAsync();

        await ctx.Database.AddProjectionAsync(
            tableName: "Events",
            projectionName: "p_by_bucket",
            selectSql: "SELECT Bucket, count(), sum(Value) GROUP BY Bucket");

        Assert.True(await ProjectionExistsAsync("Events", "p_by_bucket"));

        // Before MATERIALIZE, no projection_parts for existing rows.
        var partsBefore = await RawClickHouse.ProjectionPartsCountAsync(Conn, "Events", "p_by_bucket");

        await ctx.Database.MaterializeProjectionAsync("Events", "p_by_bucket");
        await RawClickHouse.WaitForMutationsAsync(Conn, "Events");

        var partsAfter = await RawClickHouse.ProjectionPartsCountAsync(Conn, "Events", "p_by_bucket");
        Assert.True(partsAfter > partsBefore);
    }

    [Fact]
    public async Task DropProjectionAsync_RemovesProjection()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        await ctx.Database.AddProjectionAsync(
            "Events", "p_droppable", "SELECT Bucket, count() GROUP BY Bucket");
        Assert.True(await ProjectionExistsAsync("Events", "p_droppable"));

        await ctx.Database.DropProjectionAsync("Events", "p_droppable");
        Assert.False(await ProjectionExistsAsync("Events", "p_droppable"));
    }

    [Fact]
    public async Task ProjectionLifecycle_AddAndDropAreIdempotent()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // ADD … IF NOT EXISTS — second call is a no-op, no throw.
        await ctx.Database.AddProjectionAsync("Events", "p_idem", "SELECT Bucket, count() GROUP BY Bucket");
        await ctx.Database.AddProjectionAsync("Events", "p_idem", "SELECT Bucket, count() GROUP BY Bucket");

        // DROP … IF EXISTS — same.
        await ctx.Database.DropProjectionAsync("Events", "p_idem");
        await ctx.Database.DropProjectionAsync("Events", "p_idem"); // already gone — no throw
    }

    private async Task<bool> ProjectionExistsAsync(string table, string projection)
    {
        // Query system.projections directly — engine_full doesn't always carry the PROJECTION clause.
        var n = await RawClickHouse.ScalarAsync<ulong>(Conn,
            $"SELECT count() FROM system.projections WHERE database = currentDatabase() AND table = '{table}' AND name = '{projection}'");
        return n > 0;
    }

    public sealed class Event
    {
        public uint Id { get; set; }
        public int Bucket { get; set; }
        public long Value { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Event> Events => Set<Event>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Event>(e =>
            {
                e.ToTable("Events");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
            });
        }
    }
}
