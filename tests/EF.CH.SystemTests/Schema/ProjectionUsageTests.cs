using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// Existing projection tests assert DDL emission and annotation state, not
/// that ClickHouse <em>actually selected</em> the projection at query time.
/// These tests close that gap by querying <c>system.query_log</c> after
/// running queries with and without a projection-shaped predicate, and
/// asserting the <c>projections</c> column reflects what the optimiser did.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class ProjectionUsageTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ProjectionUsageTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task QueryMatchingProjectionShape_QueryLogReportsProjectionUsed()
    {
        await using var ctx = await PrepareAsync();

        // Marker is included in the SELECT so we can locate this query in
        // system.query_log without ambiguity.
        const string marker = "/* projection_used_match */";
        var sql = $"SELECT {marker[3..^3].Length} AS marker_tag, Bucket, count() FROM \"Events\" GROUP BY Bucket";
        await ctx.Database.ExecuteSqlRawAsync($"{marker} {sql}");

        await SettleQueryLogAsync(ctx);

        var projections = await ScanProjectionsAsync(ctx, marker);
        Assert.Contains(projections, p => p.Contains("p_by_bucket"));
    }

    [Fact]
    public async Task QueryMissingProjectionFilter_QueryLogReportsBaseTable()
    {
        await using var ctx = await PrepareAsync();

        const string marker = "/* projection_used_miss */";
        // The projection groups by Bucket; selecting Value directly bypasses it.
        await ctx.Database.ExecuteSqlRawAsync($"{marker} SELECT Id, Value FROM \"Events\" WHERE Id < 5");

        await SettleQueryLogAsync(ctx);

        var projections = await ScanProjectionsAsync(ctx, marker);
        Assert.All(projections, p => Assert.Empty(p));
    }

    private async Task<Ctx> PrepareAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        for (int i = 0; i < 200; i++)
            ctx.Events.Add(new Event { Id = (uint)i, Bucket = i % 5, Value = i * 10 });
        await ctx.SaveChangesAsync();

        await ctx.Database.AddProjectionAsync(
            tableName: "Events",
            projectionName: "p_by_bucket",
            selectSql: "SELECT Bucket, count(), sum(Value) GROUP BY Bucket");
        await ctx.Database.MaterializeProjectionAsync("Events", "p_by_bucket");
        await RawClickHouse.WaitForMutationsAsync(Conn, "Events");
        return ctx;
    }

    private async Task SettleQueryLogAsync(Ctx ctx)
    {
        // Flush async query log to disk before scanning.
        await ctx.Database.ExecuteSqlRawAsync("SYSTEM FLUSH LOGS");
        await Task.Delay(500);
    }

    private async Task<List<string>> ScanProjectionsAsync(Ctx ctx, string marker)
    {
        // The `projections` column lists names of projections selected by
        // the optimiser for the query.
        var rows = await RawClickHouse.RowsAsync(Conn, $"""
            SELECT arrayStringConcat(projections, ',') AS projs
            FROM system.query_log
            WHERE type = 'QueryFinish'
              AND query LIKE '%{marker.Replace("'", "''")}%'
              AND query NOT LIKE '%system.query_log%'
            """);
        return rows.Select(r => (string)r["projs"]!).ToList();
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
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Event>(e =>
            {
                e.ToTable("Events"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
            });
    }
}
