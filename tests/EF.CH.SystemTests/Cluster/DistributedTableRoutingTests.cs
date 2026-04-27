using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Cluster;

[Collection(ShardedClusterCollection.Name)]
public class DistributedTableRoutingTests
{
    private readonly ShardedClusterFixture _fx;
    public DistributedTableRoutingTests(ShardedClusterFixture fx) => _fx = fx;

    [Fact]
    public async Task DistributedWrites_AreSharded_AndReadsReturnTheUnion()
    {
        await using var node1 = TestContextFactory.CreateWithCluster<Ctx>(_fx.Shard1ConnectionString, _fx.ClusterName);
        await node1.Database.EnsureDeletedAsync();
        await node1.Database.EnsureCreatedAsync();

        Assert.Contains("Distributed",
            await RawClickHouse.EngineFullAsync(_fx.Shard1ConnectionString, "Events"));
        Assert.Contains("MergeTree",
            await RawClickHouse.EngineFullAsync(_fx.Shard1ConnectionString, "EventsLocal"));

        const int totalRows = 900;
        var rng = new Random(17);
        node1.Events.AddRange(Enumerable.Range(1, totalRows).Select(i => new EventRow
        {
            Id = i,
            UserId = rng.Next(1, 10_000),
            Payload = "row-" + i,
        }));
        await node1.SaveChangesAsync();

        await Task.Delay(500); // Distributed-insert propagation.

        var perShardCounts = new List<ulong>();
        foreach (var conn in _fx.AllConnectionStrings)
            perShardCounts.Add(await RawClickHouse.RowCountAsync(conn, "EventsLocal"));

        Assert.Equal((ulong)totalRows, perShardCounts.Aggregate(0UL, (a, b) => a + b));
        Assert.All(perShardCounts, c => Assert.True(c > (ulong)(totalRows / 10),
            $"Shard received only {c} rows; full distribution was {string.Join(", ", perShardCounts)}"));

        Assert.Equal((ulong)totalRows,
            await RawClickHouse.RowCountAsync(_fx.Shard1ConnectionString, "Events"));
    }

    [Fact]
    public async Task DistributedAggregation_MatchesLocalAggregationAcrossShards()
    {
        await using var node1 = TestContextFactory.CreateWithCluster<Ctx>(_fx.Shard1ConnectionString, _fx.ClusterName);
        await node1.Database.EnsureDeletedAsync();
        await node1.Database.EnsureCreatedAsync();

        var rng = new Random(23);
        var rows = Enumerable.Range(1, 400).Select(i => new EventRow
        {
            Id = i, UserId = rng.Next(1, 500), Payload = "p",
        }).ToList();
        node1.Events.AddRange(rows);
        await node1.SaveChangesAsync();
        await Task.Delay(500);

        var distinctUsers = await RawClickHouse.ScalarAsync<ulong>(_fx.Shard1ConnectionString,
            "SELECT toUInt64(uniqExact(UserId)) FROM \"Events\"");
        Assert.Equal((ulong)rows.Select(r => r.UserId).Distinct().LongCount(), distinctUsers);
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<EventLocal> EventsLocal => Set<EventLocal>();
        public DbSet<EventRow> Events => Set<EventRow>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<EventLocal>(e =>
            {
                e.ToTable("EventsLocal"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });

            mb.Entity<EventRow>(e =>
            {
                e.ToTable("Events"); e.HasKey(x => x.Id);
                e.UseDistributed(ClusterConfigTemplates.ShardedClusterName, "EventsLocal")
                    .WithShardingKeyExpression("cityHash64(UserId)");
            });
        }
    }

    public class EventLocal { public long Id { get; set; } public long UserId { get; set; } public string Payload { get; set; } = ""; }
    public class EventRow { public long Id { get; set; } public long UserId { get; set; } public string Payload { get; set; } = ""; }
}
