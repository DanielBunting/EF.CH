using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Cluster;

[Collection(ReplicatedClusterCollection.Name)]
public class ReplicatedMergeTreeReplicationTests
{
    private readonly ReplicatedClusterFixture _fx;
    public ReplicatedMergeTreeReplicationTests(ReplicatedClusterFixture fx) => _fx = fx;

    [Fact]
    public async Task RowsInsertedOnOneReplica_AppearOnAllReplicasWithoutDelay()
    {
        await using var node1 = TestContextFactory.CreateWithCluster<Ctx>(_fx.Node1ConnectionString, _fx.ClusterName);
        await node1.Database.EnsureDeletedAsync();
        await node1.Database.EnsureCreatedAsync();

        Assert.Contains("ReplicatedMergeTree",
            await RawClickHouse.EngineFullAsync(_fx.Node1ConnectionString, "Messages"));

        // EF handles the inserts.
        node1.Messages.AddRange(Enumerable.Range(1, 50).Select(i => new Message
        {
            Id = i,
            Author = "author-" + (i % 5),
            Body = "message-" + i,
        }));
        await node1.SaveChangesAsync();

        // Assert via raw driver on each replica.
        foreach (var conn in _fx.AllConnectionStrings)
        {
            await RawClickHouse.WaitForReplicationAsync(conn, "Messages");
            Assert.Equal(50UL, await RawClickHouse.RowCountAsync(conn, "Messages"));

            var replicaRows = await RawClickHouse.RowsAsync(conn,
                "SELECT toUInt64(absolute_delay) AS delay, toUInt64(queue_size) AS q " +
                "FROM system.replicas WHERE database = currentDatabase() AND table = 'Messages'");
            Assert.NotEmpty(replicaRows);
            Assert.All(replicaRows, r => Assert.Equal(0UL, Convert.ToUInt64(r["delay"])));
            Assert.All(replicaRows, r => Assert.Equal(0UL, Convert.ToUInt64(r["q"])));
        }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Message> Messages => Set<Message>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Message>(e =>
            {
                e.ToTable("Messages"); e.HasKey(x => x.Id);
                e.UseReplicatedMergeTree(x => x.Id)
                    .WithReplication("/clickhouse/tables/{uuid}");
            });
    }

    public class Message
    {
        public long Id { get; set; }
        public string Author { get; set; } = "";
        public string Body { get; set; } = "";
    }
}
