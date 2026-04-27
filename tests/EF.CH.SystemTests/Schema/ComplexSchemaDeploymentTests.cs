using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EF.CH.Metadata;

namespace EF.CH.SystemTests.Schema;

[Collection(SingleNodeCollection.Name)]
public class ComplexSchemaDeploymentTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public ComplexSchemaDeploymentTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task ComplexSchema_Deploys_AndMatchesDeclaredTypes()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "Events"));
        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "Users"));

        // Inspect column types directly from system.columns.
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'Events' ORDER BY position");
        var cols = rows.ToDictionary(r => (string)r["name"]!, r => (string)r["type"]!);
        Assert.StartsWith("Array", cols["Tags"]);
        Assert.Contains("LowCardinality", cols["Category"]);

        var engine = await RawClickHouse.EngineFullAsync(Conn, "Events");
        Assert.Contains("TTL", engine);
    }

    [Fact]
    public async Task ComplexSchema_AcceptsEfInsertsAndRoundtripsValues()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Events.Add(new EventEntity
        {
            Id = Guid.NewGuid(),
            EventTime = DateTime.UtcNow,
            Category = "clicks",
            Tags = new[] { "web", "eu" },
            Payload = "hello",
        });
        await ctx.SaveChangesAsync();

        var row = (await RawClickHouse.RowsAsync(Conn,
            "SELECT Category, Tags FROM \"Events\" LIMIT 1")).Single();
        Assert.Equal("clicks", (string)row["Category"]!);
        var tags = (string[])row["Tags"]!;
        Assert.Contains("web", tags);
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<EventEntity> Events => Set<EventEntity>();
        public DbSet<UserEntity> Users => Set<UserEntity>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<EventEntity>(e =>
            {
                e.ToTable("Events"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.EventTime, x.Id });
                e.HasPartitionBy(x => x.EventTime, PartitionGranularity.Month);
                e.Property(x => x.Category).HasColumnType("LowCardinality(String)");
                e.Property(x => x.Tags).HasColumnType("Array(String)");
                e.Property(x => x.Payload).HasCodec(c => c.ZSTD(3));
                e.HasTtl("toDateTime(EventTime) + INTERVAL 6 MONTH");
            });

            mb.Entity<UserEntity>(e =>
            {
                e.ToTable("Users"); e.HasKey(x => x.Id);
                e.UseReplacingMergeTree("Version", "Id");
            });
        }
    }

    public class EventEntity
    {
        public Guid Id { get; set; }
        public DateTime EventTime { get; set; }
        public string Category { get; set; } = "";
        public string[] Tags { get; set; } = Array.Empty<string>();
        public string Payload { get; set; } = "";
    }

    public class UserEntity
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
        public long Version { get; set; }
    }
}
