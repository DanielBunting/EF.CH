using System.Text;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Engines;

public class KeeperMapIntegrationTests : IAsyncLifetime
{
    // KeeperMap requires an embedded Keeper and a configured path prefix.
    private const string KeeperMapConfig = """
        <clickhouse>
            <keeper_server>
                <tcp_port>9181</tcp_port>
                <server_id>1</server_id>
                <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
                <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
                <coordination_settings>
                    <operation_timeout_ms>10000</operation_timeout_ms>
                    <session_timeout_ms>30000</session_timeout_ms>
                </coordination_settings>
                <raft_configuration>
                    <server>
                        <id>1</id>
                        <hostname>localhost</hostname>
                        <port>9234</port>
                    </server>
                </raft_configuration>
            </keeper_server>
            <zookeeper>
                <node>
                    <host>localhost</host>
                    <port>9181</port>
                </node>
            </zookeeper>
            <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
        </clickhouse>
        """;

    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .WithResourceMapping(
            Encoding.UTF8.GetBytes(KeeperMapConfig),
            "/etc/clickhouse-server/config.d/keeper_map.xml")
        .Build();

    private readonly string _rootPath = $"/keeper_map_tables/test/{Guid.NewGuid():N}";

    public async Task InitializeAsync() => await _container.StartAsync();

    public async Task DisposeAsync() => await _container.DisposeAsync();

    private string GetConnectionString() => _container.GetConnectionString();

    [Fact]
    public async Task EnsureCreated_EmitsValidKeeperMapDdl()
    {
        await using var context = CreateContext();

        await context.Database.EnsureCreatedAsync();

        var engine = await context.Database
            .SqlQueryRaw<string>("SELECT engine AS Value FROM system.tables WHERE name = 'KvItems'")
            .FirstAsync();

        Assert.Equal("KeeperMap", engine);
    }

    [Fact]
    public async Task InsertAndQuery_RoundTrips()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        context.Items.Add(new KvItem { Key = "alpha", Value = "one" });
        context.Items.Add(new KvItem { Key = "beta", Value = "two" });
        await context.SaveChangesAsync();

        var alpha = await context.Items.SingleAsync(x => x.Key == "alpha");
        Assert.Equal("one", alpha.Value);

        var count = await context.Items.LongCountAsync();
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Upsert_ReplacesExistingKey()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        context.Items.Add(new KvItem { Key = "gamma", Value = "initial" });
        await context.SaveChangesAsync();

        // KeeperMap treats INSERT on an existing key as an atomic replace.
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""KvItems"" (""Key"", ""Value"") VALUES ('gamma', 'updated')");

        var reread = await context.Items
            .AsNoTracking()
            .SingleAsync(x => x.Key == "gamma");

        Assert.Equal("updated", reread.Value);
    }

    private KeeperMapTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<KeeperMapTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new KeeperMapTestContext(options, _rootPath);
    }
}

public class KeeperMapTestContext : DbContext
{
    private readonly string _rootPath;

    public KeeperMapTestContext(DbContextOptions<KeeperMapTestContext> options, string rootPath)
        : base(options)
    {
        _rootPath = rootPath;
    }

    public DbSet<KvItem> Items => Set<KvItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<KvItem>(entity =>
        {
            entity.ToTable("KvItems");
            entity.HasKey(e => e.Key);
            entity.UseKeeperMapEngine(_rootPath, x => x.Key);
        });
    }
}

public class KvItem
{
    public string Key { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
}
