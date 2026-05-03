using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Features;

/// <summary>
/// Integration tests for ClickHouse table functions in
/// <see cref="ClickHouseTableFunctionExtensions"/>: <c>FromUrl</c>, <c>FromCluster</c>,
/// <c>FromRemote</c>, <c>FromFile</c>, <c>FromS3</c>. Each requires a registered EF entity
/// type so the structure can be inferred. The cluster variants live in
/// <see cref="TableFunctionClusterTests"/> below.
/// </summary>
[Collection(HttpJsonEachRowCollection.Name)]
public sealed class TableFunctionUrlTests
{
    private readonly HttpJsonEachRowFixture _fx;
    public TableFunctionUrlTests(HttpJsonEachRowFixture fx) => _fx = fx;

    [Fact]
    public async Task FromUrl_QueriesJsonEachRow()
    {
        await using var ctx = TestContextFactory.Create<UrlCtx>(_fx.ClickHouseConnectionString);

        var rows = await ctx.FromUrl<RemoteRow>(_fx.InternalUrl, "JSONEachRow")
            .OrderBy(r => r.id)
            .ToListAsync();

        Assert.Equal(3, rows.Count);
        Assert.Equal("alpha", rows[0].name);
        Assert.Equal("bravo", rows[1].name);
        Assert.Equal("charlie", rows[2].name);
    }

    [Fact]
    public async Task FromUrl_ComposesWithLinqWhere()
    {
        await using var ctx = TestContextFactory.Create<UrlCtx>(_fx.ClickHouseConnectionString);

        var rows = await ctx.FromUrl<RemoteRow>(_fx.InternalUrl, "JSONEachRow")
            .Where(r => r.score > 15.0)
            .OrderBy(r => r.id)
            .ToListAsync();

        // alpha=10.5, bravo=20.25, charlie=30.0 → bravo + charlie
        Assert.Equal(2, rows.Count);
    }

    /// <summary>
    /// Property names are lowercase to match the JSON keys in the payload — InferStructure
    /// emits column names from the entity property names verbatim, and ClickHouse JSONEachRow
    /// matches them case-sensitively against the JSON.
    /// </summary>
    public sealed class RemoteRow
    {
        public uint id { get; set; }
        public string name { get; set; } = string.Empty;
        public double score { get; set; }
    }

    public sealed class UrlCtx(DbContextOptions<UrlCtx> o) : DbContext(o)
    {
        public DbSet<RemoteRow> Remote => Set<RemoteRow>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            // The entity must be in the model for FromUrl to infer the structure. HasNoKey
            // because table-function results are keyless.
            mb.Entity<RemoteRow>(e =>
            {
                e.ToTable("remote_rows");
                e.HasNoKey();
            });
        }
    }

    /// <summary>
    /// URL-embedded credentials in <c>FromUrl</c> must not leak into exception
    /// messages when the request fails. The audit calls this out as the
    /// canonical credential-leakage hazard for table functions: a malformed
    /// or unreachable URL with <c>user:secret@host</c> shouldn't surface the
    /// secret in a thrown server exception, log line, or stack trace.
    /// </summary>
    [Fact]
    public async Task FromUrl_WithCredentialsInUrl_ExceptionMessageDoesNotContainCredentials()
    {
        await using var ctx = TestContextFactory.Create<UrlCtx>(_fx.ClickHouseConnectionString);

        // RFC 3986 userinfo-with-secret pattern, pointing at an unreachable
        // host so the request is guaranteed to fail.
        const string secret = "supersecretvalue";
        var malformedUrl = $"http://user:{secret}@unreachable.invalid:65535/data";

        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await ctx.FromUrl<RemoteRow>(malformedUrl, "JSONEachRow").ToListAsync();
        });

        Assert.DoesNotContain(secret, ex.ToString(),
            StringComparison.Ordinal);
    }
}

/// <summary>Cluster table-function tests against the existing sharded cluster fixture.</summary>
[Collection(ShardedClusterCollection.Name)]
public sealed class TableFunctionClusterTests
{
    private readonly ShardedClusterFixture _fx;
    public TableFunctionClusterTests(ShardedClusterFixture fx) => _fx = fx;

    [Fact]
    public async Task FromCluster_QueriesShardedTable_UnionsAcrossNodes()
    {
        var clusterName = _fx.ClusterName;

        // Seed each node with distinguishable rows. The cluster() table function will UNION
        // results across all shards.
        for (int i = 0; i < _fx.AllConnectionStrings.Count; i++)
        {
            var conn = _fx.AllConnectionStrings[i];
            await RawClickHouse.ExecuteAsync(conn, "DROP TABLE IF EXISTS cluster_probe SYNC");
            await RawClickHouse.ExecuteAsync(conn,
                "CREATE TABLE cluster_probe (id UInt32, source String) ENGINE = MergeTree() ORDER BY id");
            await RawClickHouse.ExecuteAsync(conn,
                $"INSERT INTO cluster_probe VALUES ({i + 1}, 'node{i + 1}')");
        }

        await using var ctx = TestContextFactory.Create<ClusterCtx>(_fx.Shard1ConnectionString);

        var rows = await ctx.FromCluster<ClusterProbe>(clusterName, "default", "cluster_probe")
            .OrderBy(r => r.id)
            .ToListAsync();

        Assert.Equal(3, rows.Count);
        Assert.Equal("node1", rows[0].source);
        Assert.Equal("node2", rows[1].source);
        Assert.Equal("node3", rows[2].source);
    }

    [Fact]
    public async Task FromRemote_QueriesAnotherShardByHostname()
    {
        // Seed only on shard 2.
        await RawClickHouse.ExecuteAsync(_fx.Shard2ConnectionString,
            "DROP TABLE IF EXISTS remote_probe SYNC");
        await RawClickHouse.ExecuteAsync(_fx.Shard2ConnectionString,
            "CREATE TABLE remote_probe (id UInt32, label String) ENGINE = MergeTree() ORDER BY id");
        await RawClickHouse.ExecuteAsync(_fx.Shard2ConnectionString,
            "INSERT INTO remote_probe VALUES (1, 'shard2-only'), (2, 'also-shard2')");

        // Query from shard 1 via remote() to shard 2's hostname (use the inter-container alias).
        await using var ctx = TestContextFactory.Create<ClusterCtx>(_fx.Shard1ConnectionString);

        var rows = await ctx.FromRemote<RemoteProbe>(
                addresses: _fx.ShardHostnames[1] + ":9000",
                database: "default",
                table: "remote_probe",
                user: "clickhouse",
                password: "clickhouse")
            .OrderBy(r => r.id)
            .ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal("shard2-only", rows[0].label);
    }

    /// <summary>
    /// Lowercase property names match the source columns (created via raw CREATE TABLE
    /// with snake_case identifiers). FromCluster/FromRemote use FromSqlRaw under the hood;
    /// EF emits SELECT with the C# property names verbatim, so they must match the source.
    /// </summary>
    public sealed class ClusterProbe
    {
        public uint id { get; set; }
        public string source { get; set; } = string.Empty;
    }

    public sealed class RemoteProbe
    {
        public uint id { get; set; }
        public string label { get; set; } = string.Empty;
    }

    public sealed class ClusterCtx(DbContextOptions<ClusterCtx> o) : DbContext(o)
    {
        public DbSet<ClusterProbe> ClusterProbes => Set<ClusterProbe>();
        public DbSet<RemoteProbe> RemoteProbes => Set<RemoteProbe>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<ClusterProbe>(e =>
            {
                e.ToTable("cluster_probe");
                e.HasNoKey();
            });
            mb.Entity<RemoteProbe>(e =>
            {
                e.ToTable("remote_probe");
                e.HasNoKey();
            });
        }
    }
}

/// <summary>file() table function tests using the ClickHouse user_files dir.</summary>
[Collection(SingleNodeCollection.Name)]
public sealed class TableFunctionFileTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public TableFunctionFileTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task FromFile_QueriesCsvFromUserFiles()
    {
        // Place a CSV directly in the ClickHouse server's user_files/ via INSERT INTO FUNCTION file().
        // This is the standard way to put files into the server-readable area without needing
        // shell access to the container.
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO FUNCTION file('local_data.csv', 'CSV', 'id UInt32, label String') VALUES (1, 'one'), (2, 'two')");

        await using var ctx = TestContextFactory.Create<FileCtx>(Conn);

        var rows = await ctx.FromFile<LocalRow>("local_data.csv", "CSV")
            .OrderBy(r => r.Id)
            .ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal("one", rows[0].Label);

        // Cleanup: there's no DROP for files, but the next test run uses a different container.
    }

    public sealed class LocalRow
    {
        public uint Id { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    public sealed class FileCtx(DbContextOptions<FileCtx> o) : DbContext(o)
    {
        public DbSet<LocalRow> Rows => Set<LocalRow>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<LocalRow>(e =>
            {
                e.ToTable("local_row");
                e.HasNoKey();
            });
        }
    }
}
