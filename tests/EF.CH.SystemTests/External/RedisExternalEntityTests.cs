using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using StackExchange.Redis;
using Xunit;

namespace EF.CH.SystemTests.External;

/// <summary>
/// Integration tests for the <c>ExternalRedisEntity</c> mapping. ClickHouse queries Redis
/// through the <c>redis()</c> table function. Smaller scope than other external surfaces
/// because Redis support has shown platform fragility in past tests.
/// </summary>
[Collection(RedisCollection.Name)]
public sealed class RedisExternalEntityTests
{
    private readonly RedisFixture _fx;
    public RedisExternalEntityTests(RedisFixture fx) => _fx = fx;

    [Fact]
    public async Task ExternalRedisEntity_QueryGeneration_IncludesRedisFunction()
    {
        // Live query against ClickHouse's redis() table function expects RowBinary-encoded
        // values, which StackExchange.Redis (and the standard Redis CLI) doesn't write —
        // ClickHouse fails with CANNOT_READ_ALL_DATA on plain string values. The existing
        // EF.CH.Tests suite skips the live-query path for the same reason. This test pins
        // SQL generation: configuration → translated query references the redis() function
        // with the configured host/port. The full read-side round-trip needs a Redis writer
        // that emits RowBinary, which is out of scope here.
        await using var ctx = CreateContext();

        var sql = ctx.RedisLookups.ToQueryString();

        Assert.Contains("redis(", sql);
        Assert.Contains(_fx.RedisHostAlias, sql);

        // Smoke-check that the Redis container is reachable (so the fixture is healthy).
        var redis = await ConnectionMultiplexer.ConnectAsync(_fx.RedisConnectionString);
        var db = redis.GetDatabase();
        await db.StringSetAsync("smoke", "ok");
        var pong = await db.StringGetAsync("smoke");
        Assert.Equal("ok", (string?)pong);
    }

    private RedisExtCtx CreateContext()
    {
        Environment.SetEnvironmentVariable("REDIS_HOST", $"{_fx.RedisHostAlias}:{_fx.RedisInternalPort}");

        var options = new DbContextOptionsBuilder<RedisExtCtx>()
            .UseClickHouse(_fx.ClickHouseConnectionString)
            .Options;
        return new RedisExtCtx(options);
    }

    public sealed class RedisLookup
    {
        public string key { get; set; } = string.Empty;
        public string value { get; set; } = string.Empty;
    }

    public sealed class RedisExtCtx(DbContextOptions<RedisExtCtx> o) : DbContext(o)
    {
        public DbSet<RedisLookup> RedisLookups => Set<RedisLookup>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.ExternalRedisEntity<RedisLookup>(ext => ext
                .KeyColumn(r => r.key)
                .Connection(c => c
                    .HostPort(env: "REDIS_HOST")
                    .DbIndex(0)));
        }
    }
}
