using EF.CH.Dictionaries;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Xunit;

namespace EF.CH.SystemTests.External;

/// <summary>
/// Coverage of the FLAT, HASHED, and CACHE dictionary layouts via the fluent
/// builder. The existing dictionary tests pin only the HASHED layout (the
/// default); a regression in the layout-rendering path of
/// <c>DictionaryConfigResolver.GetLayoutSql</c> would silently revert all
/// dictionaries to HASHED and not be caught by current coverage. Each
/// layout uses its own DbContext type so EF's per-context model cache
/// doesn't share the dictionary metadata between cases.
/// </summary>
[Collection(PostgresCollection.Name)]
public sealed class DictionaryLayoutTests
{
    private readonly PostgresFixture _fx;
    public DictionaryLayoutTests(PostgresFixture fx) => _fx = fx;
    private string Conn => _fx.ClickHouseConnectionString;

    [Fact]
    public async Task FlatLayout_RendersAndQueries()
    {
        await SeedPostgresAsync((1, "alpha"), (2, "beta"));
        await RawClickHouse.ExecuteAsync(Conn, "DROP DICTIONARY IF EXISTS \"layout_flat\"");

        var opts = new DbContextOptionsBuilder<FlatCtx>().UseClickHouse(Conn).Options;
        await using var ctx = new FlatCtx(opts, _fx.PostgresHostAlias, _fx.PostgresUser, _fx.PostgresPassword, _fx.PostgresDatabase);
        await ctx.EnsureDictionariesAsync();
        await ctx.ReloadDictionaryAsync<FlatLookup>();

        Assert.Equal("Flat", await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT type FROM system.dictionaries WHERE database = currentDatabase() AND name = 'layout_flat'"));

        var dict = new ClickHouseDictionary<FlatLookup, ulong>(ctx);
        Assert.Equal("alpha", await dict.GetAsync(1uL, x => x.Name));
        Assert.Equal("beta", await dict.GetAsync(2uL, x => x.Name));
    }

    [Fact]
    public async Task CacheLayout_RendersSizeAndQueries()
    {
        await SeedPostgresAsync((1, "x"), (2, "y"));
        await RawClickHouse.ExecuteAsync(Conn, "DROP DICTIONARY IF EXISTS \"layout_cache\"");

        var opts = new DbContextOptionsBuilder<CacheCtx>().UseClickHouse(Conn).Options;
        await using var ctx = new CacheCtx(opts, _fx.PostgresHostAlias, _fx.PostgresUser, _fx.PostgresPassword, _fx.PostgresDatabase);
        await ctx.EnsureDictionariesAsync();
        await ctx.ReloadDictionaryAsync<CacheLookup>();

        Assert.Equal("Cache", await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT type FROM system.dictionaries WHERE database = currentDatabase() AND name = 'layout_cache'"));

        var ddl = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT create_table_query FROM system.tables WHERE name = 'layout_cache' AND database = currentDatabase()");
        Assert.Contains("1024", ddl);

        var dict = new ClickHouseDictionary<CacheLookup, ulong>(ctx);
        Assert.Equal("x", await dict.GetAsync(1uL, x => x.Name));
    }

    private async Task SeedPostgresAsync(params (long id, string name)[] rows)
    {
        await using var conn = new NpgsqlConnection(_fx.PostgresConnectionString);
        await conn.OpenAsync();
        await Exec(conn, "DROP TABLE IF EXISTS layout_dict_src");
        await Exec(conn, @"
            CREATE TABLE layout_dict_src (
                ""Id"" BIGINT PRIMARY KEY,
                ""Name"" TEXT NOT NULL
            )");
        foreach (var r in rows)
        {
            await using var cmd = new NpgsqlCommand(
                "INSERT INTO layout_dict_src (\"Id\", \"Name\") VALUES (@i, @n)", conn);
            cmd.Parameters.AddWithValue("i", r.id);
            cmd.Parameters.AddWithValue("n", r.name);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static async Task Exec(NpgsqlConnection conn, string sql)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    public sealed class FlatLookup : IClickHouseDictionary
    {
        public ulong Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
    public sealed class CacheLookup : IClickHouseDictionary
    {
        public ulong Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public sealed class FlatCtx(DbContextOptions<FlatCtx> o, string pgHost, string pgUser, string pgPwd, string pgDb) : DbContext(o)
    {
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<FlatLookup>(e =>
                e.AsDictionary<FlatLookup>(cfg => cfg
                    .HasName("layout_flat")
                    .HasKey(x => x.Id)
                    .FromPostgreSql(pg => pg
                        .FromTable("layout_dict_src")
                        .Connection(c => c
                            .Host(value: pgHost).Port(value: 5432).Database(value: pgDb)
                            .User(value: pgUser).Password(value: pgPwd)))
                    .UseFlatLayout()
                    .HasLifetime(minSeconds: 0, maxSeconds: 1)));
    }

    public sealed class CacheCtx(DbContextOptions<CacheCtx> o, string pgHost, string pgUser, string pgPwd, string pgDb) : DbContext(o)
    {
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<CacheLookup>(e =>
                e.AsDictionary<CacheLookup>(cfg => cfg
                    .HasName("layout_cache")
                    .HasKey(x => x.Id)
                    .FromPostgreSql(pg => pg
                        .FromTable("layout_dict_src")
                        .Connection(c => c
                            .Host(value: pgHost).Port(value: 5432).Database(value: pgDb)
                            .User(value: pgUser).Password(value: pgPwd)))
                    .UseCacheLayout(o => o.SizeInCells = 1024)
                    .HasLifetime(minSeconds: 0, maxSeconds: 1)));
    }
}
