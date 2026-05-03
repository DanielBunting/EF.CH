using EF.CH.Dictionaries;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Xunit;

namespace EF.CH.SystemTests.External;

/// <summary>
/// Live <c>dictGet</c> integration tests against a Postgres-source dictionary. Uses
/// <see cref="PostgresFixture"/> to co-locate ClickHouse with Postgres on a private Docker
/// network so ClickHouse can reach Postgres by container hostname (sidesteps the loopback
/// auth problem that blocks ClickHouse-source dictGet in
/// <see cref="EF.CH.SystemTests.Migrations.MigrateDictionariesTests"/>).
/// </summary>
[Collection(PostgresCollection.Name)]
public sealed class PostgresDictionaryTests
{
    private readonly PostgresFixture _fx;
    public PostgresDictionaryTests(PostgresFixture fx) => _fx = fx;
    private string Conn => _fx.ClickHouseConnectionString;

    [Fact]
    public async Task EnsureDictionariesAsync_PostgresSource_CreatesAndQueries()
    {
        await SeedPostgresAsync(
            (1, "United States", "US"),
            (2, "United Kingdom", "GB"),
            (3, "Germany", "DE"));
        await ResetClickHouseAsync();

        await using var ctx = CreateContext();

        var created = await ctx.EnsureDictionariesAsync();
        Assert.Equal(1, created);

        // Status may be NOT_LOADED until first lookup forces a load — explicit reload primes it.
        await ctx.ReloadDictionaryAsync<CountryLookup>();

        var dict = new ClickHouseDictionary<CountryLookup, ulong>(ctx);
        Assert.Equal("United States", await dict.GetAsync(1uL, c => c.Name));
        Assert.Equal("Germany", await dict.GetAsync(3uL, c => c.Name));
        Assert.Equal("LOADED", await RawClickHouse.DictionaryStatusAsync(Conn, "country_lookup"));
    }

    [Fact]
    public async Task DictionaryLookup_DictHasAndDictGet_ReturnExpectedValues()
    {
        await SeedPostgresAsync(
            (10, "Spain", "ES"),
            (20, "France", "FR"));
        await ResetClickHouseAsync();

        await using var ctx = CreateContext();
        await ctx.EnsureDictionariesAsync();
        await ctx.ReloadDictionaryAsync<CountryLookup>();

        var dict = new ClickHouseDictionary<CountryLookup, ulong>(ctx);

        Assert.True(await dict.ContainsKeyAsync(10uL));
        Assert.True(await dict.ContainsKeyAsync(20uL));
        Assert.False(await dict.ContainsKeyAsync(999uL));

        Assert.Equal("Spain", await dict.GetAsync(10uL, c => c.Name));
        Assert.Equal("FR", await dict.GetAsync(20uL, c => c.IsoCode));
    }

    [Fact]
    public async Task DictionaryDefaults_AreAppliedForMissingKeys()
    {
        await SeedPostgresAsync((1, "Italy", "IT"));
        await ResetClickHouseAsync();

        await using var ctx = CreateContext();
        await ctx.EnsureDictionariesAsync();
        await ctx.ReloadDictionaryAsync<CountryLookup>();

        var dict = new ClickHouseDictionary<CountryLookup, ulong>(ctx);

        // Present key — ignores default.
        Assert.Equal("Italy", await dict.GetOrDefaultAsync(1uL, c => c.Name, "Unknown"));

        // Missing key — falls back to provided default.
        Assert.Equal("Unknown", await dict.GetOrDefaultAsync(999uL, c => c.Name, "Unknown"));
    }

    [Fact]
    public async Task ReloadDictionaryAsync_Typed_PicksUpNewSourceData()
    {
        await SeedPostgresAsync((1, "OldName", "XX"));
        await ResetClickHouseAsync();

        await using var ctx = CreateContext();
        await ctx.EnsureDictionariesAsync();
        await ctx.ReloadDictionaryAsync<CountryLookup>();

        var dict = new ClickHouseDictionary<CountryLookup, ulong>(ctx);
        Assert.Equal("OldName", await dict.GetAsync(1uL, c => c.Name));

        // Update Postgres source.
        await ExecutePostgresAsync("UPDATE countries SET \"Name\" = 'NewName' WHERE \"Id\" = 1");

        // Without reload, dict still serves cached value (until the lifetime expires). Force reload.
        await ctx.ReloadDictionaryAsync<CountryLookup>();
        Assert.Equal("NewName", await dict.GetAsync(1uL, c => c.Name));
    }

    [Fact]
    public async Task GetAllDictionaryDdl_ListsExternalDictionaries()
    {
        await using var ctx = CreateContext();

        var ddls = ctx.GetAllDictionaryDdl(externalOnly: true);

        Assert.Single(ddls);
        Assert.True(ddls.ContainsKey(nameof(CountryLookup)));
        var ddl = ddls[nameof(CountryLookup)];
        Assert.Contains("CREATE DICTIONARY", ddl);
        Assert.Contains("POSTGRESQL", ddl, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("country_lookup", ddl);
    }

    private async Task SeedPostgresAsync(params (long id, string name, string iso)[] rows)
    {
        // Postgres columns are quoted Pascal case to match the dictionary entity's property
        // names — the dictionary DDL emits SELECT "Id", "Name", "IsoCode" so the column names
        // on both sides must match exactly.
        await using var conn = new NpgsqlConnection(_fx.PostgresConnectionString);
        await conn.OpenAsync();
        await ExecAsync(conn, "DROP TABLE IF EXISTS countries");
        await ExecAsync(conn, @"
            CREATE TABLE countries (
                ""Id"" BIGINT PRIMARY KEY,
                ""Name"" TEXT NOT NULL,
                ""IsoCode"" TEXT NOT NULL
            )");
        foreach (var r in rows)
        {
            await using var cmd = new NpgsqlCommand(
                "INSERT INTO countries (\"Id\", \"Name\", \"IsoCode\") VALUES (@i, @n, @c)", conn);
            cmd.Parameters.AddWithValue("i", r.id);
            cmd.Parameters.AddWithValue("n", r.name);
            cmd.Parameters.AddWithValue("c", r.iso);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task ExecutePostgresAsync(string sql)
    {
        await using var conn = new NpgsqlConnection(_fx.PostgresConnectionString);
        await conn.OpenAsync();
        await ExecAsync(conn, sql);
    }

    private static async Task ExecAsync(NpgsqlConnection conn, string sql)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task ResetClickHouseAsync()
    {
        await RawClickHouse.ExecuteAsync(Conn, "DROP DICTIONARY IF EXISTS \"country_lookup\"");
    }

    private PgDictCtx CreateContext()
    {
        var options = new DbContextOptionsBuilder<PgDictCtx>()
            .UseClickHouse(_fx.ClickHouseConnectionString)
            .Options;
        return new PgDictCtx(options, _fx.PostgresHostAlias, _fx.PostgresUser, _fx.PostgresPassword, _fx.PostgresDatabase);
    }

    public sealed class CountryLookup : IClickHouseDictionary
    {
        public ulong Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string IsoCode { get; set; } = string.Empty;
    }

    public sealed class PgDictCtx : DbContext
    {
        private readonly string _pgHost;
        private readonly string _pgUser;
        private readonly string _pgPassword;
        private readonly string _pgDatabase;

        public PgDictCtx(DbContextOptions<PgDictCtx> options, string pgHost, string pgUser, string pgPassword, string pgDatabase)
            : base(options)
        {
            _pgHost = pgHost;
            _pgUser = pgUser;
            _pgPassword = pgPassword;
            _pgDatabase = pgDatabase;
        }

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<CountryLookup>(e =>
            {
                e.AsDictionary<CountryLookup>(cfg => cfg
                    .HasName("country_lookup")
                    .HasKey(x => x.Id)
                    .FromPostgreSql(pg => pg
                        .FromTable("countries")
                        .Connection(c => c
                            .Host(value: _pgHost)
                            .Port(value: 5432)
                            .Database(value: _pgDatabase)
                            .User(value: _pgUser)
                            .Password(value: _pgPassword)))
                    .UseHashedLayout()
                    .HasLifetime(minSeconds: 0, maxSeconds: 1));
            });
        }
    }
}

