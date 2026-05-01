using EF.CH.Dictionaries;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Migrations;

/// <summary>
/// Integration tests for the runtime dictionary management surface
/// (<see cref="DictionaryDbContextExtensions"/>) against ClickHouse-source dictionaries.
/// Live <c>dictGet</c> coverage lives in <see cref="EF.CH.SystemTests.External.PostgresDictionaryTests"/>
/// — the ClickHouse-source path can't reliably exercise <c>dictGet</c> on the default
/// Testcontainers user (loopback auth).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public sealed class DictionaryLifecycleTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public DictionaryLifecycleTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task EnsureAllDictionariesAsync_ClickHouseSource_CreatesDictionary()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<ChDictCtx>(Conn);

        // EF can't auto-create the source table in the same model as the dictionary entity,
        // so create it raw (matches the established dictionary test pattern).
        await ctx.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE country_source (
                id UInt64,
                name String,
                iso_code String
            ) ENGINE = MergeTree() ORDER BY id");
        await ctx.Database.ExecuteSqlRawAsync(@"
            INSERT INTO country_source (id, name, iso_code) VALUES
            (1, 'United States', 'US'),
            (2, 'United Kingdom', 'GB'),
            (3, 'Germany', 'DE')");

        var created = await ctx.EnsureAllDictionariesAsync();
        Assert.Equal(1, created);

        // Dictionary status is NOT_LOADED until first lookup (ClickHouse loads lazily). The
        // ClickHouse-source loopback auth path also blocks dictGet on the default Testcontainers
        // user, so we don't try to force a load here. PostgresDictionaryTests covers the full
        // load + lookup path against an external (auth-clean) source.
        var status = await RawClickHouse.DictionaryStatusAsync(Conn, "country_lookup");
        Assert.Contains(status, new[] { "NOT_LOADED", "LOADED", "LOADING" });

        var createSql = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT create_table_query FROM system.tables " +
            "WHERE database = currentDatabase() AND name = 'country_lookup'");
        Assert.Contains("CREATE DICTIONARY", createSql);
        Assert.Contains("HASHED", createSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("LIFETIME", createSql);
        Assert.Contains("country_source", createSql);
    }

    [Fact]
    public async Task EnsureAllDictionariesAsync_IsIdempotent()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<ChDictCtx>(Conn);

        await ctx.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE country_source (id UInt64, name String, iso_code String)
            ENGINE = MergeTree() ORDER BY id");

        await ctx.EnsureAllDictionariesAsync();
        await ctx.EnsureAllDictionariesAsync(); // second call must not throw

        var count = await RawClickHouse.ScalarAsync<ulong>(Conn,
            "SELECT count() FROM system.dictionaries WHERE database = currentDatabase() AND name = 'country_lookup'");
        Assert.Equal(1uL, count);
    }

    [Fact]
    public async Task GetDictionaryDdl_GeneratesExpectedShape()
    {
        await using var ctx = TestContextFactory.Create<ChDictCtx>(Conn);
        var ddl = ctx.GetDictionaryDdl<CountryLookup>();

        Assert.Contains("CREATE DICTIONARY", ddl);
        Assert.Contains("country_lookup", ddl);
        Assert.Contains("PRIMARY KEY", ddl);
        Assert.Contains("LIFETIME", ddl);
        Assert.Contains("LAYOUT", ddl);
        Assert.Contains("HASHED", ddl, StringComparison.OrdinalIgnoreCase);
    }

    private async Task ResetAsync()
    {
        await RawClickHouse.ExecuteAsync(Conn, "DROP DICTIONARY IF EXISTS \"country_lookup\"");
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT name FROM system.tables WHERE database = currentDatabase() AND name NOT LIKE '.%'");
        foreach (var r in rows)
            await RawClickHouse.ExecuteAsync(Conn, $"DROP TABLE IF EXISTS \"{(string)r["name"]!}\" SYNC");
    }

    public sealed class CountrySource
    {
        public ulong Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string IsoCode { get; set; } = string.Empty;
    }

    public sealed class CountryLookup : IClickHouseDictionary
    {
        public ulong Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string IsoCode { get; set; } = string.Empty;
    }

    public sealed class ChDictCtx(DbContextOptions<ChDictCtx> o) : DbContext(o)
    {
        public DbSet<CountrySource> Sources => Set<CountrySource>();
        public DbSet<CountryLookup> CountryLookups => Set<CountryLookup>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<CountrySource>(e =>
            {
                e.ToTable("country_source");
                e.HasKey(x => x.Id);
                e.Property(x => x.Id).HasColumnName("id");
                e.Property(x => x.Name).HasColumnName("name");
                e.Property(x => x.IsoCode).HasColumnName("iso_code");
            });

            mb.Entity<CountryLookup>(e =>
            {
                e.AsDictionary<CountryLookup, CountrySource>(cfg => cfg
                    .HasName("country_lookup")
                    .HasKey(x => x.Id)
                    .FromTable(
                        projection: c => new CountryLookup
                        {
                            Id = c.Id,
                            Name = c.Name,
                            IsoCode = c.IsoCode,
                        })
                    .UseHashedLayout()
                    .HasLifetime(minSeconds: 0, maxSeconds: 300));
            });
        }
    }
}
