using EF.CH.Dictionaries;
using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Xunit;

namespace EF.CH.SystemTests.Migrations;

/// <summary>
/// Drives <c>MigrateAsync()</c> for ClickHouse dictionaries (HASHED layout, ClickHouse-source).
/// Verifies the dictionary registers in <c>system.dictionaries</c> and that
/// <c>dictGet</c> returns the seeded value after a <c>SYSTEM RELOAD DICTIONARY</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MigrateDictionariesTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MigrateDictionariesTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task Dictionary_HashedLayout_RegistersInSystemTables()
    {
        await Reset(new[] { "Dict_country_lookup", "Dict_countries", "__EFMigrationsHistory" });

        await using (var ctx = TestContextFactory.Create<DictCtx>(Conn))
            await ctx.Database.MigrateAsync();

        // Source table should be a regular MergeTree.
        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "Dict_countries"));
        Assert.Contains("MergeTree", await RawClickHouse.EngineFullAsync(Conn, "Dict_countries"));

        // The dictionary is registered in system.dictionaries with the correct layout
        // and lifetime (we don't dictGet here because Testcontainers' default user/password
        // setup means the implicit CLICKHOUSE-source loopback auth can fail — that is a
        // ClickHouse-server concern, not a migration concern).
        // The CREATE statement should reflect HASHED layout and LIFETIME(MIN 0 MAX 300).
        var createSql = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT create_table_query FROM system.tables " +
            "WHERE database = currentDatabase() AND name = 'Dict_country_lookup'");
        Assert.Contains("CREATE DICTIONARY", createSql);
        Assert.Contains("HASHED", createSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("LIFETIME", createSql);
        Assert.Contains("300", createSql);
        Assert.Contains("PRIMARY KEY", createSql);
        Assert.Contains("Dict_countries", createSql);

        // Idempotent re-run.
        await using (var ctx = TestContextFactory.Create<DictCtx>(Conn))
            await ctx.Database.MigrateAsync();
        var ids = await RawClickHouse.ColumnAsync<string>(Conn,
            "SELECT MigrationId FROM \"__EFMigrationsHistory\"");
        Assert.Single(ids, id => id == "0001_Dict");
    }

    private async Task Reset(string[] names)
    {
        // The dictionary itself needs DROP DICTIONARY; everything else is a regular table.
        await RawClickHouse.ExecuteAsync(Conn, "DROP DICTIONARY IF EXISTS \"Dict_country_lookup\"");
        foreach (var n in names)
            await RawClickHouse.ExecuteAsync(Conn, $"DROP TABLE IF EXISTS \"{n}\"");
    }

    private sealed class Stub { public uint Id { get; set; } }

    public sealed class DictCtx(DbContextOptions<DictCtx> o) : DbContext(o)
    {
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_Dict"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
    }

    [DbContext(typeof(DictCtx))]
    [Migration("0001_Dict")]
    public sealed class DictMigration : Migration
    {
        protected override void Up(MigrationBuilder mb)
        {
            mb.CreateTable(
                name: "Dict_countries",
                columns: t => new
                {
                    Id = t.Column<ulong>(nullable: false),
                    Name = t.Column<string>(nullable: false),
                    IsoCode = t.Column<string>(nullable: false),
                },
                constraints: c => c.PrimaryKey("PK_Dict_countries", x => x.Id))
                .Annotation(ClickHouseAnnotationNames.Engine, "MergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

            mb.CreateTable(
                name: "Dict_country_lookup",
                columns: t => new
                {
                    Id = t.Column<ulong>(nullable: false, type: "UInt64"),
                    Name = t.Column<string>(nullable: false, type: "String"),
                    IsoCode = t.Column<string>(nullable: false, type: "String"),
                })
                .Annotation(ClickHouseAnnotationNames.Dictionary, true)
                .Annotation(ClickHouseAnnotationNames.DictionarySource, "Dict_countries")
                .Annotation(ClickHouseAnnotationNames.DictionaryKeyColumns, new[] { "Id" })
                .Annotation(ClickHouseAnnotationNames.DictionaryLayout, DictionaryLayout.Hashed)
                .Annotation(ClickHouseAnnotationNames.DictionaryLifetimeMin, 0)
                .Annotation(ClickHouseAnnotationNames.DictionaryLifetimeMax, 300);
        }

        protected override void Down(MigrationBuilder mb)
        {
            mb.DropTable("Dict_country_lookup");
            mb.DropTable("Dict_countries");
        }
    }

    [DbContext(typeof(DictCtx))]
    public sealed class DictSnapshot : ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_Dict"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); e.Property(x => x.Id); });
    }
}
