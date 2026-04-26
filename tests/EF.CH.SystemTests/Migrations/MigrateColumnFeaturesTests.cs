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
/// Drives <c>MigrateAsync()</c> for a table that uses ClickHouse-specific column features —
/// <c>CODEC</c>, <c>MATERIALIZED</c>, <c>ALIAS</c>, <c>EPHEMERAL</c>. The features are emitted via
/// the <see cref="MigrationBuilder.CreateTable"/> column-type string (the EF.CH SQL generator
/// appends <c>CODEC(...)</c>, <c>MATERIALIZED ...</c>, etc. only when the live entity-model
/// supplies those annotations on the property — which the runtime <c>Migrate</c> path doesn't
/// surface, so we put the clauses inline). This validates the end-to-end runtime DDL execution.
///
/// The annotation-based propagation is covered by the unit-level
/// <c>tests/EF.CH.Tests/Migrations/MigrationTests.cs</c> using a constructed model.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MigrateColumnFeaturesTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MigrateColumnFeaturesTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task ColumnFeatures_CodecMaterializedAliasEphemeral_AreApplied()
    {
        await Reset();
        await using (var ctx = TestContextFactory.Create<ColCtx>(Conn))
            await ctx.Database.MigrateAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "ColFeat_rows"));

        var tagsCodec = await RawClickHouse.ColumnCompressionCodecAsync(Conn, "ColFeat_rows", "Tags");
        Assert.Contains("ZSTD", tagsCodec);

        Assert.Equal("MATERIALIZED",
            await RawClickHouse.ColumnDefaultKindAsync(Conn, "ColFeat_rows", "Total"));
        Assert.Equal("ALIAS",
            await RawClickHouse.ColumnDefaultKindAsync(Conn, "ColFeat_rows", "Label"));
        Assert.Equal("EPHEMERAL",
            await RawClickHouse.ColumnDefaultKindAsync(Conn, "ColFeat_rows", "TempField"));

        // End-to-end: insert source columns; MATERIALIZED column should be auto-computed.
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"ColFeat_rows\" (\"Id\", \"Tags\", \"Price\", \"Qty\") VALUES (1, 'a,b,c', 10, 5)");

        var total = await RawClickHouse.ScalarAsync<long>(Conn,
            "SELECT \"Total\" FROM \"ColFeat_rows\" WHERE \"Id\" = 1");
        Assert.Equal(50L, total);

        var label = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT \"Label\" FROM \"ColFeat_rows\" WHERE \"Id\" = 1");
        Assert.Equal("row-1", label);

        // Idempotent re-run.
        await using (var ctx = TestContextFactory.Create<ColCtx>(Conn))
            await ctx.Database.MigrateAsync();
        var ids = await RawClickHouse.ColumnAsync<string>(Conn,
            "SELECT MigrationId FROM \"__EFMigrationsHistory\"");
        Assert.Single(ids, id => id == "0001_ColFeat");
    }

    private async Task Reset()
    {
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"ColFeat_rows\"");
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"__EFMigrationsHistory\"");
    }

    private sealed class Stub { public uint Id { get; set; } }

    public sealed class ColCtx(DbContextOptions<ColCtx> o) : DbContext(o)
    {
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_ColFeat"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
    }

    [DbContext(typeof(ColCtx))]
    [Migration("0001_ColFeat")]
    public sealed class ColMigration : Migration
    {
        protected override void Up(MigrationBuilder mb)
        {
            mb.CreateTable(
                name: "ColFeat_rows",
                columns: t => new
                {
                    Id = t.Column<uint>(nullable: false),
                    Tags = t.Column<string>(nullable: false, type: "String CODEC(ZSTD(9))"),
                    Price = t.Column<int>(nullable: false),
                    Qty = t.Column<int>(nullable: false),
                    Total = t.Column<long>(nullable: false, type: "Int64 MATERIALIZED \"Price\" * \"Qty\""),
                    Label = t.Column<string>(nullable: false, type: "String ALIAS concat('row-', toString(\"Id\"))"),
                    TempField = t.Column<string>(nullable: false, type: "String EPHEMERAL ''"),
                },
                constraints: c => c.PrimaryKey("PK_ColFeat_rows", x => x.Id))
                .Annotation(ClickHouseAnnotationNames.Engine, "MergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
        }

        protected override void Down(MigrationBuilder mb) => mb.DropTable("ColFeat_rows");
    }

    [DbContext(typeof(ColCtx))]
    public sealed class ColSnapshot : ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_ColFeat"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); e.Property(x => x.Id); });
    }
}
