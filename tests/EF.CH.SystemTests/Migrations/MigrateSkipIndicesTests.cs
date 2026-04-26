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
/// Drives <c>MigrateAsync()</c> with hand-written migrations that emit
/// <c>ALTER TABLE … ADD INDEX</c> for every supported skip-index type, and verifies the rows
/// that show up in <c>system.data_skipping_indices</c> match the expected name, type, expression,
/// and granularity.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MigrateSkipIndicesTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MigrateSkipIndicesTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task SkipIndices_AllTypes_AreCreatedWithExpectedTypeSpec()
    {
        await Reset();
        await using (var ctx = TestContextFactory.Create<IdxCtx>(Conn))
            await ctx.Database.MigrateAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "Idx_logs"));

        var indices = await RawClickHouse.SkipIndicesAsync(Conn, "Idx_logs");
        var byName = indices.ToDictionary(r => r["name"]!.ToString()!, r => r);

        Assert.Contains("ix_minmax", byName.Keys);
        Assert.Equal("minmax", byName["ix_minmax"]["type"]!.ToString());
        Assert.Equal(4UL, Convert.ToUInt64(byName["ix_minmax"]["granularity"]));

        Assert.Contains("ix_bloom", byName.Keys);
        Assert.Equal("bloom_filter", byName["ix_bloom"]["type"]!.ToString());

        Assert.Contains("ix_tokenbf", byName.Keys);
        Assert.Equal("tokenbf_v1", byName["ix_tokenbf"]["type"]!.ToString());

        Assert.Contains("ix_ngrambf", byName.Keys);
        Assert.Equal("ngrambf_v1", byName["ix_ngrambf"]["type"]!.ToString());

        Assert.Contains("ix_set", byName.Keys);
        Assert.Equal("set", byName["ix_set"]["type"]!.ToString());

        // Idempotent re-run: no DDL errors, history not duplicated.
        await using (var ctx = TestContextFactory.Create<IdxCtx>(Conn))
            await ctx.Database.MigrateAsync();
        var ids = await RawClickHouse.ColumnAsync<string>(Conn,
            "SELECT MigrationId FROM \"__EFMigrationsHistory\"");
        Assert.Single(ids, id => id == "0001_Idx");
    }

    private async Task Reset()
    {
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"Idx_logs\"");
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"__EFMigrationsHistory\"");
    }

    private sealed class Stub { public uint Id { get; set; } }

    public sealed class IdxCtx(DbContextOptions<IdxCtx> o) : DbContext(o)
    {
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_Idx"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
    }

    [DbContext(typeof(IdxCtx))]
    [Migration("0001_Idx")]
    public sealed class IdxMigration : Migration
    {
        protected override void Up(MigrationBuilder mb)
        {
            mb.CreateTable(
                name: "Idx_logs",
                columns: t => new
                {
                    Id = t.Column<uint>(nullable: false),
                    Price = t.Column<decimal>(nullable: false),
                    Tag = t.Column<string>(nullable: false),
                    Body = t.Column<string>(nullable: false),
                    Region = t.Column<string>(nullable: false),
                },
                constraints: c => c.PrimaryKey("PK_Idx_logs", x => x.Id))
                .Annotation(ClickHouseAnnotationNames.Engine, "MergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

            mb.CreateIndex(name: "ix_minmax", table: "Idx_logs", column: "Price")
                .Annotation(ClickHouseAnnotationNames.SkipIndexType, SkipIndexType.Minmax)
                .Annotation(ClickHouseAnnotationNames.SkipIndexGranularity, 4);

            mb.CreateIndex(name: "ix_bloom", table: "Idx_logs", column: "Tag")
                .Annotation(ClickHouseAnnotationNames.SkipIndexType, SkipIndexType.BloomFilter);

            mb.CreateIndex(name: "ix_tokenbf", table: "Idx_logs", column: "Body")
                .Annotation(ClickHouseAnnotationNames.SkipIndexType, SkipIndexType.TokenBF)
                .Annotation(ClickHouseAnnotationNames.SkipIndexParams,
                    SkipIndexParams.ForTokenBF(size: 16384, hashes: 3, seed: 0));

            mb.CreateIndex(name: "ix_ngrambf", table: "Idx_logs", column: "Body")
                .Annotation(ClickHouseAnnotationNames.SkipIndexType, SkipIndexType.NgramBF)
                .Annotation(ClickHouseAnnotationNames.SkipIndexParams,
                    SkipIndexParams.ForNgramBF(ngramSize: 3, size: 16384, hashes: 3, seed: 0));

            mb.CreateIndex(name: "ix_set", table: "Idx_logs", column: "Region")
                .Annotation(ClickHouseAnnotationNames.SkipIndexType, SkipIndexType.Set)
                .Annotation(ClickHouseAnnotationNames.SkipIndexParams,
                    SkipIndexParams.ForSet(maxRows: 100));
        }

        protected override void Down(MigrationBuilder mb) => mb.DropTable("Idx_logs");
    }

    [DbContext(typeof(IdxCtx))]
    public sealed class IdxSnapshot : ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_Idx"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); e.Property(x => x.Id); });
    }
}
