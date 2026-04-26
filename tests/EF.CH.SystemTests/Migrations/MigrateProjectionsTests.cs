using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Operations;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Xunit;

namespace EF.CH.SystemTests.Migrations;

/// <summary>
/// Drives <c>MigrateAsync()</c> with hand-written migrations that emit the custom
/// <see cref="AddProjectionOperation"/>, <see cref="MaterializeProjectionOperation"/>, and
/// <see cref="DropProjectionOperation"/>. Verifies the projections are declared in the table's
/// engine-full and that materialised parts populate <c>system.projection_parts</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MigrateProjectionsTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MigrateProjectionsTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task AddProjection_WithMaterialize_AppearsInEngineFullAndProjectionParts()
    {
        await Reset();
        await using (var ctx = TestContextFactory.Create<ProjCtx>(Conn))
            await ctx.Database.MigrateAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "Proj_orders"));

        // Both projections were added via ALTER TABLE; query system.tables.create_table_query.
        var createSql = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'Proj_orders'");
        Assert.Contains("by_user", createSql);
        Assert.Contains("by_region", createSql);

        // Insert some rows then materialize the by_region projection — projection_parts
        // should pick it up after the materialise mutation completes.
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"Proj_orders\" (\"Id\", \"UserId\", \"Region\", \"Amount\") VALUES " +
            "(1, 100, 'EU', 50), (2, 100, 'US', 75), (3, 200, 'EU', 25)");

        await RawClickHouse.WaitForMutationsAsync(Conn, "Proj_orders", TimeSpan.FromSeconds(15));

        // by_region was added with Materialize=true, so its parts should be populated.
        var byRegionParts = await RawClickHouse.ProjectionPartsCountAsync(Conn, "Proj_orders", "by_region");
        Assert.True(byRegionParts >= 1, $"Expected by_region projection parts to be materialised, got {byRegionParts}");

        // Idempotent re-run.
        await using (var ctx = TestContextFactory.Create<ProjCtx>(Conn))
            await ctx.Database.MigrateAsync();
        var ids = await RawClickHouse.ColumnAsync<string>(Conn,
            "SELECT MigrationId FROM \"__EFMigrationsHistory\"");
        Assert.Single(ids, id => id == "0001_Proj");
    }

    private async Task Reset()
    {
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"Proj_orders\"");
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"__EFMigrationsHistory\"");
    }

    private sealed class Stub { public uint Id { get; set; } }

    public sealed class ProjCtx(DbContextOptions<ProjCtx> o) : DbContext(o)
    {
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_Proj"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
    }

    [DbContext(typeof(ProjCtx))]
    [Migration("0001_Proj")]
    public sealed class ProjMigration : Migration
    {
        protected override void Up(MigrationBuilder mb)
        {
            mb.CreateTable(
                name: "Proj_orders",
                columns: t => new
                {
                    Id = t.Column<uint>(nullable: false),
                    UserId = t.Column<uint>(nullable: false),
                    Region = t.Column<string>(nullable: false),
                    Amount = t.Column<decimal>(nullable: false),
                },
                constraints: c => c.PrimaryKey("PK_Proj_orders", x => x.Id))
                .Annotation(ClickHouseAnnotationNames.Engine, "MergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

            // Sort-order projection by user (no eager materialise — just declare).
            mb.Operations.Add(new AddProjectionOperation
            {
                Table = "Proj_orders",
                Name = "by_user",
                SelectSql = "SELECT * ORDER BY \"UserId\"",
                Materialize = false,
            });

            // Aggregation projection by region, with eager materialise.
            mb.Operations.Add(new AddProjectionOperation
            {
                Table = "Proj_orders",
                Name = "by_region",
                SelectSql = "SELECT \"Region\", sum(\"Amount\"), count() GROUP BY \"Region\"",
                Materialize = true,
            });
        }

        protected override void Down(MigrationBuilder mb)
        {
            mb.Operations.Add(new DropProjectionOperation { Table = "Proj_orders", Name = "by_region" });
            mb.Operations.Add(new DropProjectionOperation { Table = "Proj_orders", Name = "by_user" });
            mb.DropTable("Proj_orders");
        }
    }

    [DbContext(typeof(ProjCtx))]
    public sealed class ProjSnapshot : ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_Proj"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); e.Property(x => x.Id); });
    }
}
