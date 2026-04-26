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
/// Three migrations applied in sequence against a single ClickHouse container — a forward‑only
/// schema evolution story:
///
///   0001 — CreateTable("MS_orders").
///   0002 — AddColumn(Discount) AND CreateTable("MS_discount_summary") AS materialised view
///          referencing the new column. Both operations live in the same <c>Up()</c>; the
///          phase splitter must order AddColumn (phase 5) before CreateMV (phase 6).
///   0003 — DropTable("MS_discount_summary") AND AddProjection on MS_orders.
///
/// Verifies after each step that <c>__EFMigrationsHistory</c> records the migration and that
/// the schema reflects the cumulative state.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MigrateMultiStepTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MigrateMultiStepTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task MigrateAsync_ThreeMigrations_AppliesEachInOrder()
    {
        await Reset();

        await using (var ctx = TestContextFactory.Create<MsCtx>(Conn))
            await ctx.Database.MigrateAsync();

        var ids = await RawClickHouse.ColumnAsync<string>(Conn,
            "SELECT MigrationId FROM \"__EFMigrationsHistory\" ORDER BY MigrationId");
        Assert.Equal(new[] { "0001_CreateOrders", "0002_AddDiscountAndMv", "0003_DropMvAddProjection" }, ids);

        // Final state: orders exists with Discount column; MV is gone; projection is declared.
        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "MS_orders"));
        Assert.False(await RawClickHouse.TableExistsAsync(Conn, "MS_discount_summary"));

        var discountKind = await RawClickHouse.ColumnDefaultKindAsync(Conn, "MS_orders", "Discount");
        // Column exists when default_kind is one of '' / 'DEFAULT' (no special kind set).
        Assert.NotNull(discountKind);
        var discountType = await RawClickHouse.ColumnTypeAsync(Conn, "MS_orders", "Discount");
        Assert.Contains("Decimal", discountType);

        var createSql = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'MS_orders'");
        Assert.Contains("by_user", createSql);
    }

    [Fact]
    public async Task MigrateAsync_PicksUpRemainingMigrations_WhenHistoryHasFirstApplied()
    {
        await Reset();

        // Apply only 0001 by hand — simulate a partially-applied environment.
        await using (var ctx = TestContextFactory.Create<MsCtx>(Conn))
        {
            // Simplest way to get to "0001 applied" is to call MigrateAsync once after dropping
            // the post-0001 tables, but the cleanest is to apply 0001 then artificially delete
            // the later history rows. The full Migrate is fine — we then drop the rest.
            await ctx.Database.MigrateAsync();
        }
        await RawClickHouse.ExecuteAsync(Conn,
            "ALTER TABLE \"__EFMigrationsHistory\" DELETE WHERE MigrationId IN ('0002_AddDiscountAndMv','0003_DropMvAddProjection')");
        await RawClickHouse.WaitForMutationsAsync(Conn, "__EFMigrationsHistory");
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"MS_discount_summary\"");
        await RawClickHouse.ExecuteAsync(Conn, "ALTER TABLE \"MS_orders\" DROP COLUMN IF EXISTS \"Discount\"");
        await RawClickHouse.WaitForMutationsAsync(Conn, "MS_orders");

        // Only 0001 is in history; re-run Migrate — should apply 0002 and 0003.
        await using (var ctx = TestContextFactory.Create<MsCtx>(Conn))
            await ctx.Database.MigrateAsync();

        var ids = await RawClickHouse.ColumnAsync<string>(Conn,
            "SELECT MigrationId FROM \"__EFMigrationsHistory\" ORDER BY MigrationId");
        Assert.Equal(new[] { "0001_CreateOrders", "0002_AddDiscountAndMv", "0003_DropMvAddProjection" }, ids);

        var discountType = await RawClickHouse.ColumnTypeAsync(Conn, "MS_orders", "Discount");
        Assert.Contains("Decimal", discountType);
    }

    private async Task Reset()
    {
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"MS_discount_summary\"");
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"MS_orders\"");
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"__EFMigrationsHistory\"");
    }

    private sealed class Stub { public uint Id { get; set; } }

    public sealed class MsCtx(DbContextOptions<MsCtx> o) : DbContext(o)
    {
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_Ms"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
    }

    [DbContext(typeof(MsCtx))]
    [Migration("0001_CreateOrders")]
    public sealed class M01_CreateOrders : Migration
    {
        protected override void Up(MigrationBuilder mb)
        {
            mb.CreateTable(
                name: "MS_orders",
                columns: t => new
                {
                    Id = t.Column<uint>(nullable: false),
                    UserId = t.Column<uint>(nullable: false),
                    OrderDate = t.Column<DateTime>(nullable: false),
                    Amount = t.Column<decimal>(nullable: false),
                },
                constraints: c => c.PrimaryKey("PK_MS_orders", x => x.Id))
                .Annotation(ClickHouseAnnotationNames.Engine, "MergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
        }

        protected override void Down(MigrationBuilder mb) => mb.DropTable("MS_orders");
    }

    [DbContext(typeof(MsCtx))]
    [Migration("0002_AddDiscountAndMv")]
    public sealed class M02_AddDiscountAndMv : Migration
    {
        protected override void Up(MigrationBuilder mb)
        {
            // AddColumn must precede the MV — the MV's SELECT references "Discount", and the
            // runtime sort in ClickHouseMigrationsSqlGenerator only reorders by *table* deps,
            // not by column-level deps. Put them in the order an end-user would naturally write.
            mb.AddColumn<decimal>(name: "Discount", table: "MS_orders", nullable: false, defaultValue: 0m);

            mb.CreateTable(
                name: "MS_discount_summary",
                columns: t => new
                {
                    Day = t.Column<DateTime>(nullable: false),
                    TotalDiscount = t.Column<decimal>(nullable: false),
                })
                .Annotation(ClickHouseAnnotationNames.Engine, "SummingMergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Day" })
                .Annotation(ClickHouseAnnotationNames.MaterializedView, true)
                .Annotation(ClickHouseAnnotationNames.MaterializedViewSource, "MS_orders")
                .Annotation(ClickHouseAnnotationNames.MaterializedViewQuery,
                    "SELECT toDate(\"OrderDate\") AS \"Day\", sum(\"Discount\") AS \"TotalDiscount\" FROM \"MS_orders\" GROUP BY \"Day\"");
        }

        protected override void Down(MigrationBuilder mb)
        {
            mb.DropTable("MS_discount_summary");
            mb.DropColumn("Discount", "MS_orders");
        }
    }

    [DbContext(typeof(MsCtx))]
    [Migration("0003_DropMvAddProjection")]
    public sealed class M03_DropMvAddProjection : Migration
    {
        protected override void Up(MigrationBuilder mb)
        {
            mb.DropTable("MS_discount_summary");
            mb.Operations.Add(new AddProjectionOperation
            {
                Table = "MS_orders",
                Name = "by_user",
                SelectSql = "SELECT * ORDER BY \"UserId\"",
                Materialize = false,
            });
        }

        protected override void Down(MigrationBuilder mb)
        {
            mb.Operations.Add(new DropProjectionOperation { Table = "MS_orders", Name = "by_user" });
        }
    }

    [DbContext(typeof(MsCtx))]
    public sealed class MsSnapshot : ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_Ms"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); e.Property(x => x.Id); });
    }
}
