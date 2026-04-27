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
/// Drives <c>MigrateAsync()</c> with hand-written migrations that create materialised views,
/// then verifies the MV exists in <c>system.tables</c>, that source-table inserts propagate
/// through the MV, and (separately) that a refreshable MV registers a row in
/// <c>system.view_refreshes</c> and actually fires.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MigrateMaterializedViewsTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MigrateMaterializedViewsTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task RegularMv_Created_AndPropagatesInserts()
    {
        await Reset(new[] { "MV_events_per_day", "MV_events", "__EFMigrationsHistory" });

        await using (var ctx = TestContextFactory.Create<RegularCtx>(Conn))
            await ctx.Database.MigrateAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "MV_events"));
        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "MV_events_per_day"));

        var mvEngine = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'MV_events_per_day'");
        Assert.Equal("MaterializedView", mvEngine);

        var createSql = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'MV_events_per_day'");
        Assert.Contains("SummingMergeTree", createSql);
        Assert.Contains("toDate(", createSql);

        // Insert into source — the MV's INSERT trigger should populate the target.
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"MV_events\" (\"Id\", \"EventTime\", \"Value\") VALUES " +
            "(1, '2026-04-01 10:00:00', 5), " +
            "(2, '2026-04-01 11:00:00', 3), " +
            "(3, '2026-04-02 09:00:00', 7)");

        await RawClickHouse.SettleMaterializationAsync(Conn, "MV_events_per_day");

        var totalDays = await RawClickHouse.ScalarAsync<ulong>(Conn,
            "SELECT count() FROM \"MV_events_per_day\" FINAL");
        Assert.Equal(2UL, totalDays);

        var totalCount = await RawClickHouse.ScalarAsync<ulong>(Conn,
            "SELECT sum(\"Cnt\") FROM \"MV_events_per_day\" FINAL");
        Assert.Equal(3UL, totalCount);

        // Re-run is idempotent.
        await using (var ctx = TestContextFactory.Create<RegularCtx>(Conn))
            await ctx.Database.MigrateAsync();
        var ids = await RawClickHouse.ColumnAsync<string>(Conn,
            "SELECT MigrationId FROM \"__EFMigrationsHistory\"");
        Assert.Single(ids, id => id == "0001_RegularMv");
    }

    [Fact]
    public async Task DeferredMv_NotCreatedDuringMigrate()
    {
        await Reset(new[] { "MV_def_target", "MV_def_source", "__EFMigrationsHistory" });

        await using var ctx = TestContextFactory.Create<DeferredCtx>(Conn);
        await ctx.Database.MigrateAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "MV_def_source"));
        Assert.False(await RawClickHouse.TableExistsAsync(Conn, "MV_def_target"));
    }

    [Fact]
    public async Task RefreshableMv_FiresOnSchedule()
    {
        await Reset(new[] { "MV_rf_summary", "MV_rf_events", "__EFMigrationsHistory" });

        await using (var ctx = TestContextFactory.Create<RefreshableCtx>(Conn))
            await ctx.Database.MigrateAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "MV_rf_summary"));

        var refreshRow = await RawClickHouse.ViewRefreshAsync(Conn, "MV_rf_summary");
        Assert.NotNull(refreshRow);

        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"MV_rf_events\" (\"Id\", \"Value\") VALUES (1, 10), (2, 20)");

        // Wait for the *next* refresh after the insert — clock-skew tolerant.
        var mustExceed = DateTime.UtcNow.AddSeconds(1);
        await RawClickHouse.WaitForViewRefreshAsync(Conn, "MV_rf_summary", mustExceed,
            TimeSpan.FromSeconds(20));

        var total = await RawClickHouse.ScalarAsync<long>(Conn,
            "SELECT sum(\"Total\") FROM \"MV_rf_summary\"");
        Assert.Equal(30L, total);
    }

    private async Task Reset(string[] tables)
    {
        foreach (var t in tables)
            await RawClickHouse.ExecuteAsync(Conn, $"DROP TABLE IF EXISTS \"{t}\"");
    }

    private sealed class Stub { public uint Id { get; set; } }

    // ---- Regular MV -------------------------------------------------------

    public sealed class RegularCtx(DbContextOptions<RegularCtx> o) : DbContext(o)
    {
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_Regular"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
    }

    [DbContext(typeof(RegularCtx))]
    [Migration("0001_RegularMv")]
    public sealed class RegularMigration : Migration
    {
        protected override void Up(MigrationBuilder mb)
        {
            mb.CreateTable(
                name: "MV_events",
                columns: t => new
                {
                    Id = t.Column<uint>(nullable: false),
                    EventTime = t.Column<DateTime>(nullable: false),
                    Value = t.Column<int>(nullable: false),
                },
                constraints: c => c.PrimaryKey("PK_MV_events", x => x.Id))
                .Annotation(ClickHouseAnnotationNames.Engine, "MergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

            mb.CreateTable(
                name: "MV_events_per_day",
                columns: t => new
                {
                    Day = t.Column<DateTime>(nullable: false),
                    Cnt = t.Column<ulong>(nullable: false),
                })
                .Annotation(ClickHouseAnnotationNames.Engine, "SummingMergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Day" })
                .Annotation(ClickHouseAnnotationNames.MaterializedView, true)
                .Annotation(ClickHouseAnnotationNames.MaterializedViewSource, "MV_events")
                .Annotation(ClickHouseAnnotationNames.MaterializedViewQuery,
                    "SELECT toDate(\"EventTime\") AS \"Day\", count() AS \"Cnt\" FROM \"MV_events\" GROUP BY \"Day\"")
                .Annotation(ClickHouseAnnotationNames.MaterializedViewPopulate, true);
        }

        protected override void Down(MigrationBuilder mb)
        {
            mb.DropTable("MV_events_per_day");
            mb.DropTable("MV_events");
        }
    }

    [DbContext(typeof(RegularCtx))]
    public sealed class RegularSnapshot : ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_Regular"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); e.Property(x => x.Id); });
    }

    // ---- Deferred MV ------------------------------------------------------

    public sealed class DeferredCtx(DbContextOptions<DeferredCtx> o) : DbContext(o)
    {
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_Deferred"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
    }

    [DbContext(typeof(DeferredCtx))]
    [Migration("0001_DeferredMv")]
    public sealed class DeferredMigration : Migration
    {
        protected override void Up(MigrationBuilder mb)
        {
            mb.CreateTable(
                name: "MV_def_source",
                columns: t => new
                {
                    Id = t.Column<uint>(nullable: false),
                    Name = t.Column<string>(nullable: false),
                },
                constraints: c => c.PrimaryKey("PK_MV_def_source", x => x.Id))
                .Annotation(ClickHouseAnnotationNames.Engine, "MergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

            mb.CreateTable(
                name: "MV_def_target",
                columns: t => new
                {
                    Id = t.Column<uint>(nullable: false),
                })
                .Annotation(ClickHouseAnnotationNames.Engine, "MergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" })
                .Annotation(ClickHouseAnnotationNames.MaterializedView, true)
                .Annotation(ClickHouseAnnotationNames.MaterializedViewDeferred, true)
                .Annotation(ClickHouseAnnotationNames.MaterializedViewSource, "MV_def_source")
                .Annotation(ClickHouseAnnotationNames.MaterializedViewQuery,
                    "SELECT \"Id\" FROM \"MV_def_source\"");
        }

        protected override void Down(MigrationBuilder mb)
        {
            mb.DropTable("MV_def_target");
            mb.DropTable("MV_def_source");
        }
    }

    [DbContext(typeof(DeferredCtx))]
    public sealed class DeferredSnapshot : ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_Deferred"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); e.Property(x => x.Id); });
    }

    // ---- Refreshable MV ---------------------------------------------------

    public sealed class RefreshableCtx(DbContextOptions<RefreshableCtx> o) : DbContext(o)
    {
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_Refreshable"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
    }

    [DbContext(typeof(RefreshableCtx))]
    [Migration("0001_RefreshableMv")]
    public sealed class RefreshableMigration : Migration
    {
        protected override void Up(MigrationBuilder mb)
        {
            mb.CreateTable(
                name: "MV_rf_events",
                columns: t => new
                {
                    Id = t.Column<uint>(nullable: false),
                    Value = t.Column<int>(nullable: false),
                },
                constraints: c => c.PrimaryKey("PK_MV_rf_events", x => x.Id))
                .Annotation(ClickHouseAnnotationNames.Engine, "MergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

            mb.CreateTable(
                name: "MV_rf_summary",
                columns: t => new
                {
                    Total = t.Column<long>(nullable: false),
                })
                .Annotation(ClickHouseAnnotationNames.Engine, "MergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Total" })
                .Annotation(ClickHouseAnnotationNames.MaterializedView, true)
                .Annotation(ClickHouseAnnotationNames.MaterializedViewSource, "MV_rf_events")
                .Annotation(ClickHouseAnnotationNames.MaterializedViewQuery,
                    "SELECT sum(\"Value\") AS \"Total\" FROM \"MV_rf_events\"")
                .Annotation(ClickHouseAnnotationNames.MaterializedViewRefreshKind, "EVERY")
                .Annotation(ClickHouseAnnotationNames.MaterializedViewRefreshInterval, "2 SECOND");
        }

        protected override void Down(MigrationBuilder mb)
        {
            mb.DropTable("MV_rf_summary");
            mb.DropTable("MV_rf_events");
        }
    }

    [DbContext(typeof(RefreshableCtx))]
    public sealed class RefreshableSnapshot : ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_Refreshable"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); e.Property(x => x.Id); });
    }
}
