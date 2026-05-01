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
/// End-to-end check that <c>ALTER TABLE ... RENAME COLUMN ... TO ...</c> applies
/// against a real ClickHouse server (22.4+). Confirms the SQL emitted by
/// <c>ClickHouseMigrationsSqlGenerator.Generate(RenameColumnOperation, ...)</c>
/// is what ClickHouse expects.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MigrateRenameColumnTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MigrateRenameColumnTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task RenameColumn_OnMergeTree_AppliesViaMigration()
    {
        await Reset();

        await using (var ctx = TestContextFactory.Create<RenameCtx>(Conn))
            await ctx.Database.MigrateAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "rename_rows"));
        Assert.Equal(1UL, await ColumnCount(Conn, "rename_rows", "NewName"));
        Assert.Equal(0UL, await ColumnCount(Conn, "rename_rows", "OldName"));

        // Insert via the new column name and read it back to confirm it works as a real column.
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"rename_rows\" (\"Id\", \"NewName\") VALUES (1, 'after-rename')");
        var name = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT \"NewName\" FROM \"rename_rows\" WHERE \"Id\" = 1");
        Assert.Equal("after-rename", name);
    }

    private async Task Reset()
    {
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"rename_rows\"");
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"__EFMigrationsHistory\"");
    }

    private static Task<ulong> ColumnCount(string conn, string table, string column) =>
        RawClickHouse.ScalarAsync<ulong>(conn,
            $"SELECT count() FROM system.columns WHERE database = currentDatabase() " +
            $"AND table = '{RawClickHouse.Esc(table)}' AND name = '{RawClickHouse.Esc(column)}'");

    private sealed class Stub { public uint Id { get; set; } }

    public sealed class RenameCtx(DbContextOptions<RenameCtx> o) : DbContext(o)
    {
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_Rename"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
    }

    [DbContext(typeof(RenameCtx))]
    [Migration("0001_CreateTable")]
    public sealed class CreateTableMigration : Migration
    {
        protected override void Up(MigrationBuilder mb)
        {
            mb.CreateTable(
                name: "rename_rows",
                columns: t => new
                {
                    Id = t.Column<uint>(nullable: false),
                    OldName = t.Column<string>(nullable: false),
                },
                constraints: c => c.PrimaryKey("PK_rename_rows", x => x.Id))
                .Annotation(ClickHouseAnnotationNames.Engine, "MergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
        }

        protected override void Down(MigrationBuilder mb) => mb.DropTable("rename_rows");
    }

    [DbContext(typeof(RenameCtx))]
    [Migration("0002_RenameColumn")]
    public sealed class RenameColumnMigration : Migration
    {
        protected override void Up(MigrationBuilder mb) =>
            mb.RenameColumn(name: "OldName", table: "rename_rows", newName: "NewName");

        protected override void Down(MigrationBuilder mb) =>
            mb.RenameColumn(name: "NewName", table: "rename_rows", newName: "OldName");
    }

    [DbContext(typeof(RenameCtx))]
    public sealed class RenameSnapshot : ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder mb) =>
            mb.Entity<Stub>(e => { e.ToTable("Stub_Rename"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); e.Property(x => x.Id); });
    }
}
