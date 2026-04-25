using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using EF.CH.SystemTests.Migrations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Xunit;

namespace EF.CH.SystemTests.Migrations;

/// <summary>
/// Round-trip a real <c>ctx.Database.MigrateAsync()</c> call (not <c>EnsureCreatedAsync</c>).
/// Verifies the migration system applies the schema, records the migration row in
/// <c>__EFMigrationsHistory</c>, and is idempotent on re-run.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MigrateRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MigrateRoundTripTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task MigrateAsync_AppliesSchema_AndRecordsHistoryRow()
    {
        // Clean slate.
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"MigrateRT_Items\"");
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"__EFMigrationsHistory\"");

        await using (var ctx = TestContextFactory.Create<Ctx>(Conn))
        {
            await ctx.Database.MigrateAsync();
        }

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "MigrateRT_Items"));
        var ids = await RawClickHouse.ColumnAsync<string>(Conn,
            "SELECT MigrationId FROM \"__EFMigrationsHistory\"");
        Assert.Contains("0001_Initial", ids);

        // Re-run: idempotent.
        await using (var ctx = TestContextFactory.Create<Ctx>(Conn))
        {
            await ctx.Database.MigrateAsync();
        }

        var ids2 = await RawClickHouse.ColumnAsync<string>(Conn,
            "SELECT MigrationId FROM \"__EFMigrationsHistory\"");
        Assert.Single(ids2, id => id == "0001_Initial");
    }

    public sealed class Item
    {
        public uint Id { get; set; }
        public string Name { get; set; } = "";
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Item> Items => Set<Item>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Item>(e =>
            {
                e.ToTable("MigrateRT_Items"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }

    [DbContext(typeof(Ctx))]
    [Migration("0001_Initial")]
    public sealed class InitialMigration : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "MigrateRT_Items",
                columns: table => new
                {
                    Id = table.Column<uint>(nullable: false),
                    Name = table.Column<string>(nullable: false),
                },
                constraints: table => table.PrimaryKey("PK_MigrateRT_Items", x => x.Id));
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable("MigrateRT_Items");
        }
    }

    [DbContext(typeof(Ctx))]
    public sealed class CtxModelSnapshot : ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Item>(e =>
            {
                e.ToTable("MigrateRT_Items");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
                e.Property(x => x.Id);
                e.Property(x => x.Name);
            });
        }
    }
}
