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
/// Drives <c>Database.MigrateAsync()</c> for every MergeTree engine variant and verifies the
/// rendered <c>engine_full</c> in <c>system.tables</c>. Engine annotations are attached directly
/// to each <c>CreateTableOperation</c> via the <c>MigrationBuilder</c> fluent API rather than
/// through the entity model — this matches how <c>PhaseOrderingIntegrationTests</c> exercises
/// the SQL generator and avoids relying on snapshot-model wiring.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MigrateEnginesTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MigrateEnginesTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task ResetAsync()
    {
        foreach (var t in new[]
        {
            "Eng_Plain", "Eng_Replacing", "Eng_ReplacingDeleted",
            "Eng_Collapsing", "Eng_Summing", "Eng_Aggregating",
        })
            await RawClickHouse.ExecuteAsync(Conn, $"DROP TABLE IF EXISTS \"{t}\"");
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"__EFMigrationsHistory\"");
    }

    private async Task MigrateAsync()
    {
        await using var ctx = TestContextFactory.Create<EnginesCtx>(Conn);
        await ctx.Database.MigrateAsync();
    }

    [Fact]
    public async Task MergeTree_WithPartitionSampleTtlSettings()
    {
        await ResetAsync();
        await MigrateAsync();

        var engine = await RawClickHouse.EngineFullAsync(Conn, "Eng_Plain");
        Assert.True(engine.Contains("MergeTree", StringComparison.Ordinal), engine);
        Assert.Contains("PARTITION BY toYYYYMM", engine);
        Assert.Contains("SAMPLE BY", engine);
        Assert.Contains("TTL", engine);
        Assert.Contains("index_granularity = 4096", engine);
    }

    [Fact]
    public async Task ReplacingMergeTree_WithVersionColumn()
    {
        await ResetAsync();
        await MigrateAsync();

        var engine = await RawClickHouse.EngineFullAsync(Conn, "Eng_Replacing");
        Assert.True(engine.Contains("ReplacingMergeTree(", StringComparison.Ordinal), engine);
        Assert.Contains("Version", engine);
    }

    [Fact]
    public async Task ReplacingMergeTree_WithIsDeletedColumn()
    {
        await ResetAsync();
        await MigrateAsync();

        var engine = await RawClickHouse.EngineFullAsync(Conn, "Eng_ReplacingDeleted");
        Assert.True(engine.Contains("ReplacingMergeTree(", StringComparison.Ordinal), engine);
        Assert.Contains("Version", engine);
        Assert.Contains("IsDeleted", engine);
    }

    [Fact]
    public async Task CollapsingMergeTree_WithSignColumn()
    {
        await ResetAsync();
        await MigrateAsync();

        var engine = await RawClickHouse.EngineFullAsync(Conn, "Eng_Collapsing");
        Assert.True(engine.Contains("CollapsingMergeTree(", StringComparison.Ordinal), engine);
        Assert.Contains("Sign", engine);
    }

    [Fact]
    public async Task SummingMergeTree()
    {
        await ResetAsync();
        await MigrateAsync();

        var engine = await RawClickHouse.EngineFullAsync(Conn, "Eng_Summing");
        Assert.Contains("SummingMergeTree", engine);
    }

    [Fact]
    public async Task AggregatingMergeTree()
    {
        await ResetAsync();
        await MigrateAsync();

        var engine = await RawClickHouse.EngineFullAsync(Conn, "Eng_Aggregating");
        Assert.Contains("AggregatingMergeTree", engine);
    }

    [Fact]
    public async Task MigrateAsync_Idempotent_OnSecondRun()
    {
        await ResetAsync();
        await MigrateAsync();
        await MigrateAsync();

        var ids = await RawClickHouse.ColumnAsync<string>(Conn,
            "SELECT MigrationId FROM \"__EFMigrationsHistory\"");
        Assert.Single(ids, id => id == "0001_Engines");
    }

    public sealed class EngineRow
    {
        public uint Id { get; set; }
        public DateTime CreatedAt { get; set; }
        public ulong Version { get; set; }
        public byte IsDeleted { get; set; }
        public sbyte Sign { get; set; }
        public ulong Total { get; set; }
        public string Bucket { get; set; } = "";
        public string Name { get; set; } = "";
    }

    public sealed class EnginesCtx(DbContextOptions<EnginesCtx> o) : DbContext(o)
    {
        public DbSet<EngineRow> Rows => Set<EngineRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<EngineRow>(e => { e.ToTable("EngineRows_Unused"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
    }

    [DbContext(typeof(EnginesCtx))]
    [Migration("0001_Engines")]
    public sealed class EnginesMigration : Migration
    {
        protected override void Up(MigrationBuilder mb)
        {
            mb.CreateTable(
                name: "Eng_Plain",
                columns: t => new
                {
                    Id = t.Column<uint>(nullable: false),
                    CreatedAt = t.Column<DateTime>(nullable: false),
                    Name = t.Column<string>(nullable: false),
                },
                constraints: c => c.PrimaryKey("PK_Eng_Plain", x => x.Id))
                .Annotation(ClickHouseAnnotationNames.Engine, "MergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id", "CreatedAt" })
                .Annotation(ClickHouseAnnotationNames.PartitionBy, "toYYYYMM(\"CreatedAt\")")
                .Annotation(ClickHouseAnnotationNames.SampleBy, "\"Id\"")
                .Annotation(ClickHouseAnnotationNames.Ttl, "\"CreatedAt\" + INTERVAL 30 DAY")
                .Annotation(ClickHouseAnnotationNames.Settings,
                    new Dictionary<string, string> { ["index_granularity"] = "4096" });

            mb.CreateTable(
                name: "Eng_Replacing",
                columns: t => new
                {
                    Id = t.Column<uint>(nullable: false),
                    Version = t.Column<ulong>(nullable: false),
                    Name = t.Column<string>(nullable: false),
                },
                constraints: c => c.PrimaryKey("PK_Eng_Replacing", x => x.Id))
                .Annotation(ClickHouseAnnotationNames.Engine, "ReplacingMergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" })
                .Annotation(ClickHouseAnnotationNames.VersionColumn, "Version");

            mb.CreateTable(
                name: "Eng_ReplacingDeleted",
                columns: t => new
                {
                    Id = t.Column<uint>(nullable: false),
                    Version = t.Column<ulong>(nullable: false),
                    IsDeleted = t.Column<byte>(nullable: false),
                    Name = t.Column<string>(nullable: false),
                },
                constraints: c => c.PrimaryKey("PK_Eng_ReplacingDeleted", x => x.Id))
                .Annotation(ClickHouseAnnotationNames.Engine, "ReplacingMergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" })
                .Annotation(ClickHouseAnnotationNames.VersionColumn, "Version")
                .Annotation(ClickHouseAnnotationNames.IsDeletedColumn, "IsDeleted");

            mb.CreateTable(
                name: "Eng_Collapsing",
                columns: t => new
                {
                    Id = t.Column<uint>(nullable: false),
                    Sign = t.Column<sbyte>(nullable: false),
                    Name = t.Column<string>(nullable: false),
                },
                constraints: c => c.PrimaryKey("PK_Eng_Collapsing", x => x.Id))
                .Annotation(ClickHouseAnnotationNames.Engine, "CollapsingMergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" })
                .Annotation(ClickHouseAnnotationNames.SignColumn, "Sign");

            mb.CreateTable(
                name: "Eng_Summing",
                columns: t => new
                {
                    Id = t.Column<uint>(nullable: false),
                    Total = t.Column<ulong>(nullable: false),
                },
                constraints: c => c.PrimaryKey("PK_Eng_Summing", x => x.Id))
                .Annotation(ClickHouseAnnotationNames.Engine, "SummingMergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

            mb.CreateTable(
                name: "Eng_Aggregating",
                columns: t => new
                {
                    Id = t.Column<uint>(nullable: false),
                    Bucket = t.Column<string>(nullable: false),
                },
                constraints: c => c.PrimaryKey("PK_Eng_Aggregating", x => x.Id))
                .Annotation(ClickHouseAnnotationNames.Engine, "AggregatingMergeTree")
                .Annotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
        }

        protected override void Down(MigrationBuilder mb)
        {
            mb.DropTable("Eng_Plain");
            mb.DropTable("Eng_Replacing");
            mb.DropTable("Eng_ReplacingDeleted");
            mb.DropTable("Eng_Collapsing");
            mb.DropTable("Eng_Summing");
            mb.DropTable("Eng_Aggregating");
        }
    }

    [DbContext(typeof(EnginesCtx))]
    public sealed class EnginesSnapshot : ModelSnapshot
    {
        protected override void BuildModel(ModelBuilder mb) =>
            mb.Entity<EngineRow>(e =>
            {
                e.ToTable("EngineRows_Unused");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
                e.Property(x => x.Id);
                e.Property(x => x.Name);
            });
    }
}
