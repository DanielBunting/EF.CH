using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// Coverage of computed-column fluent surfaces:
/// <c>HasMaterializedExpression</c>, <c>HasAliasExpression</c>, <c>HasEphemeralExpression</c>,
/// <c>HasDefaultExpression</c>. Asserts the <c>default_kind</c>/<c>default_expression</c>
/// columns in <c>system.columns</c> reflect each kind.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class ComputedColumnTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ComputedColumnTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task ComputedKinds_AppearAsDefaultKindInSystemColumns()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT name, default_kind, default_expression FROM system.columns " +
            "WHERE database = currentDatabase() AND table = 'ComputedCol_Rows'");

        var byName = rows.ToDictionary(r => (string)r["name"]!);
        Assert.Equal("MATERIALIZED", byName["DoubledScore"]["default_kind"]);
        Assert.Equal("ALIAS", byName["UpperName"]["default_kind"]);
        Assert.Equal("EPHEMERAL", byName["TempBlob"]["default_kind"]);
        Assert.Equal("DEFAULT", byName["Region"]["default_kind"]);
    }

    [Fact]
    public async Task ComputedKinds_BehaveCorrectlyAtRuntime()
    {
        // Insert via raw SQL listing the user-supplied source columns only — neither
        // MATERIALIZED nor ALIAS columns can appear in an explicit INSERT column list,
        // and EPHEMERAL is write-only-via-source.
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"ComputedCol_Rows\" (Id, Name, Score) VALUES (1, 'bob', 5)");

        var row = await RawClickHouse.RowsAsync(Conn,
            "SELECT Id, Name, Score, DoubledScore, UpperName, Region FROM \"ComputedCol_Rows\" WHERE Id = 1");
        Assert.Single(row);
        Assert.Equal(5, Convert.ToInt32(row[0]["Score"]));
        // MATERIALIZED: computed at insert, persisted, readable.
        Assert.Equal(10, Convert.ToInt32(row[0]["DoubledScore"]));
        // ALIAS: computed at query time.
        Assert.Equal("BOB", (string)row[0]["UpperName"]!);
        // DEFAULT: applied because Region wasn't supplied.
        Assert.Equal("unknown", (string)row[0]["Region"]!);

        // EPHEMERAL columns are write-only and are NOT readable via SELECT *.
        var defaultKind = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT default_kind FROM system.columns WHERE database = currentDatabase() " +
            "AND table = 'ComputedCol_Rows' AND name = 'TempBlob'");
        Assert.Equal("EPHEMERAL", defaultKind);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string Name { get; set; } = "";
        public int Score { get; set; }
        public int DoubledScore { get; set; }
        public string UpperName { get; set; } = "";
        public string TempBlob { get; set; } = "";
        public string Region { get; set; } = "";
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("ComputedCol_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.DoubledScore).HasMaterializedExpression("Score * 2");
                e.Property(x => x.UpperName).HasAliasExpression("upperUTF8(Name)");
                e.Property(x => x.TempBlob).HasEphemeralExpression("''");
                e.Property(x => x.Region).HasDefaultExpression("'unknown'");
            });
    }
}
