using System.Text.Json;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Types;

/// <summary>
/// Coverage of the JSON-column fluent surfaces: <c>HasJsonOptions</c>, <c>HasMaxDynamicPaths</c>,
/// <c>HasMaxDynamicTypes</c>. Asserts a JSON column round-trips and that the engine_full
/// reflects the dynamic-paths/types options.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class JsonColumnTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public JsonColumnTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task JsonColumn_WithMaxDynamicPathsAndTypes_StoresAndReads()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Rows.Add(new Row { Id = 1, Data = JsonDocument.Parse("{\"name\":\"alpha\"}") });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var read = await ctx.Rows.SingleAsync(r => r.Id == 1);
        Assert.Equal("alpha", read.Data.RootElement.GetProperty("name").GetString());

        var t = await RawClickHouse.ColumnTypeAsync(Conn, "JsonColumn_Rows", "Data");
        Assert.StartsWith("JSON", t, StringComparison.Ordinal);

        // HasJsonOptions(maxDynamicPaths: 64, maxDynamicTypes: 16) must end up in DDL.
        var createSql = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'JsonColumn_Rows'");
        Assert.Contains("max_dynamic_paths = 64", createSql, StringComparison.Ordinal);
        Assert.Contains("max_dynamic_types = 16", createSql, StringComparison.Ordinal);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public JsonDocument Data { get; set; } = JsonDocument.Parse("{}");
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("JsonColumn_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Data)
                    .HasColumnType("JSON")
                    .HasJsonOptions(maxDynamicPaths: 64, maxDynamicTypes: 16);
            });
    }
}
