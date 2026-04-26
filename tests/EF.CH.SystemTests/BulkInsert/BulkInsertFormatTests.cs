using EF.CH.BulkInsert;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.BulkInsert;

/// <summary>
/// Coverage of <c>BulkInsertAsync</c> across the supported insert formats.
/// Toggling format should not change the visible row count.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class BulkInsertFormatTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public BulkInsertFormatTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    public static IEnumerable<object[]> Formats => new[]
    {
        new object[] { ClickHouseBulkInsertFormat.Values },
        new object[] { ClickHouseBulkInsertFormat.JsonEachRow },
    };

    [Theory]
    [MemberData(nameof(Formats))]
    public async Task BulkInsert_ToggleFormat_RowsLandIntact(ClickHouseBulkInsertFormat format)
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rows = Enumerable.Range(1, 100).Select(i => new Row { Id = (uint)i, Name = $"name-{i}" }).ToList();
        var result = await ctx.BulkInsertAsync(rows, o => o.Format = format);
        Assert.Equal(100L, result.RowsInserted);

        var n = await RawClickHouse.RowCountAsync(Conn, "BulkInsertFormat_Rows");
        Assert.Equal(100ul, n);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string Name { get; set; } = "";
    }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("BulkInsertFormat_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
