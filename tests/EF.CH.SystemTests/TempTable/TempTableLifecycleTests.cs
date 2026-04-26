using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using EF.CH.TempTable;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.TempTable;

/// <summary>
/// Lifecycle of <c>CreateTempTableAsync&lt;T&gt;</c>: the temp table is visible on the same
/// connection while the handle lives, and dropped on dispose.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class TempTableLifecycleTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public TempTableLifecycleTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task CreateTempTable_Async_VisibleWhileLive_AndDroppedOnDispose()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        string tempName;
        await using (var tt = await ctx.CreateTempTableAsync<Row>())
        {
            tempName = tt.TableName;
            await tt.InsertAsync(new[] { new Row { Id = 1, V = 100 }, new Row { Id = 2, V = 200 } });

            // Visible while the handle is alive — readable via the EF query path.
            var sum = await tt.Query().Select(r => (long?)r.V).SumAsync();
            Assert.Equal(300L, sum);
        }

        // After Dispose: the table must be dropped. Query system.tables for it.
        // Temp tables typically live in a `temp_*`-named database or as is_temporary=1
        // entries — query both candidates and assert nothing matches.
        var stillExists = await RawClickHouse.ScalarAsync<ulong>(Conn,
            $"SELECT count() FROM system.tables WHERE name = '{RawClickHouse.Esc(tempName)}'");
        Assert.Equal(0ul, stillExists);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public int V { get; set; }
    }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("TempTableLifecycle_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
