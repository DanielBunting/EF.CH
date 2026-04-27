using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization;

[Collection(SingleNodeCollection.Name)]
public class SummingMergeTreeMaterializationTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public SummingMergeTreeMaterializationTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task SourceInsertsRollUp_IntoSummingMergeTreeTarget()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "Orders"));
        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "DailyProductTotals"));

        var orders = new List<Order>();
        var rng = new Random(42);
        foreach (var day in Enumerable.Range(0, 3))
            foreach (var productId in new[] { 100, 200, 300 })
                for (int i = 0; i < 10; i++)
                    orders.Add(new Order
                    {
                        Id = Guid.NewGuid(),
                        OrderDate = new DateTime(2026, 1, 1 + day, 12, 0, 0, DateTimeKind.Utc),
                        ProductId = productId,
                        Quantity = rng.Next(1, 10),
                        Revenue = (decimal)(rng.NextDouble() * 100),
                    });

        ctx.Orders.AddRange(orders);
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "DailyProductTotals");

        var expected = orders
            .GroupBy(o => new { Day = o.OrderDate.Date, o.ProductId })
            .Select(g => (Day: g.Key.Day, ProductId: g.Key.ProductId,
                          Qty: g.Sum(x => x.Quantity), Rev: g.Sum(x => x.Revenue)))
            .OrderBy(x => x.Day).ThenBy(x => x.ProductId)
            .ToArray();

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT \"Day\", \"ProductId\", toInt64(\"TotalQuantity\") AS Qty, toFloat64(\"TotalRevenue\") AS Rev " +
            "FROM \"DailyProductTotals\" FINAL ORDER BY \"Day\", \"ProductId\"");

        Assert.Equal(expected.Length, rows.Count);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i].Day, (DateTime)rows[i]["Day"]!);
            Assert.Equal(expected[i].ProductId, Convert.ToInt32(rows[i]["ProductId"]));
            Assert.Equal(expected[i].Qty, Convert.ToInt64(rows[i]["Qty"]));
            Assert.Equal((double)expected[i].Rev, Convert.ToDouble(rows[i]["Rev"]), 1);
        }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<DailyProductTotal> DailyProductTotals => Set<DailyProductTotal>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Order>(e =>
            {
                e.ToTable("Orders"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.OrderDate, x.Id });
            });

            mb.Entity<DailyProductTotal>(e =>
            {
                e.ToTable("DailyProductTotals"); e.HasNoKey();
                e.UseSummingMergeTree(x => new { x.Day, x.ProductId });

            });
            mb.MaterializedView<DailyProductTotal>().From<Order>().DefinedAs(orders => orders
                    .GroupBy(o => new { Day = o.OrderDate.Date, o.ProductId })
                    .Select(g => new DailyProductTotal
                    {
                        Day = g.Key.Day,
                        ProductId = g.Key.ProductId,
                        TotalQuantity = g.Sum(o => o.Quantity),
                        TotalRevenue = g.Sum(o => o.Revenue),
                    }));
        }
    }

    public class Order
    {
        public Guid Id { get; set; }
        public DateTime OrderDate { get; set; }
        public int ProductId { get; set; }
        public long Quantity { get; set; }
        public decimal Revenue { get; set; }
    }

    public class DailyProductTotal
    {
        public DateTime Day { get; set; }
        public int ProductId { get; set; }
        public long TotalQuantity { get; set; }
        public decimal TotalRevenue { get; set; }
    }
}
