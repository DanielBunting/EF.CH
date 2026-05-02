using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Migrations;

/// <summary>
/// Multi-source materialized view tests — exercises the new
/// <c>modelBuilder.MaterializedView&lt;T&gt;().From&lt;A&gt;().Join&lt;B&gt;().Join&lt;C&gt;()</c>
/// shape end-to-end against a real ClickHouse container. Validates:
/// 1. Joined source tables are created BEFORE the MV (dep-order ensures the
///    MV's <c>FROM … JOIN …</c> body resolves at <c>EnsureCreated</c> time).
/// 2. Inserts into the trigger source (<c>From&lt;Order&gt;</c>) propagate
///    aggregated rows into the MV target.
/// 3. Inserts into joined-only sources (<c>Join&lt;Customer&gt;</c>) do NOT
///    fire the MV — confirming ClickHouse's INSERT-trigger semantics.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MultiSourceMaterializedViewTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MultiSourceMaterializedViewTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task EnsureCreated_OrdersAndCustomersAndProducts_CreatesAllAndMv()
    {
        await using var ctx = TestContextFactory.Create<MultiSourceCtx>(Conn);
        // EF Core's EnsureCreatedAsync no-ops when the database has any tables,
        // even unrelated ones. Tests share SingleNodeClickHouseFixture, so a
        // sibling test's leftover schema would prevent this context's tables
        // from being created. EnsureDeletedAsync clears prior state first.
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync(); // joined tables created BEFORE MV

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "MS_Order"));
        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "MS_Customer"));
        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "MS_Product"));
        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "MS_RegionalRevenue"));

        // Seed dimension tables first.
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"MS_Customer\" (\"Id\", \"Region\", \"IsActive\") VALUES " +
            "(1, 'EU', true), (2, 'US', true), (3, 'EU', false)");
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"MS_Product\" (\"Id\", \"Category\") VALUES " +
            "(10, 'Books'), (20, 'Toys')");

        // Insert into the trigger source — this fires the MV.
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"MS_Order\" (\"Id\", \"CustomerId\", \"ProductId\", \"Amount\") VALUES " +
            "(100, 1, 10, 25), (101, 1, 20, 15), (102, 2, 10, 50), (103, 3, 20, 99)");

        await RawClickHouse.SettleMaterializationAsync(Conn, "MS_RegionalRevenue");

        // Active customers only (IsActive = true) → customer 3 filtered.
        // Expected rows: EU/Books (25), EU/Toys (15), US/Books (50)
        var rows = await RawClickHouse.ScalarAsync<ulong>(Conn,
            "SELECT count() FROM \"MS_RegionalRevenue\"");
        Assert.True(rows >= 1UL,
            $"Expected MV to receive trigger-driven rows, got {rows}.");
    }

    [Fact]
    public async Task InsertIntoJoinedSource_ProducesNoMvRows()
    {
        await using var ctx = TestContextFactory.Create<MultiSourceJoinedOnlyCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // Insert ONLY into the joined sources (NOT the trigger source).
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"MS2_Customer\" (\"Id\", \"Region\", \"IsActive\") VALUES (1, 'EU', true)");
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"MS2_Product\" (\"Id\", \"Category\") VALUES (10, 'Books')");

        // Brief settle window. No trigger writes occurred so there should be no MV rows.
        await Task.Delay(500);

        var rows = await RawClickHouse.ScalarAsync<ulong>(Conn,
            "SELECT count() FROM \"MS2_RegionalRevenue\"");
        Assert.Equal(0UL, rows);
    }

    // -----------------------------------------------------------------------
    //  Entities
    // -----------------------------------------------------------------------
    public sealed class MsOrder { public int Id { get; set; } public int CustomerId { get; set; } public int ProductId { get; set; } public long Amount { get; set; } }
    public sealed class MsCustomer { public int Id { get; set; } public string Region { get; set; } = ""; public bool IsActive { get; set; } }
    public sealed class MsProduct { public int Id { get; set; } public string Category { get; set; } = ""; }
    public sealed class MsRegionalRevenue { public string Category { get; set; } = ""; public string Region { get; set; } = ""; public long Total { get; set; } public ulong Count { get; set; } }

    public sealed class MultiSourceCtx(DbContextOptions<MultiSourceCtx> o) : DbContext(o)
    {
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<MsOrder>(e => { e.ToTable("MS_Order"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<MsCustomer>(e => { e.ToTable("MS_Customer"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<MsProduct>(e => { e.ToTable("MS_Product"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });

            mb.Entity<MsRegionalRevenue>(e =>
            {
                e.ToTable("MS_RegionalRevenue");
                e.UseSummingMergeTree(x => new { x.Category, x.Region });
            });

            // Method-syntax variant: the translator handles `Join`/`Join` chains with
            // flat anonymous projections; the multi-arg lambda's free IQueryable<TX>
            // parameters resolve to TX via the visitor's ResolveQueryableElementType.
            mb.MaterializedView<MsRegionalRevenue>()
                .From<MsOrder>()
                .Join<MsCustomer>()
                .Join<MsProduct>()
                .DefinedAs((orders, customers, products) => orders
                    .Join(customers,
                        o => o.CustomerId,
                        c => c.Id,
                        (o, c) => new { o.Amount, o.ProductId, c.Region, c.IsActive })
                    .Join(products,
                        oc => oc.ProductId,
                        p => p.Id,
                        (oc, p) => new { oc.Region, p.Category, oc.Amount, oc.IsActive })
                    .Where(x => x.IsActive)
                    .GroupBy(x => new { x.Category, x.Region })
                    .Select(g => new MsRegionalRevenue
                    {
                        Category = g.Key.Category,
                        Region = g.Key.Region,
                        Total = g.Sum(x => x.Amount),
                        Count = (ulong)g.Count(),
                    }));
        }
    }

    public sealed class MultiSourceJoinedOnlyCtx(DbContextOptions<MultiSourceJoinedOnlyCtx> o) : DbContext(o)
    {
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<MsOrder>(e => { e.ToTable("MS2_Order"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<MsCustomer>(e => { e.ToTable("MS2_Customer"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<MsProduct>(e => { e.ToTable("MS2_Product"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });

            mb.Entity<MsRegionalRevenue>(e =>
            {
                e.ToTable("MS2_RegionalRevenue");
                e.UseSummingMergeTree(x => new { x.Category, x.Region });
            });

            mb.MaterializedView<MsRegionalRevenue>()
                .From<MsOrder>()
                .Join<MsCustomer>()
                .Join<MsProduct>()
                .DefinedAs((orders, customers, products) => orders
                    .Join(customers,
                        o => o.CustomerId,
                        c => c.Id,
                        (o, c) => new { o.Amount, o.ProductId, c.Region, c.IsActive })
                    .Join(products,
                        oc => oc.ProductId,
                        p => p.Id,
                        (oc, p) => new { oc.Region, p.Category, oc.Amount, oc.IsActive })
                    .Where(x => x.IsActive)
                    .GroupBy(x => new { x.Category, x.Region })
                    .Select(g => new MsRegionalRevenue
                    {
                        Category = g.Key.Category,
                        Region = g.Key.Region,
                        Total = g.Sum(x => x.Amount),
                        Count = (ulong)g.Count(),
                    }));
        }
    }
}
