using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Aggregation;

[Collection(SingleNodeCollection.Name)]
public class IfCombinatorRollupTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public IfCombinatorRollupTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task CountStateIf_SumStateIf_UniqStateIf_MatchConditionalAggregates()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rng = new Random(99);
        var data = Enumerable.Range(0, 600).Select(i => new Tx
        {
            Id = Guid.NewGuid(),
            Tenant = i % 3 == 0 ? "A" : "B",
            UserId = rng.Next(1, 25),
            Amount = Math.Round(rng.NextDouble() * 200, 2),
            IsError = rng.NextDouble() < 0.2,
        }).ToList();

        ctx.Transactions.AddRange(data);
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "TenantStats");

        var expected = data.GroupBy(d => d.Tenant)
            .Select(g => (
                Tenant: g.Key,
                ErrorCount: g.LongCount(x => x.IsError),
                BigSum: g.Where(x => x.Amount > 100).Sum(x => x.Amount),
                UniqErrorUsers: g.Where(x => x.IsError).Select(x => x.UserId).Distinct().LongCount()))
            .OrderBy(x => x.Tenant).ToArray();

        var rows = await RawClickHouse.RowsAsync(Conn,
            """
            SELECT Tenant,
                   toInt64(countMerge(ErrorCount)) AS ErrorCount,
                   toFloat64(sumMerge(BigSum)) AS BigSum,
                   toInt64(uniqMerge(UniqErrorUsers)) AS UniqErrorUsers
            FROM "TenantStats" GROUP BY Tenant ORDER BY Tenant
            """);

        Assert.Equal(expected.Length, rows.Count);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i].Tenant, (string)rows[i]["Tenant"]!);
            Assert.Equal(expected[i].ErrorCount, Convert.ToInt64(rows[i]["ErrorCount"]));
            Assert.InRange(Convert.ToDouble(rows[i]["BigSum"]),
                expected[i].BigSum - 0.5, expected[i].BigSum + 0.5);
            Assert.Equal(expected[i].UniqErrorUsers, Convert.ToInt64(rows[i]["UniqErrorUsers"]));
        }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Tx> Transactions => Set<Tx>();
        public DbSet<TenantStat> TenantStats => Set<TenantStat>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Tx>(e =>
            {
                e.ToTable("Transactions"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.Tenant, x.Id });
            });
            mb.Entity<TenantStat>(e =>
            {
                e.ToTable("TenantStats"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Tenant);
                e.Property(x => x.ErrorCount).HasAggregateFunction("countIf", typeof(ulong));
                e.Property(x => x.BigSum).HasAggregateFunction("sumIf", typeof(double));
                e.Property(x => x.UniqErrorUsers).HasAggregateFunction("uniqIf", typeof(long));

            });
            mb.MaterializedView<TenantStat>().From<Tx>().DefinedAs(src => src
                    .GroupBy(x => x.Tenant)
                    .Select(g => new TenantStat
                    {
                        Tenant = g.Key,
                        ErrorCount = g.CountStateIf(x => x.IsError),
                        BigSum = g.SumStateIf(x => x.Amount, x => x.Amount > 100),
                        UniqErrorUsers = g.UniqStateIf(x => x.UserId, x => x.IsError),
                    }));
        }
    }

    public class Tx { public Guid Id { get; set; } public string Tenant { get; set; } = ""; public long UserId { get; set; } public double Amount { get; set; } public bool IsError { get; set; } }
    public class TenantStat { public string Tenant { get; set; } = ""; public byte[] ErrorCount { get; set; } = Array.Empty<byte>(); public byte[] BigSum { get; set; } = Array.Empty<byte>(); public byte[] UniqErrorUsers { get; set; } = Array.Empty<byte>(); }
}
