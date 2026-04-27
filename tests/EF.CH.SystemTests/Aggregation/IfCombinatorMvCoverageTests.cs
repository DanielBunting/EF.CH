using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Aggregation;

/// <summary>
/// Exhaustive MV-side coverage of every <c>ClickHouseAggregates.*If</c> variant.
/// Each method is exercised inside a typed-LINQ <see cref="ClickHouseEntityTypeBuilderExtensions"/>
/// materialised view body and the resulting column round-tripped back through EF.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class IfCombinatorMvCoverageTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public IfCombinatorMvCoverageTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task EveryIfVariant_TranslatesAndRoundTripsThroughMv()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // Two regions × mixed statuses gives non-trivial -If results.
        ctx.Sales.AddRange(
            new Sale { Id = 1, Region = "us", Status = "paid",      Amount = 100, Customer = "alice", Weight = 2 },
            new Sale { Id = 2, Region = "us", Status = "paid",      Amount = 250, Customer = "bob",   Weight = 5 },
            new Sale { Id = 3, Region = "us", Status = "cancelled", Amount = 50,  Customer = "alice", Weight = 1 },
            new Sale { Id = 4, Region = "us", Status = "paid",      Amount = 75,  Customer = "carol", Weight = 3 },
            new Sale { Id = 5, Region = "eu", Status = "paid",      Amount = 200, Customer = "dave",  Weight = 4 },
            new Sale { Id = 6, Region = "eu", Status = "cancelled", Amount = 30,  Customer = "eve",   Weight = 1 });
        await ctx.SaveChangesAsync();
        await ctx.Database.ExecuteSqlRawAsync("OPTIMIZE TABLE if_mv_target FINAL");

        var us = await ctx.Targets.SingleAsync(t => t.Region == "us");

        Assert.Equal(3UL, (ulong)us.PaidCount);                                  // countIf
        Assert.Equal(425, us.PaidRevenue);                                       // sumIf
        Assert.True(us.PaidAvg > 0);                                             // avgIf
        Assert.Equal(75, us.PaidMin);                                            // minIf
        Assert.Equal(250, us.PaidMax);                                           // maxIf
        Assert.True(us.PaidUniq >= 3);                                           // uniqIf
        Assert.Equal(3UL, us.PaidUniqExact);                                     // uniqExactIf
        Assert.True(us.PaidUniqCombined >= 3);                                   // uniqCombinedIf
        Assert.True(us.PaidUniqCombined64 >= 3);                                 // uniqCombined64If
        Assert.True(us.PaidUniqHLL12 >= 1);                                      // uniqHLL12If
        Assert.True(us.PaidUniqTheta >= 1);                                      // uniqThetaIf
        Assert.NotEmpty(us.PaidAny);                                             // anyIf
        Assert.NotEmpty(us.PaidAnyLast);                                         // anyLastIf
        Assert.True(us.PaidQuantile > 0);                                        // quantileIf
        Assert.True(us.PaidMedian > 0);                                          // medianIf
        Assert.True(us.PaidStddevPop >= 0);                                      // stddevPopIf
        Assert.True(us.PaidStddevSamp >= 0);                                     // stddevSampIf
        Assert.True(us.PaidVarPop >= 0);                                         // varPopIf
        Assert.True(us.PaidVarSamp >= 0);                                        // varSampIf
        Assert.NotEmpty(us.ArgMaxCustomerByAmount);                              // argMaxIf
        Assert.NotEmpty(us.ArgMinCustomerByAmount);                              // argMinIf
        Assert.NotEmpty(us.TopCustomers);                                        // topKIf
        Assert.NotEmpty(us.TopWeightedCustomers);                                // topKWeightedIf
        Assert.NotEmpty(us.PaidCustomerArr);                                     // groupArrayIf
        Assert.NotEmpty(us.PaidCustomerArrCapped);                               // groupArrayIf (with maxSize)
        Assert.NotEmpty(us.PaidCustomerSet);                                     // groupUniqArrayIf
        Assert.NotEmpty(us.PaidCustomerSetCapped);                               // groupUniqArrayIf (with maxSize)
        Assert.True(us.PaidQuantileExact > 0);                                   // quantileExactIf
        Assert.True(us.PaidQuantileTiming >= 0);                                 // quantileTimingIf
        Assert.True(us.PaidQuantileTDigest > 0);                                 // quantileTDigestIf
        Assert.True(us.PaidQuantileDD > 0);                                      // quantileDDIf
        Assert.Equal(3, us.PaidQuantilesArray.Length);                           // quantilesIf
        Assert.Equal(3, us.PaidQuantilesTDigestArray.Length);                    // quantilesTDigestIf
    }

    public sealed class Sale
    {
        public uint Id { get; set; }
        public string Region { get; set; } = "";
        public string Status { get; set; } = "";
        public double Amount { get; set; }
        public string Customer { get; set; } = "";
        public ulong Weight { get; set; }
    }

    public sealed class Target
    {
        public string Region { get; set; } = "";
        public long PaidCount { get; set; }
        public double PaidRevenue { get; set; }
        public double PaidAvg { get; set; }
        public double PaidMin { get; set; }
        public double PaidMax { get; set; }
        public ulong PaidUniq { get; set; }
        public ulong PaidUniqExact { get; set; }
        public ulong PaidUniqCombined { get; set; }
        public ulong PaidUniqCombined64 { get; set; }
        public ulong PaidUniqHLL12 { get; set; }
        public ulong PaidUniqTheta { get; set; }
        public string PaidAny { get; set; } = "";
        public string PaidAnyLast { get; set; } = "";
        public double PaidQuantile { get; set; }
        public double PaidMedian { get; set; }
        public double PaidStddevPop { get; set; }
        public double PaidStddevSamp { get; set; }
        public double PaidVarPop { get; set; }
        public double PaidVarSamp { get; set; }
        public string ArgMaxCustomerByAmount { get; set; } = "";
        public string ArgMinCustomerByAmount { get; set; } = "";
        public string[] TopCustomers { get; set; } = [];
        public string[] TopWeightedCustomers { get; set; } = [];
        public string[] PaidCustomerArr { get; set; } = [];
        public string[] PaidCustomerArrCapped { get; set; } = [];
        public string[] PaidCustomerSet { get; set; } = [];
        public string[] PaidCustomerSetCapped { get; set; } = [];
        public double PaidQuantileExact { get; set; }
        public double PaidQuantileTiming { get; set; }
        public double PaidQuantileTDigest { get; set; }
        public double PaidQuantileDD { get; set; }
        public double[] PaidQuantilesArray { get; set; } = [];
        public double[] PaidQuantilesTDigestArray { get; set; } = [];
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Sale> Sales => Set<Sale>();
        public DbSet<Target> Targets => Set<Target>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Sale>(e =>
            {
                e.ToTable("if_mv_src"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
            mb.Entity<Target>(e =>
            {
                e.ToTable("if_mv_target"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Region);

            });
            mb.MaterializedView<Target>().From<Sale>().DefinedAs(src => src
                    .GroupBy(s => s.Region)
                    .Select(g => new Target
                    {
                        Region = g.Key,
                        PaidCount = ClickHouseAggregates.CountIf(g, s => s.Status == "paid"),
                        PaidRevenue = ClickHouseAggregates.SumIf(g, s => s.Amount, s => s.Status == "paid"),
                        PaidAvg = ClickHouseAggregates.AvgIf(g, s => s.Amount, s => s.Status == "paid"),
                        PaidMin = ClickHouseAggregates.MinIf(g, s => s.Amount, s => s.Status == "paid"),
                        PaidMax = ClickHouseAggregates.MaxIf(g, s => s.Amount, s => s.Status == "paid"),
                        PaidUniq = ClickHouseAggregates.UniqIf(g, s => s.Customer, s => s.Status == "paid"),
                        PaidUniqExact = ClickHouseAggregates.UniqExactIf(g, s => s.Customer, s => s.Status == "paid"),
                        PaidUniqCombined = ClickHouseAggregates.UniqCombinedIf(g, s => s.Customer, s => s.Status == "paid"),
                        PaidUniqCombined64 = ClickHouseAggregates.UniqCombined64If(g, s => s.Customer, s => s.Status == "paid"),
                        PaidUniqHLL12 = ClickHouseAggregates.UniqHLL12If(g, s => s.Customer, s => s.Status == "paid"),
                        PaidUniqTheta = ClickHouseAggregates.UniqThetaIf(g, s => s.Customer, s => s.Status == "paid"),
                        PaidAny = ClickHouseAggregates.AnyIf(g, s => s.Customer, s => s.Status == "paid"),
                        PaidAnyLast = ClickHouseAggregates.AnyLastIf(g, s => s.Customer, s => s.Status == "paid"),
                        PaidQuantile = ClickHouseAggregates.QuantileIf(g, 0.95, s => s.Amount, s => s.Status == "paid"),
                        PaidMedian = ClickHouseAggregates.MedianIf(g, s => s.Amount, s => s.Status == "paid"),
                        PaidStddevPop = ClickHouseAggregates.StddevPopIf(g, s => s.Amount, s => s.Status == "paid"),
                        PaidStddevSamp = ClickHouseAggregates.StddevSampIf(g, s => s.Amount, s => s.Status == "paid"),
                        PaidVarPop = ClickHouseAggregates.VarPopIf(g, s => s.Amount, s => s.Status == "paid"),
                        PaidVarSamp = ClickHouseAggregates.VarSampIf(g, s => s.Amount, s => s.Status == "paid"),
                        ArgMaxCustomerByAmount = ClickHouseAggregates.ArgMaxIf(g, s => s.Customer, s => s.Amount, s => s.Status == "paid"),
                        ArgMinCustomerByAmount = ClickHouseAggregates.ArgMinIf(g, s => s.Customer, s => s.Amount, s => s.Status == "paid"),
                        TopCustomers = ClickHouseAggregates.TopKIf(g, 3, s => s.Customer, s => s.Status == "paid"),
                        TopWeightedCustomers = ClickHouseAggregates.TopKWeightedIf(g, 3, s => s.Customer, s => s.Weight, s => s.Status == "paid"),
                        PaidCustomerArr = ClickHouseAggregates.GroupArrayIf(g, s => s.Customer, s => s.Status == "paid"),
                        PaidCustomerArrCapped = ClickHouseAggregates.GroupArrayIf(g, 5, s => s.Customer, s => s.Status == "paid"),
                        PaidCustomerSet = ClickHouseAggregates.GroupUniqArrayIf(g, s => s.Customer, s => s.Status == "paid"),
                        PaidCustomerSetCapped = ClickHouseAggregates.GroupUniqArrayIf(g, 5, s => s.Customer, s => s.Status == "paid"),
                        PaidQuantileExact = ClickHouseAggregates.QuantileExactIf(g, 0.95, s => s.Amount, s => s.Status == "paid"),
                        PaidQuantileTiming = ClickHouseAggregates.QuantileTimingIf(g, 0.95, s => s.Amount, s => s.Status == "paid"),
                        PaidQuantileTDigest = ClickHouseAggregates.QuantileTDigestIf(g, 0.95, s => s.Amount, s => s.Status == "paid"),
                        PaidQuantileDD = ClickHouseAggregates.QuantileDDIf(g, 0.01, 0.95, s => s.Amount, s => s.Status == "paid"),
                        PaidQuantilesArray = ClickHouseAggregates.QuantilesIf(g, new[] { 0.5, 0.9, 0.99 }, s => s.Amount, s => s.Status == "paid"),
                        PaidQuantilesTDigestArray = ClickHouseAggregates.QuantilesTDigestIf(g, new[] { 0.5, 0.9, 0.99 }, s => s.Amount, s => s.Status == "paid"),
                    }));
        }
    }
}
