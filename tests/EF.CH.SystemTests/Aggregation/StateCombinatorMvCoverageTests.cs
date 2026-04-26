using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Aggregation;

/// <summary>
/// Exhaustive coverage of every <c>ClickHouseAggregates.*State</c> variant —
/// each <c>-State</c> aggregate is exercised inside a typed-LINQ MV body, the
/// resulting <c>AggregateFunction(...)</c> column is materialised, and then
/// read back via the corresponding <c>-Merge</c> aggregate.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class StateCombinatorMvCoverageTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public StateCombinatorMvCoverageTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task EveryStateVariant_ProducesUsableState_ReadsBackViaMerge()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Events.AddRange(
            new Event { Id = 1, Region = "us", Customer = "alice", Amount = 10, Score = 1 },
            new Event { Id = 2, Region = "us", Customer = "bob",   Amount = 20, Score = 2 },
            new Event { Id = 3, Region = "us", Customer = "alice", Amount = 30, Score = 3 },
            new Event { Id = 4, Region = "us", Customer = "carol", Amount = 40, Score = 4 },
            new Event { Id = 5, Region = "eu", Customer = "dave",  Amount = 50, Score = 5 },
            new Event { Id = 6, Region = "eu", Customer = "eve",   Amount = 60, Score = 6 });
        await ctx.SaveChangesAsync();
        await ctx.Database.ExecuteSqlRawAsync("OPTIMIZE TABLE state_mv_target FINAL");

        // Read each state column back via the matching -Merge aggregate. We
        // group again at read time (over the single MV row per region) so the
        // merge runs and unwraps the state into the user-visible CLR type.
        var us = await ctx.Targets
            .Where(t => t.Region == "us")
            .GroupBy(t => t.Region)
            .Select(g => new
            {
                Count = g.CountMerge(t => t.CountSt),
                Sum = g.SumMerge<Target, double>(t => t.SumSt),
                Avg = g.AvgMerge(t => t.AvgSt),
                Min = g.MinMerge<Target, double>(t => t.MinSt),
                Max = g.MaxMerge<Target, double>(t => t.MaxSt),
                Uniq = g.UniqMerge(t => t.UniqSt),
                UniqExact = g.UniqExactMerge(t => t.UniqExactSt),
                Any = g.AnyMerge<Target, string>(t => t.AnySt),
                AnyLast = g.AnyLastMerge<Target, string>(t => t.AnyLastSt),
                Quantile = g.QuantileMerge(0.5, t => t.QuantileSt),
            })
            .FirstAsync();

        Assert.Equal(4, us.Count);
        Assert.Equal(100, us.Sum);
        Assert.Equal(25, us.Avg);
        Assert.Equal(10, us.Min);
        Assert.Equal(40, us.Max);
        Assert.True(us.Uniq >= 3);              // 3 distinct customers
        Assert.Equal(3UL, us.UniqExact);
        Assert.NotEmpty(us.Any);
        Assert.NotEmpty(us.AnyLast);
        Assert.True(us.Quantile > 0);
    }

    public sealed class Event
    {
        public uint Id { get; set; }
        public string Region { get; set; } = "";
        public string Customer { get; set; } = "";
        public double Amount { get; set; }
        public ulong Score { get; set; }
    }

    /// <summary>
    /// Target columns are <c>byte[]</c> on the CLR side (ClickHouse
    /// AggregateFunction state blobs), declared with HasAggregateFunction so
    /// the storage type is e.g. <c>AggregateFunction(sum, Float64)</c>.
    /// </summary>
    public sealed class Target
    {
        public string Region { get; set; } = "";
        public byte[] CountSt { get; set; } = [];
        public byte[] SumSt { get; set; } = [];
        public byte[] AvgSt { get; set; } = [];
        public byte[] MinSt { get; set; } = [];
        public byte[] MaxSt { get; set; } = [];
        public byte[] UniqSt { get; set; } = [];
        public byte[] UniqExactSt { get; set; } = [];
        public byte[] AnySt { get; set; } = [];
        public byte[] AnyLastSt { get; set; } = [];
        public byte[] QuantileSt { get; set; } = [];
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Event> Events => Set<Event>();
        public DbSet<Target> Targets => Set<Target>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Event>(e =>
            {
                e.ToTable("state_mv_src"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
            mb.Entity<Target>(e =>
            {
                e.ToTable("state_mv_target"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Region);

                e.Property(x => x.CountSt).HasAggregateFunction("count", typeof(ulong));
                e.Property(x => x.SumSt).HasAggregateFunction("sum", typeof(double));
                e.Property(x => x.AvgSt).HasAggregateFunction("avg", typeof(double));
                e.Property(x => x.MinSt).HasAggregateFunction("min", typeof(double));
                e.Property(x => x.MaxSt).HasAggregateFunction("max", typeof(double));
                e.Property(x => x.UniqSt).HasAggregateFunction("uniq", typeof(string));
                e.Property(x => x.UniqExactSt).HasAggregateFunction("uniqExact", typeof(string));
                e.Property(x => x.AnySt).HasAggregateFunction("any", typeof(string));
                e.Property(x => x.AnyLastSt).HasAggregateFunction("anyLast", typeof(string));
                e.Property(x => x.QuantileSt).HasAggregateFunction("quantile(0.5)", typeof(double));

                e.AsMaterializedView<Target, Event>(src => src
                    .GroupBy(s => s.Region)
                    .Select(g => new Target
                    {
                        Region = g.Key,
                        CountSt = g.CountState(),
                        SumSt = g.SumState(s => s.Amount),
                        AvgSt = g.AvgState(s => s.Amount),
                        MinSt = g.MinState(s => s.Amount),
                        MaxSt = g.MaxState(s => s.Amount),
                        UniqSt = g.UniqState(s => s.Customer),
                        UniqExactSt = g.UniqExactState(s => s.Customer),
                        AnySt = g.AnyState(s => s.Customer),
                        AnyLastSt = g.AnyLastState(s => s.Customer),
                        QuantileSt = g.QuantileState(0.5, s => s.Amount),
                    }));
            });
        }
    }
}
