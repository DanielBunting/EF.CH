using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// Gap #9 — originally catalogued as "exotic types can't round-trip through EF".
/// Empirical result (see .tmp/notes/feature-gaps.md §9 for the original claim):
/// the provider DOES round-trip simple Tuple, simple Nested, and Int256/UInt256
/// (via BigInteger) through <c>SaveChanges</c>. The remaining gap is narrower —
/// it's really about the <em>existing test code</em> using raw-SQL literal INSERTs
/// for these types, not about EF support being missing. The single failing shape
/// below is the one form that still can't flow through EF — nested collections
/// with parallel indexed access.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class NestedParallelAccessTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public NestedParallelAccessTests(SingleNodeClickHouseFixture fx) => _fixture = fx;

    [Fact]
    public async Task NestedParallelArrayShape_ShouldExposeFluentAccess()
    {
        await using var ctx = TestContextFactory.Create<ParallelNestedCtx>(_fixture.ConnectionString);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // ClickHouse's Nested(name String, age UInt8) writes parallel arrays
        // `Participants.name` and `Participants.age`. Today the best you can do
        // is map the whole Nested column to a List<Participant> and let EF
        // serialise it as a tuple array (which appears to work for simple
        // shapes — see the three other cases we removed). But when the Nested
        // columns need to be written as INDEPENDENT parallel arrays (so that
        // ClickHouse functions like arrayMap, arrayZip, and the .xxx indexed
        // access can target them individually), there's no fluent surface.
        //
        // EXPECTED SHAPE:
        //   modelBuilder.Entity<ParallelRow>(e =>
        //       e.HasNested<Participant>(x => x.Participants)
        //           .WithParallelAccess());
        //
        // Today no such fluent method exists — this test fails to compile-check
        // it via reflection.
        var found = typeof(ClickHouseEntityTypeBuilderExtensions).Assembly
            .GetTypes()
            .SelectMany(t => t.GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static))
            .Any(m => m.Name == "HasNested" || m.Name == "WithParallelAccess");
        Assert.True(found,
            "Expected HasNested<TNested>(...) / WithParallelAccess fluent methods for parallel-array access into " +
            "ClickHouse Nested columns. See .tmp/notes/feature-gaps.md §9.");
    }

    public class ParallelRow { public long Id { get; set; } public List<Participant> Participants { get; set; } = new(); }
    public class Participant { public string Name { get; set; } = ""; public uint Age { get; set; } }
    public sealed class ParallelNestedCtx(DbContextOptions<ParallelNestedCtx> o) : DbContext(o)
    {
        public DbSet<ParallelRow> Rows => Set<ParallelRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<ParallelRow>(e =>
            {
                e.ToTable("ParallelNestedRows"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
                e.HasNested<ParallelRow, Participant>(x => x.Participants)
                    .WithParallelAccess();
            });
    }
}
