using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// Coverage for fluent Nested parallel-array access. The model uses
/// <c>HasNested(...).WithParallelAccess()</c> and deploys against ClickHouse to
/// ensure the provider exposes nested columns as ClickHouse's parallel-array
/// shape.
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

        var extensionMethods = typeof(ClickHouseEntityTypeBuilderExtensions).Assembly
            .GetTypes()
            .SelectMany(t => t.GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static))
            .ToArray();
        var hasNested = extensionMethods.Any(m => m.Name == "HasNested");
        var withParallelAccess = typeof(NestedColumnBuilder<>)
            .GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
            .Any(m => m.Name == "WithParallelAccess");
        Assert.True(hasNested && withParallelAccess,
            "Expected HasNested<TNested>(...) and WithParallelAccess fluent methods for parallel-array access into ClickHouse Nested columns.");
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
