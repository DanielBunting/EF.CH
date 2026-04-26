using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.ValueTypes;

/// <summary>MV pass-through for <c>Array(String)</c>.</summary>
[Collection(SingleNodeCollection.Name)]
public class MvArrayPassThroughTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvArrayPassThroughTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task ArrayString_PassesThrough()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Sources.AddRange(
            new Src { Id = 1, Tags = new[] { "alpha", "beta" },         N = 1 },
            new Src { Id = 2, Tags = new[] { "x" },                      N = 2 },
            new Src { Id = 3, Tags = Array.Empty<string>(),              N = 3 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvArrayTarget");

        Assert.Equal(3UL, await RawClickHouse.RowCountAsync(Conn, "MvArrayTarget"));
        Assert.Equal("Array(String)", await RawClickHouse.ColumnTypeAsync(Conn, "MvArrayTarget", "Tags"));
    }

    public sealed class Src { public uint Id { get; set; } public string[] Tags { get; set; } = Array.Empty<string>(); public long N { get; set; } }
    public sealed class Tgt { public string[] Tags { get; set; } = Array.Empty<string>(); public long N { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e =>
            {
                e.ToTable("MvArraySource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Tags).HasColumnType("Array(String)");
            });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvArrayTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.N);
                e.Property(x => x.Tags).HasColumnType("Array(String)");
                e.AsMaterializedView<Tgt, Src>(rows => rows
                    .Select(r => new Tgt { Tags = r.Tags, N = r.N }));
            });
        }
    }
}
