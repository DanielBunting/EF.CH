using System.Text.Json;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.ValueTypes;

/// <summary>MV pass-through for the experimental <c>JSON</c> column type.</summary>
[Collection(SingleNodeCollection.Name)]
public class MvJsonRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvJsonRoundTripTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Json_PassesThrough()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Sources.AddRange(
            new Src { Id = 1, Data = JsonDocument.Parse("{\"name\":\"alpha\"}"), N = 1 },
            new Src { Id = 2, Data = JsonDocument.Parse("{\"name\":\"beta\",\"x\":2}"), N = 2 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvJsonTarget");

        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "MvJsonTarget"));
        Assert.StartsWith("JSON", await RawClickHouse.ColumnTypeAsync(Conn, "MvJsonTarget", "Data"));
    }

    public sealed class Src { public uint Id { get; set; } public JsonDocument Data { get; set; } = JsonDocument.Parse("{}"); public long N { get; set; } }
    public sealed class Tgt { public JsonDocument Data { get; set; } = JsonDocument.Parse("{}"); public long N { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e =>
            {
                e.ToTable("MvJsonSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Data).HasColumnType("JSON");
            });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvJsonTarget"); e.HasNoKey();
                e.UseMergeTree(x => x.N);
                e.Property(x => x.Data).HasColumnType("JSON");

            });
            mb.MaterializedView<Tgt>().From<Src>().DefinedAs(rows => rows
                    .Select(r => new Tgt { Data = r.Data, N = r.N }));
        }
    }
}
