using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.InsertSelect;

/// <summary>
/// Cancellation handling for INSERT…SELECT. The executor takes a CancellationToken
/// through to <c>ExecuteNonQueryAsync</c>; a pre-cancelled token must throw
/// <see cref="OperationCanceledException"/> rather than execute the insert.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public sealed class InsertSelectCancellationTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public InsertSelectCancellationTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task InsertSelect_CancelledBeforeStart_ThrowsAndDoesNotInsert()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Sources.AddRange(Enumerable.Range(1, 5)
            .Select(i => new Src { Id = (uint)i, Tag = "x" }));
        await ctx.SaveChangesAsync();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            ctx.Sources.InsertIntoAsync(ctx.Targets, s => new Tgt { Id = s.Id, Tag = s.Tag }, cts.Token));

        var copied = await RawClickHouse.RowCountAsync(Conn, "InsertSelectCancel_Tgt");
        Assert.Equal(0ul, copied);
    }

    public sealed class Src { public uint Id { get; set; } public string Tag { get; set; } = ""; }
    public sealed class Tgt { public uint Id { get; set; } public string Tag { get; set; } = ""; }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e => { e.ToTable("InsertSelectCancel_Src"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e => { e.ToTable("InsertSelectCancel_Tgt"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        }
    }
}
