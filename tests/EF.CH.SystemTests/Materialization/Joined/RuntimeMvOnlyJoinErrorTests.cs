using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Joined;

/// <summary>
/// The MV-only LINQ join methods (<c>AnyJoin</c>, <c>AnyLeftJoin</c>,
/// <c>AnyRightJoin</c>, <c>RightJoin</c>, <c>FullOuterJoin</c>,
/// <c>LeftSemiJoin</c>, <c>LeftAntiJoin</c>, <c>RightSemiJoin</c>,
/// <c>RightAntiJoin</c>, <c>CrossJoin</c>) are designed to be invoked inside
/// <c>MaterializedView&lt;T&gt;.DefinedAs(...)</c> bodies, where the MV
/// translator rewrites them to ClickHouse-specific join SQL. When a caller
/// invokes them on a runtime <c>DbSet&lt;T&gt;</c> instead, the standard EF
/// Core query pipeline runs and rejects them — today with the generic
/// <c>"could not be translated"</c> message. This test theory pins that
/// behaviour for every MV-only join method so any contract change (e.g. a
/// future improvement that swaps in a friendlier "MV-definition only"
/// message, or — worse — silently translates them at runtime) shows up as a
/// test failure rather than slipping past CI.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class RuntimeMvOnlyJoinErrorTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public RuntimeMvOnlyJoinErrorTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    public static IEnumerable<object[]> MvOnlyJoinShapes => new[]
    {
        new object[] { "AnyJoin",        (Func<IQueryable<L>, IQueryable<R>, IQueryable<object>>)((l, r) =>
            l.AnyJoin(r, x => x.Id, y => y.Id, (x, y) => new { x, y }).Cast<object>()) },
        new object[] { "AnyLeftJoin",    (Func<IQueryable<L>, IQueryable<R>, IQueryable<object>>)((l, r) =>
            l.AnyLeftJoin(r, x => x.Id, y => y.Id, (x, y) => new { x, y }).Cast<object>()) },
        new object[] { "AnyRightJoin",   (Func<IQueryable<L>, IQueryable<R>, IQueryable<object>>)((l, r) =>
            l.AnyRightJoin(r, x => x.Id, y => y.Id, (x, y) => new { x, y }).Cast<object>()) },
        new object[] { "RightJoin",      (Func<IQueryable<L>, IQueryable<R>, IQueryable<object>>)((l, r) =>
            l.RightJoin(r, x => x.Id, y => y.Id, (x, y) => new { x, y }).Cast<object>()) },
        new object[] { "FullOuterJoin",  (Func<IQueryable<L>, IQueryable<R>, IQueryable<object>>)((l, r) =>
            l.FullOuterJoin(r, x => x.Id, y => y.Id, (x, y) => new { x, y }).Cast<object>()) },
        new object[] { "LeftSemiJoin",   (Func<IQueryable<L>, IQueryable<R>, IQueryable<object>>)((l, r) =>
            l.LeftSemiJoin(r, x => x.Id, y => y.Id, x => new { x }).Cast<object>()) },
        new object[] { "LeftAntiJoin",   (Func<IQueryable<L>, IQueryable<R>, IQueryable<object>>)((l, r) =>
            l.LeftAntiJoin(r, x => x.Id, y => y.Id, x => new { x }).Cast<object>()) },
        new object[] { "RightSemiJoin",  (Func<IQueryable<L>, IQueryable<R>, IQueryable<object>>)((l, r) =>
            l.RightSemiJoin(r, x => x.Id, y => y.Id, y => new { y }).Cast<object>()) },
        new object[] { "RightAntiJoin",  (Func<IQueryable<L>, IQueryable<R>, IQueryable<object>>)((l, r) =>
            l.RightAntiJoin(r, x => x.Id, y => y.Id, y => new { y }).Cast<object>()) },
        new object[] { "CrossJoin",      (Func<IQueryable<L>, IQueryable<R>, IQueryable<object>>)((l, r) =>
            l.CrossJoin(r, (x, y) => new { x, y }).Cast<object>()) },
    };

    [Theory]
    [MemberData(nameof(MvOnlyJoinShapes))]
    public void MvOnlyJoinMethod_OnRuntimeDbSet_ThrowsAtTranslation(
        string label, Func<IQueryable<L>, IQueryable<R>, IQueryable<object>> compose)
    {
        using var ctx = TestContextFactory.Create<Ctx>(Conn);

        var query = compose(ctx.Lefts, ctx.Rights);

        var ex = Assert.Throws<InvalidOperationException>(() => query.ToQueryString());
        Assert.Contains("could not be translated", ex.Message);
        // Sanity: the offending method name appears in the message so the
        // user can grep for it. If a future change swaps in a friendlier
        // message that drops the method name, update this assertion.
        Assert.Contains(label, ex.Message);
    }

    public sealed class L { public uint Id { get; set; } }
    public sealed class R { public uint Id { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<L> Lefts => Set<L>();
        public DbSet<R> Rights => Set<R>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<L>(e => { e.ToTable("MvOnlyJoinErr_L"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<R>(e => { e.ToTable("MvOnlyJoinErr_R"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        }
    }
}
