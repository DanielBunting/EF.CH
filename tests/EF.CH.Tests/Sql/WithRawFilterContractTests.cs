using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// <c>WithRawFilter</c> is intentionally raw — the caller's SQL string is
/// forwarded into the WHERE clause verbatim, with no escaping or
/// parameterisation. The contract is "trust your inputs". This test
/// documents that contract by passing a string that <em>looks</em>
/// SQL-injectionish (a non-destructive proxy for the real injection
/// shape) and asserting the emitted query contains it byte-for-byte.
/// </summary>
public class WithRawFilterContractTests
{
    [Fact]
    public void WithRawFilter_ForwardsMaliciousString_Verbatim()
    {
        using var ctx = CreateContext();

        // Non-destructive proxy: a tautology and a comment-out follow-on.
        // Real injection vectors look like this; the test pins that the
        // provider neither escapes nor blocks the input.
        const string evil = "1 = 1; -- DROP TABLE x";

        var sql = ctx.Items.WithRawFilter(evil).ToQueryString();

        Assert.Contains(evil, sql);
    }

    private static Ctx CreateContext()
    {
        var options = new DbContextOptionsBuilder<Ctx>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;
        return new Ctx(options);
    }

    public sealed class Item
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Item> Items => Set<Item>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Item>(e =>
            {
                e.ToTable("with_raw_filter_items");
                e.HasKey(x => x.Id);
            });
    }
}
