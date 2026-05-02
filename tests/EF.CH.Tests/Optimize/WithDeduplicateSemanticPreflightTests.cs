using EF.CH.Extensions;
using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Optimize;

/// <summary>
/// <c>WithDeduplicate(params string[])</c> validates string shape today
/// (non-null, non-whitespace) but not <em>semantic</em> shape: when the
/// caller passes columns that exclude the entity's primary key, ClickHouse
/// rejects the OPTIMIZE TABLE with an unhelpful server-side error at
/// execution time. The audit recommends a model-aware pre-flight check at
/// configuration time so the call fails before any SQL is sent.
///
/// These tests are <em>specifications</em>: the pre-flight does not exist
/// yet, so the throwing test fails today and the happy-path test passes.
/// Both pin the intended contract.
/// </summary>
public class WithDeduplicateSemanticPreflightTests
{
    [Fact]
    public async Task WithDeduplicate_ColumnsExcludePrimaryKey_ThrowsAtConfigure()
    {
        await using var ctx = CreateContext();

        // The entity's PK is `Id`. Passing `Name` only — i.e. omitting the
        // PK — should be rejected at configure time, before SQL execution.
        // Today the configure path doesn't validate against the model, so
        // the call proceeds to SQL execution and fails at the network layer
        // instead — this test fails loudly until a model-aware preflight
        // is wired into WithDeduplicate / OptimizeTableAsync.
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await ctx.Database.OptimizeTableAsync<Item>(o => o.WithDeduplicate("Name"));
        });

        // The exception message must indicate a model-aware preflight
        // rejection — not a connection or syntax error from the server.
        // Update once the preflight ships.
        Assert.Contains("primary key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void WithDeduplicate_ColumnsCoverPrimaryKey_DoesNotThrow()
    {
        // Negative control: a column list that DOES include the PK is
        // semantically valid and must not throw at configure time. We
        // construct the options object directly so the test doesn't need a
        // live ClickHouse instance.
        var opts = new OptimizeTableOptions();
        var ex = Record.Exception(() => opts.WithDeduplicate("Id", "Name"));
        Assert.Null(ex);
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
                e.ToTable("dedup_preflight_items");
                e.HasKey(x => x.Id);
            });
    }
}
