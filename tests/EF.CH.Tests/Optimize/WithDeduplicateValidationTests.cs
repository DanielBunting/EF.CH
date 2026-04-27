using EF.CH.Infrastructure;
using Xunit;

namespace EF.CH.Tests.Optimize;

public class WithDeduplicateValidationTests
{
    [Fact]
    public void NoArgs_SetsDeduplicateAndLeavesByNull()
    {
        var options = new OptimizeTableOptions().WithDeduplicate();

        Assert.True(options.Deduplicate);
        Assert.Null(options.DeduplicateBy);
    }

    [Fact]
    public void NonEmptyColumns_SucceedsAndCapturesArray()
    {
        var options = new OptimizeTableOptions().WithDeduplicate("a", "b");

        Assert.True(options.Deduplicate);
        Assert.Equal(new[] { "a", "b" }, options.DeduplicateBy);
    }

    [Fact]
    public void EmptyStringEntry_ThrowsArgumentException()
    {
        var options = new OptimizeTableOptions();

        var ex = Assert.Throws<ArgumentException>(() => options.WithDeduplicate("a", ""));
        Assert.Contains("WithDeduplicate", ex.Message);
    }

    [Fact]
    public void WhitespaceEntry_ThrowsArgumentException()
    {
        var options = new OptimizeTableOptions();
        Assert.Throws<ArgumentException>(() => options.WithDeduplicate("a", "  "));
    }

    [Fact]
    public void NullEntry_ThrowsArgumentException()
    {
        var options = new OptimizeTableOptions();
        Assert.Throws<ArgumentException>(() => options.WithDeduplicate("a", null!));
    }

    [Fact]
    public void NullArray_ThrowsArgumentNullException()
    {
        var options = new OptimizeTableOptions();
        Assert.Throws<ArgumentNullException>(() => options.WithDeduplicate(null!));
    }
}
