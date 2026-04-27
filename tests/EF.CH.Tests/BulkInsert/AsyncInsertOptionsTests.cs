using EF.CH.BulkInsert;
using Xunit;

namespace EF.CH.Tests.BulkInsert;

/// <summary>
/// Theme 3a coverage — exercise the post-redesign shape of
/// <see cref="ClickHouseBulkInsertOptions.WithAsyncInsert"/> /
/// <see cref="ClickHouseBulkInsertOptions.WaitForCompletion"/>.
/// </summary>
public class AsyncInsertOptionsTests
{
    [Fact]
    public void Default_DoesNotEmitAsyncInsertSettings()
    {
        var options = new ClickHouseBulkInsertOptions();

        var settings = options.GetEffectiveSettings();

        Assert.False(settings.ContainsKey("async_insert"));
        Assert.False(settings.ContainsKey("wait_for_async_insert"));
    }

    [Fact]
    public void WithAsyncInsert_EmitsAsyncInsertOnly()
    {
        var options = new ClickHouseBulkInsertOptions().WithAsyncInsert();

        var settings = options.GetEffectiveSettings();

        Assert.Equal(1, settings["async_insert"]);
        Assert.False(settings.ContainsKey("wait_for_async_insert"));
        Assert.True(options.UseAsyncInsert);
        Assert.False(options.WaitForAsyncInsert);
    }

    [Fact]
    public void WithAsyncInsert_ThenWaitForCompletion_EmitsBothSettings()
    {
        var options = new ClickHouseBulkInsertOptions()
            .WithAsyncInsert()
            .WaitForCompletion();

        var settings = options.GetEffectiveSettings();

        Assert.Equal(1, settings["async_insert"]);
        Assert.Equal(1, settings["wait_for_async_insert"]);
        Assert.True(options.UseAsyncInsert);
        Assert.True(options.WaitForAsyncInsert);
    }

    [Fact]
    public void WaitForCompletion_AloneImpliesAsyncInsert()
    {
        // Calling WaitForCompletion() without WithAsyncInsert() first should still
        // produce a valid, async-insert-enabled configuration.
        var options = new ClickHouseBulkInsertOptions().WaitForCompletion();

        var settings = options.GetEffectiveSettings();

        Assert.Equal(1, settings["async_insert"]);
        Assert.Equal(1, settings["wait_for_async_insert"]);
        Assert.True(options.UseAsyncInsert);
        Assert.True(options.WaitForAsyncInsert);
    }

    [Fact]
    public void WithAsyncInsert_ReturnsSameInstance()
    {
        var options = new ClickHouseBulkInsertOptions();

        Assert.Same(options, options.WithAsyncInsert());
        Assert.Same(options, options.WaitForCompletion());
    }
}
