using EF.CH.BulkInsert;
using Xunit;

namespace EF.CH.Tests.BulkInsert;

public class BulkInsertOptionsTests
{
    [Fact]
    public void DefaultOptions_HasExpectedValues()
    {
        var options = new ClickHouseBulkInsertOptions();

        Assert.Equal(10_000, options.BatchSize);
        Assert.Equal(ClickHouseBulkInsertFormat.Values, options.Format);
        Assert.False(options.UseAsyncInsert);
        Assert.False(options.WaitForAsyncInsert);
        Assert.Equal(1, options.MaxDegreeOfParallelism);
        Assert.Null(options.MaxInsertThreads);
        Assert.Empty(options.Settings);
        Assert.Null(options.Timeout);
        Assert.Null(options.OnBatchCompleted);
    }

    [Fact]
    public void WithBatchSize_SetsBatchSize()
    {
        var options = new ClickHouseBulkInsertOptions()
            .WithBatchSize(5000);

        Assert.Equal(5000, options.BatchSize);
    }

    [Fact]
    public void WithFormat_SetsFormat()
    {
        var options = new ClickHouseBulkInsertOptions()
            .WithFormat(ClickHouseBulkInsertFormat.JsonEachRow);

        Assert.Equal(ClickHouseBulkInsertFormat.JsonEachRow, options.Format);
    }

    [Fact]
    public void WithAsyncInsert_EnablesAsyncInsert()
    {
        var options = new ClickHouseBulkInsertOptions()
            .WithAsyncInsert();

        Assert.True(options.UseAsyncInsert);
        Assert.False(options.WaitForAsyncInsert);
    }

    [Fact]
    public void WithAsyncInsert_WithWait_EnablesWaiting()
    {
        var options = new ClickHouseBulkInsertOptions()
            .WithAsyncInsert(wait: true);

        Assert.True(options.UseAsyncInsert);
        Assert.True(options.WaitForAsyncInsert);
    }

    [Fact]
    public void WithParallelism_SetsMaxDegreeOfParallelism()
    {
        var options = new ClickHouseBulkInsertOptions()
            .WithParallelism(4);

        Assert.Equal(4, options.MaxDegreeOfParallelism);
    }

    [Fact]
    public void WithMaxInsertThreads_SetsMaxInsertThreads()
    {
        var options = new ClickHouseBulkInsertOptions()
            .WithMaxInsertThreads(8);

        Assert.Equal(8, options.MaxInsertThreads);
    }

    [Fact]
    public void WithSetting_AddsSetting()
    {
        var options = new ClickHouseBulkInsertOptions()
            .WithSetting("max_block_size", 1000);

        Assert.Single(options.Settings);
        Assert.Equal(1000, options.Settings["max_block_size"]);
    }

    [Fact]
    public void WithSettings_AddsMultipleSettings()
    {
        var options = new ClickHouseBulkInsertOptions()
            .WithSettings(new Dictionary<string, object>
            {
                ["max_block_size"] = 1000,
                ["max_threads"] = 4
            });

        Assert.Equal(2, options.Settings.Count);
        Assert.Equal(1000, options.Settings["max_block_size"]);
        Assert.Equal(4, options.Settings["max_threads"]);
    }

    [Fact]
    public void WithTimeout_SetsTimeout()
    {
        var timeout = TimeSpan.FromMinutes(5);
        var options = new ClickHouseBulkInsertOptions()
            .WithTimeout(timeout);

        Assert.Equal(timeout, options.Timeout);
    }

    [Fact]
    public void WithProgressCallback_SetsCallback()
    {
        var called = false;
        var options = new ClickHouseBulkInsertOptions()
            .WithProgressCallback(_ => called = true);

        Assert.NotNull(options.OnBatchCompleted);
        options.OnBatchCompleted!(100);
        Assert.True(called);
    }

    [Fact]
    public void FluentApi_IsChanable()
    {
        var options = new ClickHouseBulkInsertOptions()
            .WithBatchSize(5000)
            .WithFormat(ClickHouseBulkInsertFormat.JsonEachRow)
            .WithAsyncInsert(wait: true)
            .WithParallelism(4)
            .WithMaxInsertThreads(8)
            .WithSetting("custom", "value")
            .WithTimeout(TimeSpan.FromMinutes(10))
            .WithProgressCallback(_ => { });

        Assert.Equal(5000, options.BatchSize);
        Assert.Equal(ClickHouseBulkInsertFormat.JsonEachRow, options.Format);
        Assert.True(options.UseAsyncInsert);
        Assert.True(options.WaitForAsyncInsert);
        Assert.Equal(4, options.MaxDegreeOfParallelism);
        Assert.Equal(8, options.MaxInsertThreads);
        Assert.Equal("value", options.Settings["custom"]);
        Assert.Equal(TimeSpan.FromMinutes(10), options.Timeout);
        Assert.NotNull(options.OnBatchCompleted);
    }
}
